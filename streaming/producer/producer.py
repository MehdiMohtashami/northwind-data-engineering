"""
CDC producer: tails SQL Server CDC on dbo.Orders and dbo.[Order Details] and
publishes each change as a JSON message to Kafka.

Uses its OWN watermark (ETL_Settings.cdc_state.stream_last_lsn), separate from
the batch DAG's last_lsn, so the streaming path and the (paused) micro-batch
DAG never fight over the same cursor.

Message shape:
    {"op": "I"|"U"|"D", "table": "Orders"|"OrderDetails", "lsn": "<hex>",
     "ts": "<iso8601>", "after": {...columns...}}

__$operation mapping: 1=D, 2=I, 4=U(after), 5=U(merge). op=3 (the BEFORE image
of an update) is deliberately dropped, not just relabeled -- it shares the
exact same __$start_lsn as its op=4 after-image counterpart, and the consumer
uses __$start_lsn to derive FactOrders.version. Forwarding both would give two
messages the same version, letting ClickHouse's ReplacingMergeTree pick either
one on collapse -- sometimes the stale "before" row would win.

Each message's "after" row is ordered by (__$start_lsn, __$seqval) within a
poll window, and every message for a given OrderID is Kafka-keyed by OrderID,
so a single partition sees them in true commit order -- the consumer doesn't
need cross-message coordination to stay correct.
"""
import json
import logging
import os
import time

import pymssql
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cdc-producer")

MSSQL_HOST = os.environ["MSSQL_HOST"]
MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "1433"))
MSSQL_DB = os.environ.get("MSSQL_DB", "Northwind")
MSSQL_USER = os.environ["MSSQL_USER"]
MSSQL_PASSWORD = os.environ["MSSQL_PASSWORD"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "2.5"))

OP_MAP = {1: "D", 2: "I", 4: "U", 5: "U"}  # 3 (before-image) is skipped

TOPIC_ORDERS = "nw.cdc.orders"
TOPIC_ORDER_DETAILS = "nw.cdc.order_details"
TOPIC_SUPPLIERS = "nw.cdc.suppliers"

ORDERS_COLUMNS = [
    "OrderID", "CustomerID", "EmployeeID", "OrderDate", "RequiredDate", "ShippedDate",
    "ShipVia", "Freight", "ShipName", "ShipAddress", "ShipCity", "ShipRegion",
    "ShipPostalCode", "ShipCountry",
]
ORDER_DETAILS_COLUMNS = ["OrderID", "ProductID", "UnitPrice", "Quantity", "Discount"]
# Phase 4 dimension-delete proof of concept (scoped to Suppliers): the consumer
# only acts on "D" messages from this topic (see consumer.py), so the columns
# here just need to be enough to identify the row -- SupplierID.
SUPPLIERS_COLUMNS = ["SupplierID"]


def get_conn():
    return pymssql.connect(
        server=MSSQL_HOST, port=str(MSSQL_PORT), database=MSSQL_DB,
        user=MSSQL_USER, password=MSSQL_PASSWORD,
    )


def json_default(o):
    if isinstance(o, (bytes, bytearray)):
        return o.hex()
    return str(o)


def fetch_changes(conn, capture_instance, columns, from_lsn, to_lsn):
    cur = conn.cursor(as_dict=True)
    col_list = ", ".join(columns)
    cur.execute(
        f"SELECT __$operation AS op, __$start_lsn AS lsn, {col_list} "
        f"FROM cdc.fn_cdc_get_all_changes_{capture_instance}(%s, %s, 'all') "
        f"ORDER BY __$start_lsn, __$seqval",
        (from_lsn, to_lsn),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def build_message(table_key, row, columns):
    op = OP_MAP.get(row["op"])
    if op is None:
        return None
    return {
        "op": op,
        "table": table_key,
        "lsn": row["lsn"].hex(),
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "after": {c: row[c] for c in columns},
    }


def run_once(producer):
    conn = get_conn()
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT stream_last_lsn FROM ETL_Settings.dbo.cdc_state WHERE source_table = 'Orders'")
        row = cur.fetchone()
        last_lsn = row["stream_last_lsn"] if row else None

        if last_lsn is None:
            cur.execute("SELECT sys.fn_cdc_get_min_lsn('dbo_Orders') AS lsn")
            from_lsn = cur.fetchone()["lsn"]
        else:
            cur.execute("SELECT sys.fn_cdc_increment_lsn(%s) AS lsn", (last_lsn,))
            from_lsn = cur.fetchone()["lsn"]

        cur.execute("SELECT sys.fn_cdc_get_max_lsn() AS lsn")
        to_lsn = cur.fetchone()["lsn"]
        cur.close()

        if from_lsn is None or to_lsn is None or from_lsn > to_lsn:
            return 0

        t0 = time.time()
        published = 0

        for row in fetch_changes(conn, "dbo_Orders", ORDERS_COLUMNS, from_lsn, to_lsn):
            msg = build_message("Orders", row, ORDERS_COLUMNS)
            if msg is None:
                continue
            producer.produce(TOPIC_ORDERS, key=str(msg["after"]["OrderID"]),
                              value=json.dumps(msg, default=json_default))
            published += 1

        for row in fetch_changes(conn, "dbo_OrderDetails", ORDER_DETAILS_COLUMNS, from_lsn, to_lsn):
            msg = build_message("OrderDetails", row, ORDER_DETAILS_COLUMNS)
            if msg is None:
                continue
            producer.produce(TOPIC_ORDER_DETAILS, key=str(msg["after"]["OrderID"]),
                              value=json.dumps(msg, default=json_default))
            published += 1

        for row in fetch_changes(conn, "dbo_Suppliers", SUPPLIERS_COLUMNS, from_lsn, to_lsn):
            msg = build_message("Suppliers", row, SUPPLIERS_COLUMNS)
            if msg is None:
                continue
            producer.produce(TOPIC_SUPPLIERS, key=str(msg["after"]["SupplierID"]),
                              value=json.dumps(msg, default=json_default))
            published += 1

        producer.flush(10)

        upd = conn.cursor()
        upd.execute(
            "UPDATE ETL_Settings.dbo.cdc_state SET stream_last_lsn = %s, last_processed_at = SYSUTCDATETIME() "
            "WHERE source_table IN ('Orders', 'Order Details', 'Suppliers')",
            (to_lsn,),
        )
        conn.commit()
        upd.close()

        if published:
            elapsed = time.time() - t0
            rate = published / elapsed if elapsed > 0 else float("inf")
            log.info("published %d messages in %.3fs (%.1f msg/s), window %s -> %s",
                      published, elapsed, rate, from_lsn.hex(), to_lsn.hex())
        return published
    finally:
        conn.close()


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    log.info("CDC producer starting: kafka=%s mssql=%s:%s/%s poll=%.1fs",
              KAFKA_BOOTSTRAP, MSSQL_HOST, MSSQL_PORT, MSSQL_DB, POLL_INTERVAL_SECONDS)
    while True:
        try:
            run_once(producer)
        except Exception:
            log.exception("producer loop iteration failed")
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
