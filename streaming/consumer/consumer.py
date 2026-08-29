"""
CDC consumer: reads nw.cdc.orders / nw.cdc.order_details from Kafka and
applies each change directly to ClickHouse NorthwindDW.FactOrders (Decision
D4 -- no staging in between).

Same semantics as the Phase 3 batch fact job:
  - order_details I/U -> resolve dim keys against CURRENT dims (is_current=1),
    inferred-member placeholder insert on any miss, write is_deleted=0.
  - order_details D   -> rewrite the existing row with is_deleted=1.
  - orders I/U (master)-> fan out to every CURRENT line of that OrderID
    (read live from OLTP dbo.[Order Details], since a brand-new order's
    detail line may not have reached ClickHouse yet), same new order-level
    attrs, is_deleted=0.
  - orders D          -> mark every existing FactOrders line for that OrderID
    is_deleted=1.

version = the first 8 bytes of the message's LSN, big-endian, as a UInt64 --
see streaming/producer/producer.py's module docstring for why (idempotent
replay: same LSN always produces the same version, so at-least-once Kafka
delivery can't create spurious extra versions).

Order-level context for a detail-only change (CustomerID, EmployeeID,
ShipVia, ship-to address, dates, Freight, ShipName) isn't in the
order_details CDC message at all -- it's read live from OLTP dbo.Orders by
OrderID, exactly like the Phase 3 batch job did.
"""
import json
import logging
import os
import time
import urllib.request
from datetime import date, datetime, timezone

import clickhouse_connect
import pymssql
from confluent_kafka import Consumer
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("cdc-consumer")

MSSQL_HOST = os.environ["MSSQL_HOST"]
MSSQL_PORT = int(os.environ.get("MSSQL_PORT", "1433"))
MSSQL_DB = os.environ.get("MSSQL_DB", "Northwind")
MSSQL_USER = os.environ["MSSQL_USER"]
MSSQL_PASSWORD = os.environ["MSSQL_PASSWORD"]

CH_HOST = os.environ["CH_HOST"]
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "NorthwindDW")

MONGO_HOST = os.environ.get("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.environ.get("MONGO_PORT", "27017"))
MONGO_USER = os.environ.get("MONGO_USER", "")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD", "")

ES_URL = os.environ.get("ES_URL", "http://elasticsearch:9200")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "nw-cdc-consumer")
TOPIC_ORDERS = "nw.cdc.orders"
TOPIC_ORDER_DETAILS = "nw.cdc.order_details"
TOPIC_SUPPLIERS = "nw.cdc.suppliers"

FAR_FUTURE_DATE = date(2099, 12, 31)

FACT_COLUMNS = [
    "OrderID", "ProductKey", "CustomerKey", "EmployeeKey", "ShipperKey", "GeographyKey",
    "ShipName", "OrderDateKey", "RequiredDateKey", "ShippedDateKey",
    "UnitPrice", "Quantity", "Discount", "Freight", "is_deleted", "version",
]

DIM_INFO = {
    "product": {"table": "NorthwindDW.DimProducts", "key": "ProductKey",
                "business_key": "ProductID", "business_type": "Int32", "has_current": True},
    "customer": {"table": "NorthwindDW.DimCustomer", "key": "CustomerKey",
                 "business_key": "CustomerID", "business_type": "String", "has_current": True},
    "employee": {"table": "NorthwindDW.DimEmployees", "key": "EmployeeKey",
                 "business_key": "EmployeeID", "business_type": "Int32", "has_current": True},
    "shipper": {"table": "NorthwindDW.DimShippers", "key": "ShipperKey",
                "business_key": "ShipperID", "business_type": "Int32", "has_current": False},
}


def lsn_to_version(lsn_hex):
    return int.from_bytes(bytes.fromhex(lsn_hex)[:8], "big")


def date_key(d):
    if d is None:
        return None
    if isinstance(d, str):
        d = d[:10]
        return int(d.replace("-", ""))
    return int(d.strftime("%Y%m%d"))


def lookup_dim_key(client, dim, business_value):
    info = DIM_INFO[dim]
    current_filter = "is_current = 1 AND " if info["has_current"] else ""
    q = (f"SELECT {info['key']} FROM {info['table']} FINAL "
         f"WHERE {current_filter}{info['business_key']} = {{bv:{info['business_type']}}} LIMIT 1")
    res = client.query(q, parameters={"bv": business_value})
    return res.result_rows[0][0] if res.result_rows else None


def next_surrogate_key(client, table, key_col):
    res = client.query(f"SELECT max({key_col}) FROM {table} FINAL")
    max_key = res.result_rows[0][0] or 0
    return max_key + 1


def infer_dim_placeholder(client, dim, business_value, version):
    info = DIM_INFO[dim]
    new_key = next_surrogate_key(client, info["table"], info["key"])

    if dim == "product":
        row = [new_key, business_value, "Unknown", 0, None, None, None, 0, 0,
               1, date.today(), FAR_FUTURE_DATE, version, 1]
        cols = ["ProductKey", "ProductID", "ProductName", "SupplierKey", "CategoryID", "CategoryName",
                "QuantityPerUnit", "UnitPrice", "Discontinued", "is_current", "Startdate", "Enddate",
                "version", "is_inferred"]
    elif dim == "customer":
        row = [new_key, business_value, "Unknown", None, None, None, 0,
               1, date.today(), FAR_FUTURE_DATE, version, 1]
        cols = ["CustomerKey", "CustomerID", "CompanyName", "ContactName", "ContactTitle", "Phone",
                "GeographyKey", "is_current", "Startdate", "Enddate", "version", "is_inferred"]
    elif dim == "employee":
        row = [new_key, business_value, "Unknown", "Unknown", None, None, None, None, 0, None,
               1, date.today(), FAR_FUTURE_DATE, version, 1]
        cols = ["EmployeeKey", "EmployeeID", "LastName", "FirstName", "Title", "TitleOfCourtesy",
                "BirthDate", "HireDate", "GeographyKey", "ManagerKey", "is_current", "Startdate",
                "Enddate", "version", "is_inferred"]
    elif dim == "shipper":
        row = [new_key, business_value, "Unknown", None, version, 1]
        cols = ["ShipperKey", "ShipperID", "CompanyName", "Phone", "version", "is_inferred"]
    else:
        raise ValueError(f"unknown dim {dim}")

    client.insert(info["table"], [row], column_names=cols)
    log.info("inferred placeholder: dim=%s business_key=%s new_key=%s", dim, business_value, new_key)
    return new_key


def lookup_or_infer(client, dim, business_value, version):
    if business_value is None:
        return 0
    key = lookup_dim_key(client, dim, business_value)
    if key is not None:
        return key
    return infer_dim_placeholder(client, dim, business_value, version)


def lookup_or_infer_geography(client, country, region, city, postal_code, version):
    conds = []
    params = {}
    for name, val, typ in [("Country", country, "String"), ("Region", region, "Nullable(String)"),
                           ("City", city, "String"), ("PostalCode", postal_code, "Nullable(String)")]:
        if val is None:
            conds.append(f"{name} IS NULL")
        else:
            params[name.lower()] = val
            conds.append(f"{name} = {{{name.lower()}:{typ}}}")
    where = " AND ".join(conds)
    q = f"SELECT GeographyKey FROM NorthwindDW.DimGeography FINAL WHERE is_current = 1 AND {where} LIMIT 1"
    res = client.query(q, parameters=params)
    if res.result_rows:
        return res.result_rows[0][0]

    new_key = next_surrogate_key(client, "NorthwindDW.DimGeography", "GeographyKey")
    row = [new_key, city or "Unknown", region, country or "Unknown", postal_code,
           1, date.today(), FAR_FUTURE_DATE, version, 1]
    cols = ["GeographyKey", "City", "Region", "Country", "PostalCode",
            "is_current", "Startdate", "Enddate", "version", "is_inferred"]
    client.insert("NorthwindDW.DimGeography", [row], column_names=cols)
    log.info("inferred placeholder: dim=geography (%s,%s,%s,%s) new_key=%s", country, region, city, postal_code, new_key)
    return new_key


def get_current_fact_row(client, order_id, product_key):
    q = (f"SELECT {','.join(FACT_COLUMNS)} FROM NorthwindDW.FactOrders FINAL "
         f"WHERE OrderID = {{oid:Int32}} AND ProductKey = {{pk:UInt32}} AND is_deleted = 0")
    res = client.query(q, parameters={"oid": order_id, "pk": product_key})
    if not res.result_rows:
        return None
    return dict(zip(FACT_COLUMNS, res.result_rows[0]))


def get_current_fact_rows_for_order(client, order_id):
    q = (f"SELECT {','.join(FACT_COLUMNS)} FROM NorthwindDW.FactOrders FINAL "
         f"WHERE OrderID = {{oid:Int32}} AND is_deleted = 0")
    res = client.query(q, parameters={"oid": order_id})
    return [dict(zip(FACT_COLUMNS, r)) for r in res.result_rows]


def write_fact_row(client, row):
    client.insert("NorthwindDW.FactOrders", [[row[c] for c in FACT_COLUMNS]], column_names=FACT_COLUMNS)


def get_live_order_context(mssql_conn, order_id):
    cur = mssql_conn.cursor(as_dict=True)
    cur.execute(
        "SELECT OrderID, CustomerID, EmployeeID, ShipVia, ShipName, ShipCountry, ShipRegion, "
        "ShipCity, ShipPostalCode, OrderDate, RequiredDate, ShippedDate, Freight "
        "FROM dbo.Orders WHERE OrderID = %s",
        (order_id,),
    )
    row = cur.fetchone()
    cur.close()
    return row


def get_live_order_details(mssql_conn, order_id):
    cur = mssql_conn.cursor(as_dict=True)
    cur.execute(
        "SELECT ProductID, UnitPrice, Quantity, Discount FROM dbo.[Order Details] WHERE OrderID = %s",
        (order_id,),
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def build_fact_line(client, order_id, order_ctx, detail, version):
    product_key = lookup_or_infer(client, "product", detail["ProductID"], version)
    customer_key = lookup_or_infer(client, "customer", order_ctx["CustomerID"], version)
    employee_key = lookup_or_infer(client, "employee", order_ctx["EmployeeID"], version)
    shipper_key = lookup_or_infer(client, "shipper", order_ctx["ShipVia"], version)
    geography_key = lookup_or_infer_geography(
        client, order_ctx["ShipCountry"], order_ctx["ShipRegion"],
        order_ctx["ShipCity"], order_ctx["ShipPostalCode"], version,
    )
    return {
        "OrderID": order_id,
        "ProductKey": product_key,
        "CustomerKey": customer_key,
        "EmployeeKey": employee_key,
        "ShipperKey": shipper_key,
        "GeographyKey": geography_key,
        "ShipName": order_ctx["ShipName"],
        "OrderDateKey": date_key(order_ctx["OrderDate"]),
        "RequiredDateKey": date_key(order_ctx["RequiredDate"]),
        "ShippedDateKey": date_key(order_ctx["ShippedDate"]),
        "UnitPrice": detail["UnitPrice"],
        "Quantity": detail["Quantity"],
        "Discount": detail["Discount"],
        "Freight": order_ctx["Freight"],
        "is_deleted": 0,
        "version": version,
    }


def apply_order_details_change(client, mssql_conn, msg):
    after = msg["after"]
    order_id = int(after["OrderID"])
    product_id = int(after["ProductID"])
    version = lsn_to_version(msg["lsn"])

    if msg["op"] == "D":
        product_key = lookup_dim_key(client, "product", product_id)
        if product_key is None:
            return
        existing = get_current_fact_row(client, order_id, product_key)
        if existing is None:
            return
        existing["is_deleted"] = 1
        existing["version"] = version
        write_fact_row(client, existing)
        log.info("order_details DELETE order=%s product=%s", order_id, product_id)
        return

    order_ctx = get_live_order_context(mssql_conn, order_id)
    if order_ctx is None:
        log.warning("order_details %s for order=%s but order no longer exists live; skipping", msg["op"], order_id)
        return

    detail = {"ProductID": product_id, "UnitPrice": after["UnitPrice"],
              "Quantity": after["Quantity"], "Discount": after["Discount"]}
    row = build_fact_line(client, order_id, order_ctx, detail, version)
    write_fact_row(client, row)
    log.info("order_details %s order=%s product=%s qty=%s", msg["op"], order_id, product_id, after["Quantity"])


def apply_orders_change(client, mssql_conn, msg):
    after = msg["after"]
    order_id = int(after["OrderID"])
    version = lsn_to_version(msg["lsn"])

    if msg["op"] == "D":
        rows = get_current_fact_rows_for_order(client, order_id)
        for row in rows:
            row["is_deleted"] = 1
            row["version"] = version
            write_fact_row(client, row)
        log.info("orders DELETE order=%s fanned out to %d lines", order_id, len(rows))
        return

    detail_rows = get_live_order_details(mssql_conn, order_id)
    if not detail_rows:
        log.warning("orders %s for order=%s but it has no live detail lines; nothing to fan out yet", msg["op"], order_id)
        return

    for d in detail_rows:
        detail = {"ProductID": d["ProductID"], "UnitPrice": d["UnitPrice"],
                  "Quantity": d["Quantity"], "Discount": d["Discount"]}
        row = build_fact_line(client, order_id, after, detail, version)
        write_fact_row(client, row)
    log.info("orders %s order=%s fanned out to %d lines", msg["op"], order_id, len(detail_rows))


def apply_suppliers_change(client, msg):
    """
    Scoped proof-of-concept (Phase 4, Task 6): real-time delete detection for
    ONE dimension. The Phase 2 batch SCD job (load_dim_suppliers.py) only ever
    notices a business key that is new or changed -- it never notices one that
    disappeared from the source, since it just compares against the latest
    staging extract. This closes that gap for Suppliers specifically: an OLTP
    delete expires the current DimSuppliers row (is_current=0, Enddate=today)
    with no replacement row inserted. Inserts/updates on this topic are
    intentionally ignored -- those are still the batch job's job.
    """
    if msg["op"] != "D":
        return
    supplier_id = int(msg["after"]["SupplierID"])
    version = lsn_to_version(msg["lsn"])

    cols = ["SupplierKey", "SupplierID", "CompanyName", "ContactName", "ContactTitle", "Address", "Phone",
            "GeographyKey", "Startdate"]
    res = client.query(
        f"SELECT {','.join(cols)} FROM NorthwindDW.DimSuppliers FINAL "
        "WHERE is_current = 1 AND SupplierID = {sid:Int32} LIMIT 1",
        parameters={"sid": supplier_id},
    )
    if not res.result_rows:
        return

    row = dict(zip(cols, res.result_rows[0]))
    row["is_current"] = 0
    row["Enddate"] = date.today()
    row["version"] = version
    write_cols = cols + ["is_current", "Enddate", "version"]
    client.insert("NorthwindDW.DimSuppliers", [[row[c] for c in write_cols]], column_names=write_cols)
    log.info("suppliers DELETE supplier_id=%s (SupplierKey=%s) expired", supplier_id, row["SupplierKey"])


def get_mongo_collection():
    """Durable CDC event log (Phase 6B). Best-effort: a Mongo hiccup must
    never block the actual ClickHouse pipeline (see log_event's own
    try/except)."""
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client["monitoring"]["cdc_events"]


def _parse_event_ts(ts_str):
    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _index_to_es(doc):
    """Best-effort: mirrors the same monitoring doc into Elasticsearch
    (nw-consumer-events-*), giving Kibana a genuine processing-outcome
    source (status/error/latency_ms) distinct from the raw Kafka-mirrored
    nw-cdc-events-* index Logstash writes."""
    index = f"nw-consumer-events-{doc['processed_ts'].strftime('%Y.%m.%d')}"
    es_doc = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in doc.items()}
    body = json.dumps(es_doc).encode("utf-8")
    req = urllib.request.Request(
        f"{ES_URL}/{index}/_doc", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()


def log_event(mongo_col, payload, status, error=None):
    after = payload.get("after") or {}
    event_ts = _parse_event_ts(payload["ts"]) if payload.get("ts") else None
    processed_ts = datetime.now(timezone.utc)
    latency_ms = (processed_ts - event_ts).total_seconds() * 1000 if event_ts else None
    doc = {
        "op": payload.get("op"),
        "table": payload.get("table"),
        "order_id": after.get("OrderID"),
        "product_id": after.get("ProductID"),
        "lsn": payload.get("lsn"),
        "event_ts": event_ts,
        "processed_ts": processed_ts,
        "latency_ms": latency_ms,
        "status": status,
    }
    if error:
        doc["error"] = error

    try:
        mongo_col.insert_one(dict(doc))
    except Exception:
        log.exception("failed to write monitoring event to MongoDB (non-fatal)")

    try:
        _index_to_es(doc)
    except Exception:
        log.exception("failed to write monitoring event to Elasticsearch (non-fatal)")


def main():
    ch_client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE,
    )
    mssql_conn = pymssql.connect(
        server=MSSQL_HOST, port=str(MSSQL_PORT), database=MSSQL_DB,
        user=MSSQL_USER, password=MSSQL_PASSWORD, autocommit=True,
    )

    mongo_col = get_mongo_collection()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_ORDERS, TOPIC_ORDER_DETAILS, TOPIC_SUPPLIERS])
    log.info("CDC consumer starting: kafka=%s group=%s ch=%s:%s mssql=%s:%s/%s mongo=%s:%s",
              KAFKA_BOOTSTRAP, GROUP_ID, CH_HOST, CH_PORT, MSSQL_HOST, MSSQL_PORT, MSSQL_DB,
              MONGO_HOST, MONGO_PORT)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue

            try:
                payload = json.loads(msg.value())
            except Exception:
                log.exception("failed to parse message JSON, skipping: %s", msg.value())
                consumer.commit(msg)
                continue

            try:
                t0 = time.time()
                if payload["table"] == "OrderDetails":
                    apply_order_details_change(ch_client, mssql_conn, payload)
                elif payload["table"] == "Orders":
                    apply_orders_change(ch_client, mssql_conn, payload)
                elif payload["table"] == "Suppliers":
                    apply_suppliers_change(ch_client, payload)
                else:
                    log.warning("unknown table in message: %s", payload.get("table"))
                log.info("applied in %.3fs (lsn=%s)", time.time() - t0, payload.get("lsn"))
                log_event(mongo_col, payload, status="success")
                consumer.commit(msg)
            except Exception as exc:
                log.exception("failed to process message, will retry: %s", msg.value())
                log_event(mongo_col, payload, status="error", error=str(exc))
                time.sleep(1.0)
    finally:
        consumer.close()
        mssql_conn.close()


if __name__ == "__main__":
    main()
