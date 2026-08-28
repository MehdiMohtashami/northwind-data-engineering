"""
Incremental CDC load into FactOrders for the LSN window [from_lsn, to_lsn].

Reads 'all changes' (not net changes) from both CDC capture instances via the
CDC table-valued functions, nets them down to one row per changed key within
this window (by max __$start_lsn, after dropping op=3 before-images), then:

  - Order Details changes (line-level): insert/update -> upsert that one line;
    delete -> mark that line is_deleted=1.
  - Orders changes (master-level): insert/update -> re-emit EVERY current
    detail line of that order with the new order-level attributes (fan-out);
    delete -> mark EVERY existing FactOrders line of that order is_deleted=1.

__$operation: 1=delete, 2=insert, 3=update(before, dropped), 4=update(after),
5=update-merge. 2/4/5 are all treated as "upsert" (after-image).

If the same (OrderID, ProductID) is both directly changed AND swept up by an
order-level fan-out in the same window, the direct change wins.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import scd_lib

ORDER_ATTR_COLS = [
    "CustomerID", "EmployeeID", "ShipperID", "ShipName",
    "Country", "Region", "City", "PostalCode",
    "OrderDate", "RequiredDate", "ShippedDate", "Freight",
]
DETAIL_ATTR_COLS = ["UnitPrice", "Quantity", "Discount"]


def keep_latest_per_key(df, key_cols):
    w = Window.partitionBy(*key_cols).orderBy(F.col("lsn").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def main():
    parser = scd_lib.build_arg_parser(__doc__, needs_mssql=True)
    parser.add_argument("--from-lsn-hex", required=True)
    parser.add_argument("--to-lsn-hex", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_fact_orders_cdc").getOrCreate()
    scd_lib.init_from_args(spark, args)

    from_lsn = f"0x{args.from_lsn_hex}"
    to_lsn = f"0x{args.to_lsn_hex}"

    od_raw = scd_lib.read_mssql(f"""
        (SELECT __$operation AS op, __$start_lsn AS lsn,
                OrderID, ProductID, UnitPrice, Quantity, Discount
         FROM cdc.fn_cdc_get_all_changes_dbo_OrderDetails({from_lsn}, {to_lsn}, 'all')) x
    """)
    ord_raw = scd_lib.read_mssql(f"""
        (SELECT __$operation AS op, __$start_lsn AS lsn,
                OrderID, CustomerID, EmployeeID, ShipVia, ShipName,
                ShipCountry, ShipRegion, ShipCity, ShipPostalCode,
                OrderDate, RequiredDate, ShippedDate, Freight
         FROM cdc.fn_cdc_get_all_changes_dbo_Orders({from_lsn}, {to_lsn}, 'all')) x
    """)

    od_net = keep_latest_per_key(od_raw.filter(F.col("op") != 3), ["OrderID", "ProductID"])
    ord_net = keep_latest_per_key(ord_raw.filter(F.col("op") != 3), ["OrderID"])

    od_deletes = od_net.filter(F.col("op") == 1).select("OrderID", "ProductID")
    od_upserts = od_net.filter(F.col("op") != 1).select("OrderID", "ProductID", "UnitPrice", "Quantity", "Discount")

    ord_deletes = ord_net.filter(F.col("op") == 1).select("OrderID").distinct()
    ord_upserts_ids = ord_net.filter(F.col("op") != 1).select("OrderID").distinct()

    if not od_net.take(1) and not ord_net.take(1):
        print("FACT_CDC_RESULT no changes in this LSN window")
        spark.stop()
        return

    touched_ids = sorted({r["OrderID"] for r in od_upserts.select("OrderID").union(ord_upserts_ids).distinct().collect()})
    fanout_ids = sorted({r["OrderID"] for r in ord_upserts_ids.collect()})

    # WHERE-clause fallback keeps these on the JDBC/Catalyst path when there's nothing to
    # fetch (spark.createDataFrame([], schema) would spawn a distributed Python worker
    # even for zero rows, which crashes on a driver/executor Python minor-version
    # mismatch -- see the Phase 2 territories job for the same issue).
    orders_where = f"OrderID IN ({','.join(str(i) for i in touched_ids)})" if touched_ids else "1=0"
    live_orders = scd_lib.read_mssql(f"""
        (SELECT OrderID, CustomerID, EmployeeID, ShipVia AS ShipperID, ShipName,
                ShipCountry AS Country, ShipRegion AS Region, ShipCity AS City,
                ShipPostalCode AS PostalCode, OrderDate, RequiredDate, ShippedDate, Freight
         FROM dbo.Orders WHERE {orders_where}) x
    """)

    details_where = f"OrderID IN ({','.join(str(i) for i in fanout_ids)})" if fanout_ids else "1=0"
    live_details_fanout = scd_lib.read_mssql(f"""
        (SELECT OrderID, ProductID, UnitPrice, Quantity, Discount
         FROM dbo.[Order Details] WHERE {details_where}) x
    """)

    detail_changed_rows = od_upserts.join(live_orders, on="OrderID", how="inner")
    fanout_rows = live_details_fanout.join(live_orders, on="OrderID", how="inner")

    fanout_rows_unique = fanout_rows.join(
        detail_changed_rows.select("OrderID", "ProductID"), on=["OrderID", "ProductID"], how="left_anti"
    )
    to_upsert_raw = detail_changed_rows.unionByName(fanout_rows_unique)

    to_upsert_raw = scd_lib.lookup_or_infer(to_upsert_raw, "NorthwindDW.DimProducts", on="ProductID", key_out="ProductKey")
    to_upsert_raw = scd_lib.lookup_or_infer(to_upsert_raw, "NorthwindDW.DimCustomer", on="CustomerID", key_out="CustomerKey")
    to_upsert_raw = scd_lib.lookup_or_infer(to_upsert_raw, "NorthwindDW.DimEmployees", on="EmployeeID", key_out="EmployeeKey")
    to_upsert_raw = scd_lib.lookup_or_infer(to_upsert_raw, "NorthwindDW.DimShippers", on="ShipperID", key_out="ShipperKey")
    to_upsert_raw = scd_lib.lookup_or_infer(
        to_upsert_raw, "NorthwindDW.DimGeography",
        on=["Country", "Region", "City", "PostalCode"], key_out="GeographyKey",
    ).drop("Country", "Region", "City", "PostalCode")

    version = scd_lib.run_version()
    fact_cols = [
        "OrderID", "ProductKey", "CustomerKey", "EmployeeKey", "ShipperKey", "GeographyKey",
        "ShipName", "OrderDateKey", "RequiredDateKey", "ShippedDateKey",
        "UnitPrice", "Quantity", "Discount", "Freight", "is_deleted", "version",
    ]
    to_upsert = (
        to_upsert_raw
        .withColumn("OrderDateKey", F.date_format(F.col("OrderDate"), "yyyyMMdd").cast("int"))
        .withColumn("RequiredDateKey", F.date_format(F.col("RequiredDate"), "yyyyMMdd").cast("int"))
        .withColumn("ShippedDateKey", F.date_format(F.col("ShippedDate"), "yyyyMMdd").cast("int"))
        .withColumn("is_deleted", F.lit(0).cast("int"))
        .withColumn("version", F.lit(version).cast("long"))
        .select(*fact_cols)
    )

    current_fact = scd_lib.read_ch("NorthwindDW.FactOrders").filter(F.col("is_deleted") == 0)

    od_deletes_resolved = scd_lib.lookup_or_infer(od_deletes, "NorthwindDW.DimProducts", on="ProductID", key_out="ProductKey")
    od_delete_fact_rows = current_fact.join(
        od_deletes_resolved.select("OrderID", "ProductKey"), on=["OrderID", "ProductKey"], how="inner"
    )
    ord_delete_fact_rows = current_fact.join(ord_deletes, on="OrderID", how="inner")

    to_delete = (
        od_delete_fact_rows.unionByName(ord_delete_fact_rows)
        .dropDuplicates(["OrderID", "ProductKey"])
        .withColumn("is_deleted", F.lit(1).cast("int"))
        .withColumn("version", F.lit(version).cast("long"))
        .select(*fact_cols)
    )

    to_upsert_final = to_upsert.join(
        to_delete.select("OrderID", "ProductKey"), on=["OrderID", "ProductKey"], how="left_anti"
    )

    final_write = scd_lib.materialize(to_upsert_final.unionByName(to_delete))
    scd_lib.write_ch_append(final_write, "NorthwindDW.FactOrders")

    n_upsert = to_upsert_final.count()
    n_delete = to_delete.count()
    print(f"FACT_CDC_RESULT upserted={n_upsert} deleted={n_delete}")

    spark.stop()


if __name__ == "__main__":
    main()
