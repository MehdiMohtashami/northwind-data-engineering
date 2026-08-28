"""
FactOrders initial full load (grain = one Order Detail line):
staging.stg_orders JOIN staging.stg_order_details on order_id.

Dimension keys resolved via lookups to CURRENT (is_current=1) dims. Freight
is order-level and repeated on every line, matching the professor's design
(see docs/phase3_freight_rule.md) -- this falls out naturally since
stg_orders already carries one freight value per order_id, replicated
across lines by the join itself.

NOTE on GeographyKey: the ship-to tuple is matched on (Country, Region, City,
PostalCode) only, NOT including street address. DimGeography has no Address
column -- adding one would need Address threaded all the way back through
stg_geography and the extraction DAG (Phase 0/1), which is out of scope here.
This is the same tuple already used for the Suppliers/Customers/Employees
GeographyKey lookups in Phase 2.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_fact_orders_initial").getOrCreate()
    scd_lib.init_from_args(spark, args)

    orders = scd_lib.read_pg("staging.stg_orders").select(
        F.col("order_id").cast("int").alias("OrderID"),
        F.col("customer_id").alias("CustomerID"),
        F.col("employee_id").cast("int").alias("EmployeeID"),
        F.col("ship_via").cast("int").alias("ShipperID"),
        F.col("ship_name").alias("ShipName"),
        F.col("ship_country").alias("Country"),
        F.col("ship_region").alias("Region"),
        F.col("ship_city").alias("City"),
        F.col("ship_postal_code").alias("PostalCode"),
        F.col("order_date").cast("date").alias("OrderDate"),
        F.col("required_date").cast("date").alias("RequiredDate"),
        F.col("shipped_date").cast("date").alias("ShippedDate"),
        F.col("freight").cast("decimal(19,4)").alias("Freight"),
    )
    details = scd_lib.read_pg("staging.stg_order_details").select(
        F.col("order_id").cast("int").alias("OrderID"),
        F.col("product_id").cast("int").alias("ProductID"),
        F.col("unit_price").cast("decimal(19,4)").alias("UnitPrice"),
        F.col("quantity").cast("int").alias("Quantity"),
        F.col("discount").cast("float").alias("Discount"),
    )

    src = orders.join(details, on="OrderID", how="inner")

    src = scd_lib.lookup(src, "NorthwindDW.DimProducts", on="ProductID", key_out="ProductKey")
    src = scd_lib.lookup(src, "NorthwindDW.DimCustomer", on="CustomerID", key_out="CustomerKey")
    src = scd_lib.lookup(src, "NorthwindDW.DimEmployees", on="EmployeeID", key_out="EmployeeKey")
    src = scd_lib.lookup(src, "NorthwindDW.DimShippers", on="ShipperID", key_out="ShipperKey")
    src = scd_lib.lookup(
        src, "NorthwindDW.DimGeography",
        on=["Country", "Region", "City", "PostalCode"],
        key_out="GeographyKey",
    ).drop("Country", "Region", "City", "PostalCode")

    version = scd_lib.run_version()
    fact = (
        src.withColumn("OrderDateKey", F.date_format(F.col("OrderDate"), "yyyyMMdd").cast("int"))
        .withColumn("RequiredDateKey", F.date_format(F.col("RequiredDate"), "yyyyMMdd").cast("int"))
        .withColumn("ShippedDateKey", F.date_format(F.col("ShippedDate"), "yyyyMMdd").cast("int"))
        .withColumn("is_deleted", F.lit(0).cast("int"))
        .withColumn("version", F.lit(version).cast("long"))
        .select(
            "OrderID", "ProductKey", "CustomerKey", "EmployeeKey", "ShipperKey", "GeographyKey",
            "ShipName", "OrderDateKey", "RequiredDateKey", "ShippedDateKey",
            "UnitPrice", "Quantity", "Discount", "Freight", "is_deleted", "version",
        )
    )
    fact = scd_lib.materialize(fact)

    scd_lib.write_ch_append(fact, "NorthwindDW.FactOrders")
    print(f"FACT_LOAD_RESULT table=FactOrders rows_written={fact.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
