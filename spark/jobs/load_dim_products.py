"""DimProducts load (SCD2): staging.stg_products (+stg_categories for CategoryName)
-> NorthwindDW.DimProducts, SupplierKey resolved from current DimSuppliers."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_dim_products").getOrCreate()
    scd_lib.init_from_args(spark, args)

    products = scd_lib.read_pg("staging.stg_products").select(
        F.col("product_id").cast("int").alias("ProductID"),
        F.col("product_name").alias("ProductName"),
        F.col("supplier_id").cast("int").alias("SupplierID"),
        F.col("category_id").cast("int").alias("CategoryID"),
        F.col("quantity_per_unit").alias("QuantityPerUnit"),
        F.col("unit_price").cast("decimal(19,4)").alias("UnitPrice"),
        F.col("discontinued").cast("int").alias("Discontinued"),
    )
    categories = scd_lib.read_pg("staging.stg_categories").select(
        F.col("category_id").cast("int").alias("CategoryID"),
        F.col("category_name").alias("CategoryName"),
    )

    src = products.join(categories, on="CategoryID", how="left")

    src = scd_lib.lookup(
        src, "NorthwindDW.DimSuppliers", on="SupplierID", key_out="SupplierKey",
    ).drop("SupplierID")

    scd_attrs = ["ProductName", "SupplierKey", "CategoryID", "CategoryName", "QuantityPerUnit", "UnitPrice", "Discontinued"]
    written = scd_lib.scd2_apply(
        src, "NorthwindDW.DimProducts", key_col="ProductKey",
        business_key="ProductID", scd_attrs=scd_attrs,
    )
    print(f"DIM_LOAD_RESULT table=DimProducts rows_written={written.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
