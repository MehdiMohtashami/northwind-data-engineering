"""DimCustomer load (SCD2): staging.stg_customers -> NorthwindDW.DimCustomer,
GeographyKey resolved by matching the geography tuple."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_dim_customer").getOrCreate()
    scd_lib.init_from_args(spark, args)

    src = scd_lib.read_pg("staging.stg_customers").select(
        F.col("customer_id").alias("CustomerID"),
        F.col("company_name").alias("CompanyName"),
        F.col("contact_name").alias("ContactName"),
        F.col("contact_title").alias("ContactTitle"),
        F.col("phone").alias("Phone"),
        F.col("country").alias("Country"),
        F.col("region").alias("Region"),
        F.col("city").alias("City"),
        F.col("postal_code").alias("PostalCode"),
    )

    src = scd_lib.lookup(
        src, "NorthwindDW.DimGeography",
        on=["Country", "Region", "City", "PostalCode"],
        key_out="GeographyKey",
    ).drop("Country", "Region", "City", "PostalCode")

    scd_attrs = ["CompanyName", "ContactName", "ContactTitle", "Phone", "GeographyKey"]
    written = scd_lib.scd2_apply(
        src, "NorthwindDW.DimCustomer", key_col="CustomerKey",
        business_key="CustomerID", scd_attrs=scd_attrs,
    )
    print(f"DIM_LOAD_RESULT table=DimCustomer rows_written={written.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
