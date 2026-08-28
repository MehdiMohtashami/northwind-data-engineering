"""DimShippers load (SCD1): staging.stg_shippers -> NorthwindDW.DimShippers."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_dim_shippers").getOrCreate()
    scd_lib.init_from_args(spark, args)

    src = scd_lib.read_pg("staging.stg_shippers").select(
        F.col("shipper_id").cast("int").alias("ShipperID"),
        F.col("company_name").alias("CompanyName"),
        F.col("phone").alias("Phone"),
    )

    written = scd_lib.scd1_apply(
        src, "NorthwindDW.DimShippers", key_col="ShipperKey", business_key="ShipperID",
    )
    print(f"DIM_LOAD_RESULT table=DimShippers rows_written={written.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
