"""
DimGeography load: append-only distinct (Country, Region, City, PostalCode)
tuples from staging.stg_geography.

Geography tuples are treated as immutable dimension members (no SCD2 history
here -- matches the professor's DimGeography, which has no Startdate/Enddate
at all). New tuples get new surrogate keys; existing tuples are left alone.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_dim_geography").getOrCreate()
    scd_lib.init_from_args(spark, args)

    src = (
        scd_lib.read_pg("staging.stg_geography")
        .select(
            F.col("country").alias("Country"),
            F.col("region").alias("Region"),
            F.col("city").alias("City"),
            F.col("postal_code").alias("PostalCode"),
        )
        .distinct()
    )

    current = scd_lib.read_ch("NorthwindDW.DimGeography").select("Country", "Region", "City", "PostalCode")

    join_cond = (
        src["Country"].eqNullSafe(current["Country"])
        & src["Region"].eqNullSafe(current["Region"])
        & src["City"].eqNullSafe(current["City"])
        & src["PostalCode"].eqNullSafe(current["PostalCode"])
    )
    new_tuples = src.join(current, on=join_cond, how="left_anti")

    version = scd_lib.run_version()
    to_insert = scd_lib.materialize(
        scd_lib.assign_surrogate_keys(new_tuples, "NorthwindDW.DimGeography", "GeographyKey")
        .withColumn("is_current", F.lit(1).cast("int"))
        .withColumn("Startdate", F.current_date())
        .withColumn("Enddate", F.to_date(F.lit(scd_lib.FAR_FUTURE_DATE)))
        .withColumn("version", F.lit(version).cast("long"))
    )

    scd_lib.write_ch_append(to_insert, "NorthwindDW.DimGeography")
    print(f"DIM_LOAD_RESULT table=DimGeography rows_written={to_insert.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
