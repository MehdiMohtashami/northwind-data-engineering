"""
DimTerritories load (SCD2): staging.stg_territories -> NorthwindDW.DimTerritories,
RegionDescription resolved via a Territories->Region join.

NOTE: Northwind's `Region` table (RegionID -> RegionDescription) was never
extracted into a staging table in Phase 1 (only region_id made it into
stg_territories, not the description). Region has exactly 4 static rows in
the standard Northwind dataset (Eastern/Western/Northern/Southern) that never
change, so rather than adding a whole staging table + extraction task for 4
constant rows, this job hardcodes that lookup inline. Flagged in the Phase 2
report -- a proper stg_region table would be the cleaner fix if Region ever
stops being static.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib

REGION_DESCRIPTIONS = {
    1: "Eastern",
    2: "Western",
    3: "Northern",
    4: "Southern",
}


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_dim_territories").getOrCreate()
    scd_lib.init_from_args(spark, args)

    territories = scd_lib.read_pg("staging.stg_territories").select(
        F.col("territory_id").alias("TerritoryID"),
        F.col("territory_description").alias("TerritoryDescription"),
        F.col("region_id").cast("int").alias("region_id"),
    )

    # Pure Catalyst expression (no spark.createDataFrame from a local Python list,
    # and no UDF) -- avoids spawning a distributed Python worker, which would hit a
    # Python-minor-version mismatch between this driver (3.10) and the bitnami
    # Spark workers (3.12).
    items = list(REGION_DESCRIPTIONS.items())
    region_expr = F.when(F.col("region_id") == items[0][0], F.lit(items[0][1]))
    for rid, desc in items[1:]:
        region_expr = region_expr.when(F.col("region_id") == rid, F.lit(desc))
    region_expr = region_expr.otherwise(F.lit(None))

    src = territories.withColumn("RegionDescription", region_expr).drop("region_id")

    scd_attrs = ["RegionDescription", "TerritoryDescription"]
    written = scd_lib.scd2_apply(
        src, "NorthwindDW.DimTerritories", key_col="TerritoryKey",
        business_key="TerritoryID", scd_attrs=scd_attrs,
    )
    print(f"DIM_LOAD_RESULT table=DimTerritories rows_written={written.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
