"""
FactEmployeeTerritories load (factless bridge): staging.stg_employee_territories
-> NorthwindDW.FactEmployeeTerritories, keys resolved from current
DimEmployees/DimTerritories.

Every run rewrites the full current set of (EmployeeKey, TerritoryKey) pairs
with is_deleted=0, and any pair that was previously active but has since
disappeared from the source is written again with is_deleted=1 -- both as
new versions of the same (EmployeeKey, TerritoryKey) row, which is this
table's ReplacingMergeTree ORDER BY key.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_fact_employee_territories").getOrCreate()
    scd_lib.init_from_args(spark, args)

    stg = scd_lib.read_pg("staging.stg_employee_territories").select(
        F.col("employee_id").cast("int").alias("EmployeeID"),
        F.col("territory_id").alias("TerritoryID"),
    )

    resolved = scd_lib.lookup(stg, "NorthwindDW.DimEmployees", on="EmployeeID", key_out="EmployeeKey")
    resolved = scd_lib.lookup(resolved, "NorthwindDW.DimTerritories", on="TerritoryID", key_out="TerritoryKey")
    current_pairs = resolved.select("EmployeeKey", "TerritoryKey").dropna()

    version = scd_lib.run_version()

    active_rows = current_pairs.withColumn("is_deleted", F.lit(0).cast("int")).withColumn(
        "version", F.lit(version).cast("long")
    )

    previously_active = (
        scd_lib.read_ch("NorthwindDW.FactEmployeeTerritories")
        .filter(F.col("is_deleted") == 0)
        .select("EmployeeKey", "TerritoryKey")
    )
    removed_pairs = previously_active.join(current_pairs, on=["EmployeeKey", "TerritoryKey"], how="left_anti")
    removed_rows = removed_pairs.withColumn("is_deleted", F.lit(1).cast("int")).withColumn(
        "version", F.lit(version).cast("long")
    )

    to_write = scd_lib.materialize(active_rows.unionByName(removed_rows))
    scd_lib.write_ch_append(to_write, "NorthwindDW.FactEmployeeTerritories")
    print(f"DIM_LOAD_RESULT table=FactEmployeeTerritories rows_written={to_write.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
