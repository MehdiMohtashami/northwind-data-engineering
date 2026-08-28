"""
DimEmployees load, two-pass:

  Pass 1 -- SCD2 on descriptive attrs (Title/TitleOfCourtesy/etc, GeographyKey);
            ManagerKey is left NULL for any newly-inserted row (self-reference
            can't be resolved until every employee already has a current
            surrogate key, including employees inserted in this same run).
  Pass 2 -- resolve ManagerKey: join stg_employees.reports_to (raw source
            manager EmployeeID) to the now-current DimEmployees to get the
            manager's current EmployeeKey, and rewrite (as a new version,
            same EmployeeKey) any row whose ManagerKey needs to change.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import scd_lib


def main():
    parser = scd_lib.build_arg_parser(__doc__)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("load_dim_employees").getOrCreate()
    scd_lib.init_from_args(spark, args)

    stg = scd_lib.read_pg("staging.stg_employees").select(
        F.col("employee_id").cast("int").alias("EmployeeID"),
        F.col("last_name").alias("LastName"),
        F.col("first_name").alias("FirstName"),
        F.col("title").alias("Title"),
        F.col("title_of_courtesy").alias("TitleOfCourtesy"),
        F.col("birth_date").cast("date").alias("BirthDate"),
        F.col("hire_date").cast("date").alias("HireDate"),
        F.col("reports_to").cast("int").alias("reports_to"),
        F.col("country").alias("Country"),
        F.col("region").alias("Region"),
        F.col("city").alias("City"),
        F.col("postal_code").alias("PostalCode"),
    )

    pass1_src = scd_lib.lookup(
        stg, "NorthwindDW.DimGeography",
        on=["Country", "Region", "City", "PostalCode"],
        key_out="GeographyKey",
    ).drop("Country", "Region", "City", "PostalCode", "reports_to")

    scd_attrs = ["LastName", "FirstName", "Title", "TitleOfCourtesy", "BirthDate", "HireDate", "GeographyKey"]
    pass1_written = scd_lib.scd2_apply(
        pass1_src, "NorthwindDW.DimEmployees", key_col="EmployeeKey",
        business_key="EmployeeID", scd_attrs=scd_attrs,
    )
    print(f"DIM_LOAD_RESULT table=DimEmployees pass=1 rows_written={pass1_written.count()}")

    # --- Pass 2: resolve ManagerKey from raw ReportsTo -> current EmployeeKey ---
    current_emp = scd_lib.read_ch("NorthwindDW.DimEmployees").filter(F.col("is_current") == 1)

    reports_to = stg.select("EmployeeID", "reports_to")
    manager_lookup = current_emp.select(
        F.col("EmployeeID").alias("_mgr_employee_id"),
        F.col("EmployeeKey").alias("ManagerKey_new"),
    )
    resolved = reports_to.join(
        manager_lookup, reports_to["reports_to"] == manager_lookup["_mgr_employee_id"], "left",
    ).select("EmployeeID", "ManagerKey_new")

    joined = current_emp.alias("c").join(resolved.alias("r"), on="EmployeeID", how="left")
    needs_update = joined.filter(~F.col("c.ManagerKey").eqNullSafe(F.col("r.ManagerKey_new")))

    pass2_version = scd_lib.run_version() + 1
    carry_cols = [c for c in current_emp.columns if c not in ("ManagerKey", "version")]
    pass2_rows = scd_lib.materialize(
        needs_update.select(
            *[F.col(f"c.{c}").alias(c) for c in carry_cols],
            F.col("r.ManagerKey_new").alias("ManagerKey"),
        )
        .withColumn("version", F.lit(pass2_version).cast("long"))
    )

    scd_lib.write_ch_append(pass2_rows, "NorthwindDW.DimEmployees")
    print(f"DIM_LOAD_RESULT table=DimEmployees pass=2 rows_written={pass2_rows.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
