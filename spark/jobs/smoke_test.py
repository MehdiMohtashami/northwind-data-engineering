"""Smoke test: read staging.stg_suppliers from Postgres via JDBC and print the row count."""
import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--table", default="staging.stg_suppliers")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nw_de_smoke_test").getOrCreate()

    df = spark.read.format("jdbc").options(
        url=args.jdbc_url,
        dbtable=args.table,
        user=args.user,
        password=args.password,
        driver="org.postgresql.Driver",
    ).load()

    count = df.count()
    print(f"SMOKE_TEST_RESULT table={args.table} row_count={count}")

    spark.stop()


if __name__ == "__main__":
    main()
