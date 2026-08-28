from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_smoke_test",
    description="One-off smoke test: PySpark reads staging.stg_suppliers via JDBC on the standalone cluster.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["phase2", "smoke-test"],
) as dag:

    pg_conn = BaseHook.get_connection("pg_staging")
    jdbc_url = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"

    smoke_test = SparkSubmitOperator(
        task_id="smoke_test",
        application="/opt/airflow/spark/jobs/smoke_test.py",
        conn_id="spark_default",
        jars="/opt/airflow/spark/jars/postgresql-42.7.3.jar,/opt/airflow/spark/jars/clickhouse-jdbc-0.6.5-shaded.jar",
        application_args=[
            "--jdbc-url", jdbc_url,
            "--user", pg_conn.login,
            "--password", pg_conn.password,
            "--table", "staging.stg_suppliers",
        ],
        verbose=True,
    )
