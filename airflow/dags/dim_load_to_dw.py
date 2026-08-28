from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

JARS = "/opt/airflow/spark/jars/postgresql-42.7.3.jar,/opt/airflow/spark/jars/clickhouse-jdbc-0.6.5-shaded.jar"

pg_conn = BaseHook.get_connection("pg_staging")
ch_conn = BaseHook.get_connection("clickhouse_dw")

PG_JDBC_URL = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
CH_JDBC_URL = f"jdbc:clickhouse://{ch_conn.host}:{ch_conn.port}/{ch_conn.schema}"

COMMON_ARGS = [
    "--pg-jdbc-url", PG_JDBC_URL,
    "--pg-user", pg_conn.login,
    "--pg-password", pg_conn.password,
    "--ch-jdbc-url", CH_JDBC_URL,
    "--ch-user", ch_conn.login,
    "--ch-password", ch_conn.password,
]


def spark_job(task_id, job_file):
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/opt/airflow/spark/jobs/{job_file}",
        conn_id="spark_default",
        jars=JARS,
        application_args=COMMON_ARGS,
        verbose=True,
    )


with DAG(
    dag_id="dim_load_to_dw",
    description="Load ClickHouse NorthwindDW dimensions + FactEmployeeTerritories bridge from Postgres staging via PySpark SCD jobs.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["phase2", "dim-load"],
) as dag:

    dim_geography = spark_job("load_dim_geography", "load_dim_geography.py")
    dim_suppliers = spark_job("load_dim_suppliers", "load_dim_suppliers.py")
    dim_customer = spark_job("load_dim_customer", "load_dim_customer.py")
    dim_shippers = spark_job("load_dim_shippers", "load_dim_shippers.py")
    dim_territories = spark_job("load_dim_territories", "load_dim_territories.py")
    dim_employees = spark_job("load_dim_employees", "load_dim_employees.py")
    dim_products = spark_job("load_dim_products", "load_dim_products.py")
    fact_employee_territories = spark_job("load_fact_employee_territories", "load_fact_employee_territories.py")

    dim_geography >> [dim_suppliers, dim_customer, dim_shippers, dim_territories, dim_employees]
    dim_suppliers >> dim_products
    [dim_employees, dim_territories] >> fact_employee_territories
