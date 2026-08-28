"""One-shot initial full load of FactOrders, then seed the CDC watermark so
incremental loads only pick up changes from this point forward."""
from datetime import datetime

from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

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


def seed_cdc_watermark():
    hook = MsSqlHook(mssql_conn_id="mssql_northwind")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
    max_lsn = cur.fetchone()[0]
    cur.execute(
        "UPDATE ETL_Settings.dbo.cdc_state "
        "SET last_lsn = %s, last_processed_at = SYSUTCDATETIME() "
        "WHERE source_table IN ('Orders', 'Order Details')",
        (max_lsn,),
    )
    conn.commit()
    print(f"CDC watermark seeded to LSN {max_lsn.hex()} for Orders and Order Details")
    cur.close()
    conn.close()


with DAG(
    dag_id="fact_initial_load",
    description="One-shot: full FactOrders load from staging, then seed the CDC watermark.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["phase3", "fact-init"],
) as dag:

    load_fact_orders = SparkSubmitOperator(
        task_id="load_fact_orders_initial",
        application="/opt/airflow/spark/jobs/load_fact_orders_initial.py",
        conn_id="spark_default",
        jars=JARS,
        application_args=COMMON_ARGS,
        verbose=True,
    )

    seed_watermark = PythonOperator(
        task_id="seed_cdc_watermark",
        python_callable=seed_cdc_watermark,
    )

    load_fact_orders >> seed_watermark
