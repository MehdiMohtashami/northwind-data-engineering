"""Incremental CDC micro-batch load of FactOrders from the Northwind OLTP source."""
from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

JARS = (
    "/opt/airflow/spark/jars/postgresql-42.7.3.jar,"
    "/opt/airflow/spark/jars/clickhouse-jdbc-0.6.5-shaded.jar,"
    "/opt/airflow/spark/jars/mssql-jdbc-12.10.1.jre11.jar"
)

pg_conn = BaseHook.get_connection("pg_staging")
ch_conn = BaseHook.get_connection("clickhouse_dw")
mssql_conn = BaseHook.get_connection("mssql_northwind")

PG_JDBC_URL = f"jdbc:postgresql://{pg_conn.host}:{pg_conn.port}/{pg_conn.schema}"
CH_JDBC_URL = f"jdbc:clickhouse://{ch_conn.host}:{ch_conn.port}/{ch_conn.schema}"
MSSQL_JDBC_URL = (
    f"jdbc:sqlserver://{mssql_conn.host}:{mssql_conn.port};"
    f"databaseName={mssql_conn.schema};encrypt=true;trustServerCertificate=true"
)

COMMON_ARGS = [
    "--pg-jdbc-url", PG_JDBC_URL,
    "--pg-user", pg_conn.login,
    "--pg-password", pg_conn.password,
    "--ch-jdbc-url", CH_JDBC_URL,
    "--ch-user", ch_conn.login,
    "--ch-password", ch_conn.password,
    "--mssql-jdbc-url", MSSQL_JDBC_URL,
    "--mssql-user", mssql_conn.login,
    "--mssql-password", mssql_conn.password,
]


def compute_lsn_window(**context):
    hook = MsSqlHook(mssql_conn_id="mssql_northwind")
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute(
        "SELECT last_lsn FROM ETL_Settings.dbo.cdc_state WHERE source_table = 'Orders'"
    )
    last_lsn = cur.fetchone()[0]

    if last_lsn is None:
        cur.execute("SELECT sys.fn_cdc_get_min_lsn('dbo_Orders')")
        from_lsn = cur.fetchone()[0]
    else:
        cur.execute("SELECT sys.fn_cdc_increment_lsn(%s)", (last_lsn,))
        from_lsn = cur.fetchone()[0]

    cur.execute("SELECT sys.fn_cdc_get_max_lsn()")
    to_lsn = cur.fetchone()[0]
    cur.close()
    conn.close()

    if from_lsn is None or to_lsn is None or from_lsn > to_lsn:
        raise AirflowSkipException("No new LSNs to process in this window.")

    context["ti"].xcom_push(key="from_lsn_hex", value=from_lsn.hex())
    context["ti"].xcom_push(key="to_lsn_hex", value=to_lsn.hex())
    print(f"LSN window: from={from_lsn.hex()} to={to_lsn.hex()}")


def advance_watermark(**context):
    to_lsn_hex = context["ti"].xcom_pull(task_ids="compute_lsn_window", key="to_lsn_hex")
    to_lsn = bytes.fromhex(to_lsn_hex)

    hook = MsSqlHook(mssql_conn_id="mssql_northwind")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE ETL_Settings.dbo.cdc_state "
        "SET last_lsn = %s, last_processed_at = SYSUTCDATETIME() "
        "WHERE source_table IN ('Orders', 'Order Details')",
        (to_lsn,),
    )
    conn.commit()
    print(f"CDC watermark advanced to LSN {to_lsn_hex} for Orders and Order Details")
    cur.close()
    conn.close()


with DAG(
    dag_id="fact_incremental_cdc",
    description="Incremental CDC micro-batch: Orders + Order Details changes into FactOrders.",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["phase3", "fact-cdc"],
) as dag:

    compute_window = PythonOperator(
        task_id="compute_lsn_window",
        python_callable=compute_lsn_window,
    )

    load_cdc = SparkSubmitOperator(
        task_id="load_fact_orders_cdc",
        application="/opt/airflow/spark/jobs/load_fact_orders_cdc.py",
        conn_id="spark_default",
        jars=JARS,
        application_args=COMMON_ARGS + [
            "--from-lsn-hex", "{{ ti.xcom_pull(task_ids='compute_lsn_window', key='from_lsn_hex') }}",
            "--to-lsn-hex", "{{ ti.xcom_pull(task_ids='compute_lsn_window', key='to_lsn_hex') }}",
        ],
        verbose=True,
    )

    advance = PythonOperator(
        task_id="advance_watermark",
        python_callable=advance_watermark,
    )

    compute_window >> load_cdc >> advance
