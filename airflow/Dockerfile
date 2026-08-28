FROM apache/airflow:2.8.1-python3.10

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt" \
    apache-airflow-providers-microsoft-mssql \
    pymssql \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    pandas \
    sqlalchemy \
    clickhouse-connect
