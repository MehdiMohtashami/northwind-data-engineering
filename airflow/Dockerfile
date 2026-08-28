FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt" \
    apache-airflow-providers-microsoft-mssql \
    pymssql \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    pandas \
    sqlalchemy \
    clickhouse-connect \
    apache-airflow-providers-apache-spark

# pyspark pinned unconstrained to match the standalone cluster's exact patch
# version (bitnamilegacy/spark:3.5 resolves to 3.5.6) — the Airflow 2.8.1
# constraints file hard-pins pyspark==3.5.0, which is a different patch and
# fails with Spark's Task serialVersionUID mismatch against a 3.5.6 cluster.
RUN pip install --no-cache-dir pyspark==3.5.6
