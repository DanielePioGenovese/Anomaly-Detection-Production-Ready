FROM apache/airflow:2.10.2-python3.12

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --upgrade uv

RUN uv pip install --system \
    "apache-airflow==2.10.2" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-redis" \
    "feast[redis]" \
    "mlflow" \
    "psycopg2-binary" \
    "pyspark" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"