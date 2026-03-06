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

RUN uv pip install \
    "apache-airflow==3.0.0" \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-redis" \
    "mlflow" \
    "psycopg2-binary" \
    "pyspark" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.0/constraints-3.12.txt"

RUN uv pip install "uvicorn[standard]>=0.37.0"

RUN uv pip install "feast[redis]"