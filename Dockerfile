# Base Image
FROM apache/airflow:2.10.3-python3.10

USER root

# Install OpenJDK-17
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER root
RUN mkdir -p /opt/data/raw && chown -R airflow:0 /opt/data

USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.10.txt" \
  pydantic==2.9.2 pyspark==3.5.3 requests==2.32.3 \
  apache-airflow-providers-apache-spark==4.11.2 apache-airflow-providers-amazon typer

COPY include /opt/airflow/include
COPY spark_jobs ./spark_jobs

USER airflow