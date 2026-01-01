# Base Image
FROM apache/airflow:2.10.3-python3.10

USER root

# ---- System deps ----
# Java Spark + toolchain/libpq (pg_config, etc.)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  openjdk-17-jre-headless \
  gcc \
  python3-dev \
  libpq-dev \
  curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# install uv
COPY --from=ghcr.io/astral-sh/uv:0.5.6 /uv /usr/local/bin/uv

# needed for pip/uv installs afterwards
USER airflow
WORKDIR /opt/airflow

# Create virtualenv and install dependencies
# We use the virtualenv because it's managed by uv and consistent
ENV PATH="/opt/airflow/.venv/bin:${PATH}"

# use cache for performance
COPY --chown=airflow:0 pyproject.toml uv.lock ./

RUN uv sync --frozen --no-cache --no-dev

# Copy code
COPY --chown=airflow:0 include /opt/airflow/include
COPY --chown=airflow:0 spark_jobs /opt/airflow/spark_jobs
COPY --chown=airflow:0 dags /opt/airflow/dags
COPY --chown=airflow:0 dbt_transform /opt/airflow/dbt_transform
