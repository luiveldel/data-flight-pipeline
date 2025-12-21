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

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.5.6 /uv /usr/local/bin/uv

# Install dependencies using uv (export to requirements.txt for pip compatibility)
# Install as root to system paths, overriding PIP_USER=true
COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache -r pyproject.toml && \
    rm -rf ~/.cache/pip /tmp/*

COPY spark_jobs ./spark_jobs

USER airflow