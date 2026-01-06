from datetime import timedelta, datetime
import json
import os

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

from utils import upload_to_s3, format_dbt_vars

# -----------------------------------------------------------------------------
# - VARS
# -----------------------------------------------------------------------------
DAG_NAME = "flights_data_pipeline"
OWNER = "luisandresvelazquez.d@gmail.com"
EXECUTION_DATE = "{{ ds }}"

# -----------------------------------------------------------------------------
# - S3 (MINIO)
# -----------------------------------------------------------------------------
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")

S3_BUCKET = "flights-data-lake"
LOCAL_RAW_PATH = "/opt/airflow/data/raw"
LOCAL_BRONZE_PATH = "/opt/airflow/data/bronze"
MAX_PAGES = 1

# -----------------------------------------------------------------------------
# - ETL
# -----------------------------------------------------------------------------
aviationstack_vars = json.dumps(
    {
        "raw_dir": f"s3a://{S3_BUCKET}/raw/insert_date={EXECUTION_DATE}",
        "bronze_dir": f"s3a://{S3_BUCKET}/bronze/insert_date={EXECUTION_DATE}",
        "max_pages": MAX_PAGES,
    }
)

openflights_vars = json.dumps(
    {
        "raw_dir": f"s3a://{S3_BUCKET}/raw/openflights",
        "bronze_dir": f"s3a://{S3_BUCKET}/bronze/openflights",
    }
)

# -----------------------------------------------------------------------------
# - Spark
# -----------------------------------------------------------------------------
SPARK_MASTER_URL = os.environ.get("SPARK_MASTER_URL")

# -----------------------------------------------------------------------------
# - DAG
# -----------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": OWNER,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    DAG_NAME,
    start_date=datetime(2025, 12, 21),
    catchup=False,
    schedule_interval="0 0 * * *",
    default_args=DEFAULT_ARGS,
    tags=["spark", "minio", "aviationstack", "etl"],
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_bucket = S3CreateBucketOperator(
        task_id="create_minio_bucket",
        bucket_name=S3_BUCKET,
        aws_conn_id="aws_default",
    )

    # TODO: refactor this task, it's not airflow-y, it's just a bash command
    extract_api = BashOperator(
        task_id="extract_api_to_json",
        bash_command=f"python3 -m include flights \
            {LOCAL_RAW_PATH} \
            --max-pages {MAX_PAGES} \
            --execution-date {EXECUTION_DATE}",
    )

    extract_openflights = BashOperator(
        task_id="extract_openflights_to_csv",
        bash_command=f"python3 -m include openflights \
            {LOCAL_RAW_PATH}/openflights/",
    )

    upload_raw_to_minio = upload_to_s3(
        task_id="upload_raw_to_minio",
        bucket=S3_BUCKET,
        local_path=LOCAL_RAW_PATH,
        remote_prefix="raw",
    )

    def run_etl(
        *,
        etl_name: str,
        _vars: str | None = None,
    ) -> BashOperator:
        if _vars is None:
            _vars = "{}"

        return BashOperator(
            task_id=f"etl_{etl_name}",
            bash_command=f"""
                /opt/airflow/.venv/bin/spark-submit \
                --master {SPARK_MASTER_URL} \
                --total-executor-cores 1 \
                --executor-memory 512M \
                --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \
                --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                --jars /opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar \
                --name {etl_name}-etl \
                /opt/airflow/spark_jobs/main_cli.py {etl_name} '{_vars}'
            """,
        )

    etl_openflights = run_etl(etl_name="openflights", _vars=openflights_vars)
    etl_aviationstack = run_etl(etl_name="aviationstack", _vars=aviationstack_vars)

    dbt_vars = format_dbt_vars(
        {
            "execution_date": f"{EXECUTION_DATE}",
            "s3_bronze_path": f"s3://{S3_BUCKET}/bronze/insert_date=",
        }
    )

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=f"cd /opt/airflow/dbt_transform && \
            dbt run --vars {dbt_vars}",
    )

    (
        start
        >> create_bucket
        >> [extract_api, extract_openflights]
        >> upload_raw_to_minio
        >> [etl_aviationstack, etl_openflights]
        >> dbt_transform
        >> end
    )


dag_()
