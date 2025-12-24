from datetime import timedelta, datetime
import json
import os

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

from utils.upload_to_s3 import upload_to_s3

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
LOCAL_RAW_PATH = "/opt/data/raw"
LOCAL_BRONZE_PATH = "/opt/data/bronze"
MAX_PAGES = 1

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
        bash_command=f"python3 -m include.api \
            {LOCAL_RAW_PATH} \
            {MAX_PAGES} \
            {EXECUTION_DATE}",
        env={
            **os.environ.copy(),
            "PYTHONPATH": "/opt/airflow",
        },
    )

    upload_raw_to_minio = upload_to_s3(
        task_id="upload_raw_to_minio",
        bucket=S3_BUCKET,
        local_path=LOCAL_RAW_PATH,
        remote_prefix=f"raw/{EXECUTION_DATE}",
    )

    spark_vars = json.dumps(
        {
            "raw_dir": f"s3a://{S3_BUCKET}/raw/{EXECUTION_DATE}",
            "bronze_dir": f"s3a://{S3_BUCKET}/bronze/{EXECUTION_DATE}",
            "max_pages": MAX_PAGES,
        }
    )

    spark_etl = BashOperator(
        task_id="etl_aviationstack",
        bash_command=f"""
            /home/airflow/.local/bin/spark-submit \
                --master {SPARK_MASTER_URL} \
                --conf spark.hadoop.fs.s3a.endpoint={MINIO_ENDPOINT} \
                --conf spark.hadoop.fs.s3a.access.key={MINIO_ACCESS_KEY} \
                --conf spark.hadoop.fs.s3a.secret.key={MINIO_SECRET_KEY} \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                --conf spark.jars.packages=org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                --name arrow-spark \
                /spark_jobs/main_cli.py aviationstack '{spark_vars}'
        """,
        env={
            "PYTHONPATH": "/opt/airflow/dags:/opt/airflow/include:/spark_jobs",
            "PYSPARK_PYTHON": "/home/airflow/.local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/home/airflow/.local/bin/python3",
        },
    )

    (
        start
        >> create_bucket
        >> extract_api
        >> upload_raw_to_minio
        >> spark_etl
        >> end
    )


dag_()
