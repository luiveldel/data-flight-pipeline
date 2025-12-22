from datetime import timedelta, datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from utils.extract_api import extract_flights

DAG_NAME = "flights_extraction"

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    DAG_NAME,
    start_date=datetime(2025, 12, 21),
    catchup=False,
    description="AviationStack → PySpark → dbt ETL Pipeline",
    schedule_interval="@daily",
    default_args=DEFAULT_ARGS,
    tags=["spark", "aviationstack", "etl", "portfolio"],
)
def dag_() -> None:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_api = PythonOperator(
        task_id="extract_api_to_json",
        python_callable=extract_flights,
        op_kwargs={
            "raw_dir": "/data/raw",
            "max_pages": 2,
        },
    )

    # spark_etl = SparkSubmitOperator(
    #     task_id="etl_aviationstack",
    #     application="spark_jobs/aviationstack/main_etl.py",
    #     conf={
    #         "spark.master": "spark://spark-master:7077",
    #         "spark.executor.memory": "2g",
    #         "spark.executor.cores": "2",
    #         "spark.driver.memory": "2g",
    #         "spark.sql.adaptive.enabled": "true",
    #     },
    # )

    start >> extract_api >> end


dag_()
