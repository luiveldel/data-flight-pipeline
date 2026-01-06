import logging
import json
from enum import Enum
import typer
from pyspark.sql import SparkSession
from contextlib import contextmanager

from spark_jobs.aviationstack._etl import AviationStackETL
from spark_jobs.openflights._etl import OpenFlightsETL
from spark_jobs.shared.base_etl import BaseETL
from spark_jobs.shared.config import AviationStackParams, OpenFlightsParams


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

@contextmanager
def spark_session(app_name: str) -> SparkSession:
    """Reuse active or create a new SparkSession."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    try:
        yield spark
    finally:
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()

class ETL(str, Enum):
    """Enum representing the various ETL processes available for execution."""

    AVIATIONSTACK = "aviationstack"
    OPENFLIGHTS = "openflights"


ETL_ENUM_TO_CLASS_DICT: dict[ETL, type[BaseETL]] = {
    ETL.AVIATIONSTACK: AviationStackETL,
    ETL.OPENFLIGHTS: OpenFlightsETL,
}

ETL_ENUM_TO_PARAMS_DICT: dict[ETL, type] = {
    ETL.AVIATIONSTACK: AviationStackParams,
    ETL.OPENFLIGHTS: OpenFlightsParams,
}

app = typer.Typer()


@app.command()
def main(etl: ETL, vars_: str) -> None:
    """Run an ETL."""

    etl_class = ETL_ENUM_TO_CLASS_DICT[etl]
    params_class = ETL_ENUM_TO_PARAMS_DICT[etl]

    config_dict = json.loads(vars_)
    config = params_class(**config_dict)

    logger.info(f"Starting ETL: {etl.value}")
    try:
        with spark_session(etl.value) as spark:
            etl_class(spark, config).run()
        logger.info(f"ETL {etl.value} completed successfully.")
    except Exception:
        logger.exception(f"ETL {etl.value} failed")
        raise


if __name__ == "__main__":
    app()
