import logging
import json
from enum import Enum
import typer
from pyspark.sql import SparkSession
from spark_jobs.aviationstack._etl import AviationStackETL
from spark_jobs.shared.base_etl import BaseETL
from spark_jobs.shared.config import AviationStackParams

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class ETL(str, Enum):
    """Enum representing the various ETL processes available for execution."""

    AVIATIONSTACK = "aviationstack"


ETL_ENUM_TO_CLASS_DICT: dict[ETL, type[BaseETL]] = {
    ETL.AVIATIONSTACK: AviationStackETL,
    # Add more ETLs here: ETL.OPENSKY
}

app = typer.Typer()


@app.command()
def main(etl: ETL, vars_: str) -> None:
    """Run an ETL."""

    etl_class = ETL_ENUM_TO_CLASS_DICT[etl]

    # Parse JSON string into AviationStackParams
    config_dict = json.loads(vars_)
    config = AviationStackParams(**config_dict)

    spark: SparkSession = SparkSession.builder.appName(etl.value).getOrCreate()

    # Ensure the SparkContext is not stopped
    if spark.sparkContext._jsc is None or spark.sparkContext._jsc.version() is None:
        logger.warning("SparkContext was stopped. Recreating...")
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
        spark = SparkSession.builder.appName(etl.value).getOrCreate()

    logger.info(f"Starting ETL: {etl.value}")
    try:
        etl_class(spark, config).run()
        logger.info(f"ETL {etl.value} completed successfully.")
    except Exception as e:
        logger.error(f"ETL {etl.value} failed with error: {e}")
        raise
    finally:
        logger.info("Stopping Spark session...")
        spark.stop()


if __name__ == "__main__":
    app()
