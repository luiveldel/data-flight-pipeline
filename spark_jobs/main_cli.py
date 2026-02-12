import logging
import json
import sys
from enum import Enum
from typing import Generator
import typer
from pyspark.sql import SparkSession
from pydantic import ValidationError
from contextlib import contextmanager

from spark_jobs.aviationstack._etl import AviationStackETL
from spark_jobs.openflights._etl import OpenFlightsETL
from spark_jobs.shared.base_etl import BaseETL, ETLError, ExtractError, TransformError, LoadError
from spark_jobs.shared.config import AviationStackParams, OpenFlightsParams


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


@contextmanager
def spark_session(app_name: str, stop_on_exit: bool = True) -> Generator[SparkSession, None, None]:
    """Create or reuse a SparkSession.
    
    Args:
        app_name: Name for the Spark application
        stop_on_exit: Whether to stop the session on exit (default: True)
        
    Yields:
        SparkSession instance
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info(f"SparkSession created/reused for app: {app_name}")

    try:
        yield spark
    finally:
        if stop_on_exit:
            spark.stop()
            logger.info(f"SparkSession stopped for app: {app_name}")


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
    """Run an ETL process.
    
    Args:
        etl: The ETL type to run (aviationstack or openflights)
        vars_: JSON string with configuration parameters
    """
    etl_class = ETL_ENUM_TO_CLASS_DICT[etl]
    params_class = ETL_ENUM_TO_PARAMS_DICT[etl]

    # Parse and validate configuration
    try:
        config_dict = json.loads(vars_)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON configuration: {e}")
        raise typer.Exit(code=1)

    try:
        config = params_class(**config_dict)
    except ValidationError as e:
        logger.error(f"Invalid configuration parameters: {e}")
        raise typer.Exit(code=1)

    logger.info(f"Starting ETL: {etl.value}")
    logger.info(f"Configuration: {config.model_dump()}")

    try:
        with spark_session(etl.value) as spark:
            etl_class(spark, config).run()
        logger.info(f"ETL {etl.value} completed successfully.")

    except ExtractError as e:
        logger.error(f"ETL {etl.value} failed during extraction: {e}")
        sys.exit(2)

    except TransformError as e:
        logger.error(f"ETL {etl.value} failed during transformation: {e}")
        sys.exit(3)

    except LoadError as e:
        logger.error(f"ETL {etl.value} failed during load: {e}")
        sys.exit(4)

    except ETLError as e:
        logger.error(f"ETL {etl.value} failed: {e}")
        sys.exit(5)

    except Exception as e:
        logger.exception(f"ETL {etl.value} failed with unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    app()
