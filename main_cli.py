from enum import Enum

import typer
from pyspark.sql import SparkSession

from spark_jobs.aviationstack._etl import AviationStackETL
from spark_jobs.shared.base_etl import BaseETL

# ruff: noqa: T201, S311


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

    spark: SparkSession = SparkSession.builder.appName(etl.value).getOrCreate()

    etl_class(spark, vars_).run()


if __name__ == "__main__":
    app()
