from enum import Enum

import typer
from pyspark.sql import SparkSession

from aviationstack._etl import AviationStackETL
from shared.base_etl import BaseETL
from shared.config import AviationStackParams

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
    import json

    etl_class = ETL_ENUM_TO_CLASS_DICT[etl]

    # Parse JSON string into AviationStackParams
    config_dict = json.loads(vars_)
    config = AviationStackParams(**config_dict)

    spark: SparkSession = SparkSession.builder.appName(etl.value).getOrCreate()

    etl_class(spark, config).run()


if __name__ == "__main__":
    app()
