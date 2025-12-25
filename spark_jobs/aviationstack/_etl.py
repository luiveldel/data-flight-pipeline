from spark_jobs.aviationstack._load import load_flights
from spark_jobs.aviationstack._transform import transform_flights
from typing import NamedTuple

from pyspark.sql import DataFrame
from spark_jobs.shared.base_etl import BaseETL


class AviationStackDataframes(NamedTuple):
    """NamedTuple to hold the extracted DataFrames."""

    aviationstack_json_df: DataFrame


class AviationStackETL(BaseETL[AviationStackDataframes, DataFrame]):
    """ETL class for aviationstack."""

    def _extract(self) -> AviationStackDataframes:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""

        aviationstack_json_df = self._spark.read.json(self._config.raw_dir)

        return AviationStackDataframes(aviationstack_json_df)

    def _transform(self, extracted_dfs: AviationStackDataframes) -> DataFrame:
        """Transform the extracted DataFrames and return a DataFrame."""

        return transform_flights(extracted_dfs.aviationstack_json_df)

    def _load(self, transformed_df: DataFrame) -> None:
        """Load the transformed DataFrame to the sink."""

        return load_flights(transformed_df, self._config)
