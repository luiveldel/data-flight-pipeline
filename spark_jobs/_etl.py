from spark_jobs._transform import transform_flights
from spark_jobs._extract import extract_flights
from typing import NamedTuple

from pyspark.sql import DataFrame
from .shared.base_etl import BaseETL


class JsonToTabularDataframes(NamedTuple):
    """NamedTuple to hold the extracted DataFrames."""

    aviationstack_json_df: DataFrame

class AviationStackETL(BaseETL[JsonToTabularDataframes, DataFrame]):
    """ETL class for aviationstack."""

    def _extract(self) -> JsonToTabularDataframes:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""

        json_paths = extract_flights(
            raw_dir=self._config.aviationstack.raw_dir,
            max_pages=self._config.aviationstack.max_pages,
        )

        aviationstack_json_df = self._spark.read.json(json_paths)

        return JsonToTabularDataframes(aviationstack_json_df)

    def _transform(self, extracted_dfs: JsonToTabularDataframes) -> DataFrame:
        """Transform the extracted DataFrames and return a DataFrame."""

        return transform_flights(extracted_dfs.aviationstack_json_df)

    def _load(self, transformed_df: DataFrame) -> None:
        """Load the transformed DataFrame to the sink."""

        transformed_df.write.mode("overwrite").parquet(self._config.aviationstack.bronze_dir)
