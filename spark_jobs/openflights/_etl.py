from pyspark.sql import DataFrame
from typing import NamedTuple, Dict

from spark_jobs.openflights._extract import extract_files
from spark_jobs.openflights._transform import transform_files
from spark_jobs.openflights._load import load_files
from spark_jobs.shared.base_etl import BaseETL
from spark_jobs.shared.config import OpenFlightsParams


class OpenFlightsDataframes(NamedTuple):
    """NamedTuple to hold the extracted DataFrames."""

    dfs: Dict[str, DataFrame]


class OpenFlightsETL(
    BaseETL[OpenFlightsDataframes, Dict[str, DataFrame], OpenFlightsParams]
):
    """ETL class for openflights."""

    def _extract(self) -> OpenFlightsDataframes:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""

        dfs = extract_files(self._spark, self._config.raw_dir)

        return OpenFlightsDataframes(dfs=dfs)

    def _transform(self, extracted_dfs: OpenFlightsDataframes) -> Dict[str, DataFrame]:
        """Transform the extracted DataFrames and return a dictionary of DataFrames."""

        return transform_files(extracted_dfs.dfs)

    def _load(self, transformed_dfs: Dict[str, DataFrame]) -> None:
        """Load the transformed DataFrames to the sink."""

        return load_files(transformed_dfs, self._config)
