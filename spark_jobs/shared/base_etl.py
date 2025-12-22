from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pyspark.sql import SparkSession

from .config import AviationStackParams


ExtractedDataFrames = TypeVar("ExtractedDataFrames")
TransformedDataFrames = TypeVar("TransformedDataFrames")


class BaseETL(
    ABC,
    Generic[
        ExtractedDataFrames,
        TransformedDataFrames,
    ],
):
    """Base class for implementing an ETL process."""

    def __init__(self, spark: SparkSession, config: AviationStackParams) -> None:
        self._spark = spark
        self._config = config

    def run(self) -> None:
        """Run all the steps of the ETL."""
        extracted_dfs = self._extract()
        transformed_dfs = self._transform(extracted_dfs)
        self._load(transformed_dfs)

    @abstractmethod
    def _extract(self, config: AviationStackParams) -> ExtractedDataFrames:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""

    @abstractmethod
    def _transform(self, extracted_dfs: ExtractedDataFrames) -> TransformedDataFrames:
        """Transform the extracted DataFrames and return a NamedTuple of DataFrame(s)."""

    @abstractmethod
    def _load(self, transformed_dfs: TransformedDataFrames, config: AviationStackParams) -> None:
        """Load the transformed DataFrames to the sink(s)."""
