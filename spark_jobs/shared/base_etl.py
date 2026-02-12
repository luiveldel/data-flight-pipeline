import logging
import time
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pyspark.sql import SparkSession


Config = TypeVar("Config")
ExtractedDataFrames = TypeVar("ExtractedDataFrames")
TransformedDataFrames = TypeVar("TransformedDataFrames")

logger = logging.getLogger(__name__)


class ETLError(Exception):
    """Base exception for ETL errors."""
    pass


class ExtractError(ETLError):
    """Exception raised during extraction phase."""
    pass


class TransformError(ETLError):
    """Exception raised during transformation phase."""
    pass


class LoadError(ETLError):
    """Exception raised during load phase."""
    pass


class BaseETL(
    ABC,
    Generic[
        ExtractedDataFrames,
        TransformedDataFrames,
        Config,
    ],
):
    """Base class for implementing an ETL process."""

    def __init__(self, spark: SparkSession, config: Config) -> None:
        self._spark = spark
        self._config = config

    def run(self) -> None:
        """Run all the steps of the ETL with logging and error handling."""
        total_start = time.time()
        etl_name = self.__class__.__name__

        try:
            # Extract phase
            logger.info(f"[{etl_name}] Starting extraction...")
            extract_start = time.time()
            extracted_dfs = self._extract()
            extract_elapsed = time.time() - extract_start
            logger.info(f"[{etl_name}] Extraction completed in {extract_elapsed:.2f}s")

            # Transform phase
            logger.info(f"[{etl_name}] Starting transformation...")
            transform_start = time.time()
            transformed_dfs = self._transform(extracted_dfs)
            transform_elapsed = time.time() - transform_start
            logger.info(f"[{etl_name}] Transformation completed in {transform_elapsed:.2f}s")

            # Load phase
            logger.info(f"[{etl_name}] Starting load...")
            load_start = time.time()
            self._load(transformed_dfs)
            load_elapsed = time.time() - load_start
            logger.info(f"[{etl_name}] Load completed in {load_elapsed:.2f}s")

            total_elapsed = time.time() - total_start
            logger.info(f"[{etl_name}] ETL completed successfully in {total_elapsed:.2f}s")

        except ExtractError:
            raise
        except TransformError:
            raise
        except LoadError:
            raise
        except Exception as e:
            total_elapsed = time.time() - total_start
            logger.error(f"[{etl_name}] ETL failed after {total_elapsed:.2f}s: {e}")
            raise ETLError(f"ETL {etl_name} failed: {e}") from e

    @abstractmethod
    def _extract(self) -> ExtractedDataFrames:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""

    @abstractmethod
    def _transform(self, extracted_dfs: ExtractedDataFrames) -> TransformedDataFrames:
        """Transform the extracted DataFrames and return a NamedTuple of DataFrame(s)."""

    @abstractmethod
    def _load(self, transformed_dfs: TransformedDataFrames) -> None:
        """Load the transformed DataFrames to the sink(s)."""
