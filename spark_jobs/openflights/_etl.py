import logging
from pyspark.sql import DataFrame
from typing import NamedTuple, Dict

from spark_jobs.openflights._extract import extract_files
from spark_jobs.openflights._transform import transform_files
from spark_jobs.openflights._load import load_files
from spark_jobs.shared.base_etl import BaseETL, TransformError
from spark_jobs.shared.config import OpenFlightsParams

logger = logging.getLogger(__name__)


class OpenFlightsDataframes(NamedTuple):
    """NamedTuple to hold the extracted DataFrames."""

    dfs: Dict[str, DataFrame]


class OpenFlightsETL(
    BaseETL[OpenFlightsDataframes, Dict[str, DataFrame], OpenFlightsParams]
):
    """ETL class for openflights."""

    def _extract(self) -> OpenFlightsDataframes:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""
        logger.info(f"Extracting OpenFlights data from: {self._config.raw_dir}")
        dfs = extract_files(self._spark, self._config.raw_dir)
        logger.info(f"Extracted {len(dfs)} datasets from OpenFlights")
        return OpenFlightsDataframes(dfs=dfs)

    def _transform(self, extracted_dfs: OpenFlightsDataframes) -> Dict[str, DataFrame]:
        """Transform the extracted DataFrames and return a dictionary of DataFrames."""
        logger.info(f"Transforming {len(extracted_dfs.dfs)} OpenFlights datasets")
        
        try:
            transformed_dfs = transform_files(extracted_dfs.dfs)
        except Exception as e:
            raise TransformError(f"Failed to transform OpenFlights data: {e}") from e

        for name, df in transformed_dfs.items():
            if not df.rdd.isEmpty():
                logger.info(f"Transformed {name}: {df.count()} rows")
            else:
                logger.warning(f"Transformed {name}: empty DataFrame")

        return transformed_dfs

    def _load(self, transformed_dfs: Dict[str, DataFrame]) -> None:
        """Load the transformed DataFrames to the sink."""
        logger.info(f"Loading {len(transformed_dfs)} datasets to: {self._config.bronze_dir}")
        load_files(transformed_dfs, self._config)
