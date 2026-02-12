import logging
from typing import NamedTuple
from pyspark.sql import DataFrame

from spark_jobs.aviationstack._load import load_flights
from spark_jobs.aviationstack._transform import transform_flights
from spark_jobs.shared.base_etl import BaseETL, ExtractError, TransformError
from spark_jobs.shared.config import AviationStackParams

logger = logging.getLogger(__name__)


class AviationStackDataframes(NamedTuple):
    """NamedTuple to hold the extracted DataFrames."""

    aviationstack_json_df: DataFrame


class AviationStackETL(
    BaseETL[AviationStackDataframes, DataFrame, AviationStackParams]
):
    """ETL class for aviationstack."""

    def _extract(self) -> AviationStackDataframes:
        """Extract the data from the source(s) and return a NamedTuple of DataFrame(s)."""
        raw_dir = self._config.raw_dir
        logger.info(f"Reading JSON from: {raw_dir}")

        try:
            aviationstack_json_df = self._spark.read.json(raw_dir)
        except Exception as e:
            raise ExtractError(f"Failed to read JSON from {raw_dir}: {e}") from e

        # Validate DataFrame is not empty
        if aviationstack_json_df.rdd.isEmpty():
            raise ExtractError(f"No data found in {raw_dir}")

        # Validate expected 'data' column exists
        if "data" not in aviationstack_json_df.columns:
            raise ExtractError(
                f"JSON does not contain expected 'data' field. "
                f"Found columns: {aviationstack_json_df.columns}"
            )

        logger.info(f"Extracted {aviationstack_json_df.count()} records from {raw_dir}")
        return AviationStackDataframes(aviationstack_json_df)

    def _transform(self, extracted_dfs: AviationStackDataframes) -> DataFrame:
        """Transform the extracted DataFrames and return a DataFrame."""
        try:
            transformed_df = transform_flights(extracted_dfs.aviationstack_json_df)
        except Exception as e:
            raise TransformError(f"Failed to transform flights data: {e}") from e

        row_count = transformed_df.count()
        if row_count == 0:
            logger.warning("Transformation resulted in 0 rows")
        else:
            logger.info(f"Transformed {row_count} flight records")

        return transformed_df

    def _load(self, transformed_df: DataFrame) -> None:
        """Load the transformed DataFrame to the sink."""
        load_flights(transformed_df, self._config)
