import logging
import os
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.openflights.dictionary import OPENFLIGHTS_FILES
from spark_jobs.shared.base_etl import ExtractError

logger = logging.getLogger(__name__)


def read_csv_no_header(spark: SparkSession, path: str) -> DataFrame:
    """Read a CSV file without header."""
    return (
        spark.read.option("header", "false")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .option("encoding", "UTF-8")
        .csv(path)
    )


def extract_files(spark: SparkSession, raw_dir: str) -> Dict[str, DataFrame]:
    """Extract OpenFlights CSV files from the raw directory.
    
    Args:
        spark: SparkSession instance
        raw_dir: Path to directory containing raw CSV files
        
    Returns:
        Dictionary mapping file names to DataFrames
        
    Raises:
        ExtractError: If raw directory doesn't exist or files are missing
    """
    # Validate raw directory exists (for local paths)
    if not raw_dir.startswith("s3") and not os.path.exists(raw_dir):
        raise ExtractError(f"Raw directory does not exist: {raw_dir}")

    dfs: Dict[str, DataFrame] = {}
    missing_files: list[str] = []
    empty_files: list[str] = []

    for name, filename in OPENFLIGHTS_FILES.items():
        path = os.path.join(raw_dir, filename)
        logger.info(f"Reading {name} from: {path}")

        try:
            df = read_csv_no_header(spark, path)
            
            # Check if DataFrame is empty
            if df.rdd.isEmpty():
                empty_files.append(name)
                logger.warning(f"File {name} ({filename}) is empty")
            else:
                row_count = df.count()
                logger.info(f"Extracted {row_count} rows from {name}")
            
            dfs[name] = df
        except Exception as e:
            missing_files.append(f"{name} ({filename}): {e}")
            logger.error(f"Failed to read {name} from {path}: {e}")

    if missing_files:
        raise ExtractError(
            f"Failed to extract files: {', '.join(missing_files)}"
        )

    if empty_files:
        logger.warning(f"Empty files detected: {', '.join(empty_files)}")

    logger.info(f"Successfully extracted {len(dfs)} OpenFlights files")
    return dfs
