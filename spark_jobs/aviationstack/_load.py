import logging
import time
from pyspark.sql import DataFrame

from spark_jobs.shared.base_etl import LoadError
from spark_jobs.shared.config import AviationStackParams

logger = logging.getLogger(__name__)

# Default configuration for Parquet output
DEFAULT_COMPRESSION = "snappy"
DEFAULT_MAX_OUTPUT_FILES = 4


def load_flights(
    df: DataFrame,
    config: AviationStackParams,
    compression: str = DEFAULT_COMPRESSION,
    max_output_files: int = DEFAULT_MAX_OUTPUT_FILES,
) -> None:
    """Load transformed flights DataFrame to bronze Parquet.
    
    Args:
        df: Transformed flights DataFrame
        config: AviationStack configuration parameters
        compression: Parquet compression codec (snappy, gzip, zstd, none)
        max_output_files: Maximum number of output files (uses coalesce)
        
    Raises:
        LoadError: If writing to Parquet fails
    """
    target_path = config.bronze_dir.rstrip("/")
    
    if df.rdd.isEmpty():
        logger.warning(f"DataFrame is empty, skipping write to {target_path}")
        return

    try:
        start_time = time.time()
        row_count = df.count()
        
        # Get current partition count and optimize
        current_partitions = df.rdd.getNumPartitions()
        target_partitions = min(current_partitions, max_output_files)
        
        if target_partitions < current_partitions:
            logger.info(
                f"Coalescing from {current_partitions} to {target_partitions} partitions"
            )
            df = df.coalesce(target_partitions)
        
        # Write with compression
        (
            df.write
            .mode("overwrite")
            .option("compression", compression)
            .parquet(target_path)
        )
        
        elapsed = time.time() - start_time
        logger.info(
            f"Loaded {row_count} flights to Parquet: {target_path} "
            f"(compression={compression}, partitions={target_partitions}, "
            f"elapsed={elapsed:.2f}s)"
        )
    except Exception as e:
        raise LoadError(f"Failed to write flights to {target_path}: {e}") from e
