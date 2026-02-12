import logging
import os
import time
from typing import Dict
from pyspark.sql import DataFrame

from spark_jobs.shared.base_etl import LoadError
from spark_jobs.shared.config import OpenFlightsParams

logger = logging.getLogger(__name__)

# Default configuration for Parquet output
DEFAULT_COMPRESSION = "snappy"
DEFAULT_MAX_OUTPUT_FILES = 2  # OpenFlights files are small


def load_files(
    dfs: Dict[str, DataFrame],
    config: OpenFlightsParams,
    compression: str = DEFAULT_COMPRESSION,
    max_output_files: int = DEFAULT_MAX_OUTPUT_FILES,
) -> None:
    """Load the transformed DataFrames to the bronze directory.
    
    Args:
        dfs: Dictionary mapping names to DataFrames
        config: OpenFlights configuration parameters
        compression: Parquet compression codec (snappy, gzip, zstd, none)
        max_output_files: Maximum number of output files per dataset
        
    Raises:
        LoadError: If writing any file fails
    """
    bronze_dir = config.bronze_dir
    loaded_count = 0
    skipped_count = 0
    total_rows = 0
    start_time = time.time()

    for name, df in dfs.items():
        if df is None:
            logger.warning(f"Skipping {name}: DataFrame is None")
            skipped_count += 1
            continue

        if df.rdd.isEmpty():
            logger.warning(f"Skipping {name}: DataFrame is empty")
            skipped_count += 1
            continue

        output_path = os.path.join(bronze_dir, f"{name}.parquet")
        try:
            row_count = df.count()
            total_rows += row_count
            
            # Coalesce to reduce small files
            current_partitions = df.rdd.getNumPartitions()
            target_partitions = min(current_partitions, max_output_files)
            
            if target_partitions < current_partitions:
                df = df.coalesce(target_partitions)
            
            # Write with compression
            (
                df.write
                .mode("overwrite")
                .option("compression", compression)
                .parquet(output_path)
            )
            
            logger.info(
                f"Loaded {row_count} rows to {output_path} "
                f"(partitions={target_partitions})"
            )
            loaded_count += 1
        except Exception as e:
            raise LoadError(f"Failed to write {name} to {output_path}: {e}") from e

    elapsed = time.time() - start_time
    logger.info(
        f"Load completed: {loaded_count} files written, {skipped_count} skipped, "
        f"{total_rows} total rows, compression={compression}, elapsed={elapsed:.2f}s"
    )
