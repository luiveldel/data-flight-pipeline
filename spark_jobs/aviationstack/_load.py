from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


def load_flights(df: DataFrame, bronze_dir: str) -> None:
    # Use string manipulation instead of os.path/os.makedirs for S3 compatibility
    target_path = f"{bronze_dir.rstrip('/')}/flights"

    (df.write.mode("append").parquet(target_path))

    logger.info(f"Loaded flights into {target_path}")
