from pyspark.sql import DataFrame
import logging

from spark_jobs.shared.config import AviationStackParams

logger = logging.getLogger(__name__)


def load_flights(df: DataFrame, config: AviationStackParams) -> None:
    # Load to Bronze Parquet (Lake)
    target_path = config.bronze_dir.rstrip("/")
    df.write.mode("overwrite").parquet(target_path)
    logger.info(f"Loaded flights into Parquet: {target_path}")
