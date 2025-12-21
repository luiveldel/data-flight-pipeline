import os
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def load_flights(df: DataFrame, bronze_dir: str) -> None:

    os.makedirs(bronze_dir, exist_ok=True)

    target_path = os.path.join(bronze_dir, "flights")

    (
        df.write
        .mode("append")
        .partitionBy("flight_date", "dep_iata")
        .parquet(target_path)
    )

    logger.info(f"Loaded flights into {target_path} (partitioned by flight_date, dep_iata)")
