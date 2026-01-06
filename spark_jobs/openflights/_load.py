import os
from typing import Dict
from pyspark.sql import DataFrame

from spark_jobs.shared.config import OpenFlightsParams


def load_files(dfs: Dict[str, DataFrame], config: OpenFlightsParams) -> None:
    """Load the transformed DataFrames to the bronze directory."""
    for name, df in dfs.items():
        if df:
            output_path = os.path.join(config.bronze_dir, name)
            df.write.mode("overwrite").parquet(output_path)
