import os
from typing import Dict
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.openflights.dictionary import OPENFLIGHTS_FILES


def read_csv_no_header(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read.option("header", "false")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .option("encoding", "UTF-8")
        .csv(path)
    )


def extract_files(spark: SparkSession, raw_dir: str) -> Dict[str, DataFrame]:
    dfs: Dict[str, DataFrame] = {}

    for name, filename in OPENFLIGHTS_FILES.items():
        path = os.path.join(raw_dir, filename)
        dfs[name] = read_csv_no_header(spark, path)

    return dfs
