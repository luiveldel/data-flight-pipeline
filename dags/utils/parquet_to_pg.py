import pandas as pd
import psycopg2
import io
import os
import glob
import logging

logger = logging.getLogger(__name__)


def load_parquet_dir_to_postgres(parquet_dir: str, table_name: str, conn_params: dict):
    """
    Reads all parquet files in a directory and uses COPY to ingest into Postgres.
    """

    parquet_files = glob.glob(os.path.join(parquet_dir, "*.parquet"))
    if not parquet_files:
        logger.warning(f"No parquet files found in {parquet_dir}")
        return

    logger.info(f"Found {len(parquet_files)} parquet files. Loading...")

    df = pd.concat([pd.read_parquet(f) for f in parquet_files])

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep="|")
    buffer.seek(0)
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cursor:
            logger.info(f"Truncating table {table_name}...")
            cursor.execute(f"TRUNCATE TABLE {table_name};")

            logger.info(f"Executing COPY to {table_name}...")
            cursor.copy_from(buffer, table_name, sep="|", null="")
            conn.commit()

    logger.info(f"Successfully ingested {len(df)} rows into {table_name}")
