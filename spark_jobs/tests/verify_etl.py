import sys
import os
import json
import shutil
from unittest.mock import patch

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from pyspark.sql import SparkSession
from spark_jobs.shared.config import Config, AviationStackParams
from spark_jobs.aviationstack._etl import AviationStackETL


def verify_etl():
    print("Setting up SparkSession...")
    spark = (
        SparkSession.builder.appName("VerifyAviationStackETL")
        .master("local[*]")
        .getOrCreate()
    )

    # Setup paths
    base_dir = os.path.dirname(__file__)
    raw_dir = os.path.join(base_dir, "data", "raw")
    bronze_dir = os.path.join(base_dir, "data", "bronze")

    # Cleanup previous runs
    print("Cleaning up previous run data...")
    if os.path.exists(raw_dir):
        shutil.rmtree(raw_dir)
    if os.path.exists(bronze_dir):
        shutil.rmtree(bronze_dir)

    # Create dummy data file
    dummy_data = {
        "pagination": {"limit": 100, "offset": 0, "count": 1, "total": 1},
        "data": [
            {
                "flight_date": "2023-10-27",
                "flight_status": "scheduled",
                "departure": {
                    "airport": "San Francisco International",
                    "timezone": "America/Los_Angeles",
                    "iata": "SFO",
                    "icao": "KSFO",
                    "terminal": "2",
                    "gate": "D11",
                    "delay": 13,
                    "scheduled": "2023-10-27T11:00:00+00:00",
                    "estimated": "2023-10-27T11:00:00+00:00",
                    "actual": "2023-10-27T11:13:00+00:00",
                    "estimated_runway": "2023-10-27T11:13:00+00:00",
                    "actual_runway": "2023-10-27T11:13:00+00:00",
                },
                "arrival": {
                    "airport": "Dallas Fort Worth International",
                    "timezone": "America/Chicago",
                    "iata": "DFW",
                    "icao": "KDFW",
                    "terminal": "A",
                    "gate": "A22",
                    "baggage": "A17",
                    "delay": 10,
                    "scheduled": "2023-10-27T16:00:00+00:00",
                    "estimated": "2023-10-27T16:00:00+00:00",
                    "actual": None,
                    "estimated_runway": None,
                    "actual_runway": None,
                },
                "airline": {"name": "American Airlines", "iata": "AA", "icao": "AAL"},
                "flight": {
                    "number": "100",
                    "iata": "AA100",
                    "icao": "AAL100",
                    "codeshared": None,
                },
                "aircraft": {
                    "registration": "N123AA",
                    "iata": "A321",
                    "icao": "A321",
                    "icao24": "A00001",
                },
                "live": {
                    "updated": "2023-10-27T12:00:00+00:00",
                    "latitude": 30.0,
                    "longitude": -90.0,
                    "altitude": 30000.0,
                    "direction": 90.0,
                    "speed_horizontal": 500.0,
                    "speed_vertical": 0.0,
                    "is_ground": False,
                },
            }
        ],
    }

    # Config
    config = Config(
        aviationstack=AviationStackParams(
            raw_dir=raw_dir,
            bronze_dir=bronze_dir,
            max_pages=1,  # This might not be in the Config definition I saw earlier, checking Config again...
        )
    )

    # Correction: The Config definition in shared/config.py was:
    # class AviationStackParams(BaseModel):
    #     raw_dir: str
    #     bronze_dir: str
    #
    # Wait, _etl.py uses: self._config.aviationstack.max_pages
    # So max_pages MUST be in AviationStackParams.
    # I saw shared/config.py earlier and it DID NOT have max_pages.
    # This implies a BUG in config.py or _etl.py.
    # I will stick with what I saw in config.py and report the error if it fails,
    # OR better, I will check config.py again to be absolutely sure.

    print("Running ETL with Mock...")

    # We mock extract_flights to return our dummy file path
    dummy_json_path = os.path.join(raw_dir, "flights_page_0.json")
    with open(dummy_json_path, "w") as f:
        json.dump(dummy_data, f)

    with patch("spark_jobs._etl.extract_flights") as mock_extract:
        mock_extract.return_value = [dummy_json_path]

        # NOTE: If max_pages is accessed in _etl.py but not in Config, this will crash.
        # Let's see if we need to monkeypatch Config or just let it fail to verify the bug.
        # I'll let it fail if the code is indeed broken.

        etl = AviationStackETL(spark, config)
        try:
            etl.run()
        except Exception as e:
            print(f"ETL Failed: {e}")
            # If it failed due to max_pages, we found a bug!
            return

    # Verify Output
    print(f"Verifying output in {bronze_dir}...")
    if os.path.exists(bronze_dir):
        files = os.listdir(bronze_dir)
        parquet_files = [f for f in files if f.endswith(".parquet")]
        if parquet_files:
            print(f"SUCCESS: Found {len(parquet_files)} parquet files.")
            df = spark.read.parquet(bronze_dir)
            df.show()
            print(f"Row count: {df.count()}")
        else:
            print("FAILURE: No parquet files found.")
    else:
        print("FAILURE: Bronze directory not created.")


if __name__ == "__main__":
    verify_etl()
