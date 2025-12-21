# Spark Jobs

This directory contains PySpark ETL jobs for the Flight Data Pipeline.

## Overview

The main ETL job (`_etl.py`) extracts flight data from the AviationStack API, transforms/flattens the JSON structure, and loads it into a Parquet Bronze layer.

## Prerequisites

- **uv**: This project uses `uv` for dependency management.
- **Python**: 3.10+ (managed automatically by `uv`).
- **Java**: Java 8/11/17 is required for Spark. Ensure `JAVA_HOME` is set if not automatically detected.

## Setup

Environment variables are required for the real API interaction (though the verification script mocks them).

Create a `.env` file in the project root if you plan to run against the real API:

```bash
AVIATIONSTACK_API_KEY=your_api_key
AVIATIONSTACK_BASE_URL=http://api.aviationstack.com/v1
```

## Running Verification

To verify that the ETL logic works correctly without hitting the external API, run the provided test script using `uv`:

```bash
uv run spark_jobs/tests/verify_etl.py
```

### What this script does:

1.  **Mocks the API**: Intercepts calls to `extract_flights` so no API key is needed.
2.  **Runs Spark Local**: Starts a local Spark session.
3.  **Executes ETL**: specificies temporary `raw` and `bronze` directories.
4.  **Validates Output**: Checks that Parquet files are generated in the `bronze` output directory.

## Project Structure

- `_extract.py`: Handles API pagination and raw JSON saving.
- `_transform.py`: Uses Spark to explode and flatten nested JSON data.
- `_load.py`: Writes data to the target sink (Parquet).
- `_etl.py`: The orchestrator class `AviationStackETL`.
- `shared/`: Shared configurations and base classes.
