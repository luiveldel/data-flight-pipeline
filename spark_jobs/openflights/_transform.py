from typing import Dict, List, NamedTuple
from pyspark.sql import DataFrame


class OpenFlightSpec(NamedTuple):
    name: str
    filename: str
    columns: List[str]


def get_datasets_spec() -> List[OpenFlightSpec]:
    return [
        OpenFlightSpec(
            name="airports",
            filename="airports.dat",
            columns=[
                "airport_id",
                "name",
                "city",
                "country",
                "iata",
                "icao",
                "latitude",
                "longitude",
                "altitude",
                "timezone",
                "dst",
                "tz_database_time_zone",
                "type",
                "source",
            ],
        ),
        OpenFlightSpec(
            name="airlines",
            filename="airlines.dat",
            columns=[
                "airline_id",
                "name",
                "alias",
                "iata",
                "icao",
                "callsign",
                "country",
                "active",
            ],
        ),
        OpenFlightSpec(
            name="routes",
            filename="routes.dat",
            columns=[
                "airline",
                "airline_id",
                "source_airport",
                "source_airport_id",
                "destination_airport",
                "destination_airport_id",
                "codeshare",
                "stops",
                "equipment",
            ],
        ),
        OpenFlightSpec(
            name="planes",
            filename="planes.dat",
            columns=["name", "iata_code", "icao_code"],
        ),
        OpenFlightSpec(
            name="countries",
            filename="countries.dat",
            columns=["name", "iso_code", "dafif_code"],
        ),
    ]


def transform_files(dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """Transform the extracted DataFrames into a dictionary of processed DataFrames."""
    specs = {spec.name: spec for spec in get_datasets_spec()}

    transformed_dfs = {}
    for name, df in dfs.items():
        if name in specs:
            spec = specs[name]
            # Rename columns based on the spec
            transformed_dfs[name] = df.toDF(*spec.columns)
        else:
            transformed_dfs[name] = df

    return transformed_dfs
