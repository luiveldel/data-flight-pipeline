from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    lit,
    to_timestamp,
)


def transform_flights(df_raw: DataFrame) -> DataFrame:
    """
    TRANSFORM:
    - Explode 'data' y flatea estructura.
    """
    df = df_raw.select(explode("data").alias("flight"))

    df_flat = df.select(
        col("flight.flight_date").alias("flight_date"),
        col("flight.flight_status").alias("flight_status"),

        col("flight.departure.airport").alias("dep_airport"),
        col("flight.departure.iata").alias("dep_iata"),
        col("flight.departure.icao").alias("dep_icao"),
        col("flight.departure.scheduled").alias("dep_scheduled"),
        col("flight.departure.actual").alias("dep_actual"),
        col("flight.departure.delay").alias("dep_delay_min"),

        col("flight.arrival.airport").alias("arr_airport"),
        col("flight.arrival.iata").alias("arr_iata"),
        col("flight.arrival.icao").alias("arr_icao"),
        col("flight.arrival.scheduled").alias("arr_scheduled"),
        col("flight.arrival.actual").alias("arr_actual"),
        col("flight.arrival.delay").alias("arr_delay_min"),

        col("flight.airline.name").alias("airline_name"),
        col("flight.airline.iata").alias("airline_iata"),
        col("flight.airline.icao").alias("airline_icao"),

        col("flight.flight.number").alias("flight_number"),
        col("flight.flight.iata").alias("flight_iata_full"),
        col("flight.flight.icao").alias("flight_icao_full"),

        col("flight.aircraft.registration").alias("aircraft_reg"),
        col("flight.aircraft.iata").alias("aircraft_iata"),
        col("flight.aircraft.icao").alias("aircraft_icao"),

        col("flight.live.latitude").alias("live_latitude"),
        col("flight.live.longitude").alias("live_longitude"),
        col("flight.live.altitude").alias("live_altitude"),
        col("flight.live.is_ground").alias("live_is_ground"),
    )

    df_clean = (
        df_flat
        .withColumn("dep_scheduled_ts", to_timestamp("dep_scheduled"))
        .withColumn("dep_actual_ts", to_timestamp("dep_actual"))
        .withColumn("arr_scheduled_ts", to_timestamp("arr_scheduled"))
        .withColumn("arr_actual_ts", to_timestamp("arr_actual"))
        .withColumn("dep_delay_min", col("dep_delay_min").cast("int"))
        .withColumn("arr_delay_min", col("arr_delay_min").cast("int"))
        .withColumn("ingestion_date", lit(date.today().isoformat()))
    )

    return df_clean
