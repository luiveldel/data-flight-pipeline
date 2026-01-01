{{
    config(
        materialized="view",
        pre_hook='''{{
            copy_to_minio(
                target_table=source("flights_raw", "lnd_flights").identifier,
                column_names=[
                    ("flight_date", "date"),
                    ("dep_iata", "varchar(3)"),
                    ("arr_iata", "varchar(3)"),
                    ("airline_iata", "varchar(3)"),
                    ("flight_number", "varchar(10)"),
                    ("dep_delay_min", "int"),
                    ("arr_delay_min", "int"),
                    ("flight_status", "varchar(20)"),
                    ("ingestion_date", "date")
                ],
                s3_path=var("s3_bronze_path") ~ var("execution_date")
            )
        }}''',
    )
}}

with source as (
    select * from {{ source("flights_raw", "lnd_flights") }}
)

select * from source
