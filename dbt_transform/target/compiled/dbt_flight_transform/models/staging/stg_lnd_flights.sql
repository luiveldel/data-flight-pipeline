

with source as (
    select * from 's3://flights-data-lake/bronze/insert_date=2025-01-01/*.parquet'
)

select
    '2025-01-01'::date as insert_date_ci,
    flight_date::date as flight_date,
    flight_status::varchar(20) as flight_status,
    dep_airport::varchar(100) as dep_airport,
    dep_iata::varchar(3) as dep_iata,
    dep_scheduled::timestamp as dep_scheduled,
    dep_actual as dep_actual,
    arr_airport::varchar(100) as arr_airport,
    arr_iata::varchar(3) as arr_iata,
    arr_scheduled::timestamp as arr_scheduled,
    arr_actual as arr_actual,
    airline_iata::varchar(3) as airline_iata,
    flight_number::varchar(10) as flight_number,
    airline_iata || flight_number as flight_iata_full,
    dep_delay_min::int as dep_delay_min,
    arr_delay_min::int as arr_delay_min,
    dep_scheduled_ts::timestamp as dep_scheduled_ts,
    arr_scheduled_ts::timestamp as arr_scheduled_ts,
    ingestion_date::date as ingestion_date
from source
where flight_date::date is not null