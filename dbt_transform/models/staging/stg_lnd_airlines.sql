{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from read_parquet('s3://flights-data-lake/bronze/openflights/airlines.parquet/*.parquet')
)

select
    airline_id::int,
    name::varchar(100) as airline_name,
    alias::varchar(50) as airline_alias,
    iata::char(2) as iata_code,
    icao::char(3) as icao_code,
    callsign::varchar(100) as airline_callsign,
    country::varchar(100) as country,
    active::char(1) as is_active
from source
