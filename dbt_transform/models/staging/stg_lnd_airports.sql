{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from read_parquet('s3://flights-data-lake/bronze/openflights/airports.parquet/*.parquet')
)

select
    airport_id::int,
    name::varchar(100) as airport_name,
    city::varchar(50) as airport_city,
    country::varchar(50) as airport_country,
    iata::char(3) as airport_iata_code,
    icao::char(4) as airport_icao_code,
    latitude::decimal(10, 6) as airport_latitude,
    longitude::decimal(10, 6) as airport_longitude,
    altitude::int as airport_altitude_feet,
    timezone::decimal(4, 2) as airport_timezone,
    dst::char(1) as daylight_saving_time,
    tz_database_time_zone::varchar(50) as airport_tz_database_time_zone,
    type::varchar(50) as airport_type,
    source::varchar(50) as airport_source
from source
