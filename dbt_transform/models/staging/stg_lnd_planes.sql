{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from {{ source('openflights_raw', 'lnd_planes') }}
)

select
    name::varchar(50) as aircraft_name,
    iata_code::char(3) as iata_code,
    icao_code::char(4) as icao_code
from source
