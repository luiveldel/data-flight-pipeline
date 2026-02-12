{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from {{ source('openflights_raw', 'lnd_routes') }}
)

select
    airline::varchar(3) as airline_code,
    airline_id::int,
    source_airport::varchar(4) as source_airport_code,
    source_airport_id::int,
    destination_airport::varchar(4) as destination_airport_code,
    destination_airport_id::int,
    codeshare::varchar(1) as codeshare,
    stops::smallint as stops,
    equipment::varchar(3) as plane_type
from source
