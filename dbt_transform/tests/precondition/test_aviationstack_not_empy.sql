{{
    config(
        error_if="= 0",
        warn_if="= 0",
        tags=["precondition-test"],
    )
}}

with source as (
    select * from {{ source("flights_raw", "avionstack_flights") }}
)

select *
from source
limit 1
