{{
    config(
        error_if='> 0',
        warn_if='> 0',
        tags=["precondition-test"],
    )
}}

with stg_flights_bronze as (
    select * from {{ ref('stg_flights_bronze') }}
)

select count(*) as null_count
from stg_flights_bronze
where flight_date is null
