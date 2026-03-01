{{
    config(
        materialized='table',
    )
}}

with int_avg_flight_delays as (
    select * from {{ ref('int_avg_flight_delays') }}
)

select
    flight_date,
    route,
    dep_iata,
    arr_iata,
    airline_iata,
    total_flights,
    avg_dep_delay_min,
    delayed_pct
from int_avg_flight_delays
