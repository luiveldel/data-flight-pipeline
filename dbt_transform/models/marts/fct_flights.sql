{{
    config(
        materialized='incremental',
        unique_key='flight_key',
    )
}}

with int_flights as (
    select * from {{ ref('int_flights') }}
)

select
    '{{ var("execution_date") }}'::date as insert_date_ci,
    flight_iata_full,
    flight_number,
    airline_iata,
    dep_iata,
    arr_iata,
    flight_date,
    dep_scheduled,
    dep_actual,
    arr_scheduled,
    arr_actual,
    flight_status,
    dep_delay_min,
    arr_delay_min,
    day_of_week,
    day_of_week_name,
    is_weekend,
    dep_hour,
    time_of_day,
    scheduled_duration_min,
    actual_duration_min,
    duration_diff_min,
    delay_category,
    is_delayed_15,
    avg_delay_7d
from int_flights
