{{
    config(
        materialized='table'
    )
}}

with int_weekly_comparison as (
    select * from {{ ref('int_weekly_comparison') }}
)

select
    week_start,
    total_flights,
    avg_delay_min,
    avg_arr_delay_min,
    ontime_pct,
    severe_pct,
    prev_week_flights,
    prev_week_avg_delay,
    prev_week_ontime_pct,
    prev_week_severe_pct,
    flights_change,
    delay_change,
    ontime_pct_change,
    severe_pct_change
from int_weekly_comparison
