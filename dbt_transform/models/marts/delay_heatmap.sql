{{
    config(
        materialized='table'
    )
}}

with int_flights as (
    select * from {{ ref('int_flights') }}
),

get_avg_metrics as (
    select
        day_of_week,
        day_of_week_name,
        dep_hour,
        count(*) as total_flights,
        round(avg(dep_delay_min), 2) as avg_delay_min,
        round(avg(arr_delay_min), 2) as avg_arr_delay_min,
        sum(case when is_delayed_15 then 1 else 0 end) as delayed_flights,
        round(sum(case when is_delayed_15 then 1 else 0 end) * 100.0 / count(*), 2) as delayed_pct
    from int_flights
    where dep_hour is not null
    group by 1, 2, 3
    order by 1, 3
)

select
    day_of_week,
    day_of_week_name,
    dep_hour,
    total_flights,
    avg_delay_min,
    avg_arr_delay_min,
    delayed_flights,
    delayed_pct
from get_avg_metrics
