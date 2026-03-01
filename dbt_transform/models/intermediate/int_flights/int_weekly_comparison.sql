with int_flights as (
    select * from {{ ref('int_flights') }}
),

get_weekly_metrics as (
    select
        date_trunc('week', flight_date) as week_start,
        count(*) as total_flights,
        round(avg(dep_delay_min), 2) as avg_delay_min,
        round(avg(arr_delay_min), 2) as avg_arr_delay_min,
        sum(case when delay_category in ('ontime', 'early') then 1 else 0 end) as ontime_flights,
        sum(case when delay_category = 'severe_delay' then 1 else 0 end) as severe_delay_flights,
        round(
            sum(case when delay_category in ('ontime', 'early') then 1 else 0 end) * 100.0 / count(*),
            2
        ) as ontime_pct,
        round(
            sum(case when delay_category = 'severe_delay' then 1 else 0 end) * 100.0 / count(*),
            2
        ) as severe_pct
    from int_flights
    group by 1
),

with_lag as (
    select
        week_start,
        total_flights,
        avg_delay_min,
        avg_arr_delay_min,
        ontime_flights,
        severe_delay_flights,
        ontime_pct,
        severe_pct,
        lag(total_flights) over (order by week_start) as prev_week_flights,
        lag(avg_delay_min) over (order by week_start) as prev_week_avg_delay,
        lag(ontime_pct) over (order by week_start) as prev_week_ontime_pct,
        lag(severe_pct) over (order by week_start) as prev_week_severe_pct
    from get_weekly_metrics
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
    -- Week-over-week changes
    round(total_flights - coalesce(prev_week_flights, 0), 0) as flights_change,
    round(avg_delay_min - coalesce(prev_week_avg_delay, 0), 2) as delay_change,
    round(ontime_pct - coalesce(prev_week_ontime_pct, 0), 2) as ontime_pct_change,
    round(severe_pct - coalesce(prev_week_severe_pct, 0), 2) as severe_pct_change
from with_lag
order by 1 desc
