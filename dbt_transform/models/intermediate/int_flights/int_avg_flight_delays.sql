with int_flights as (
    select * from {{ ref('int_flights') }}
),

agg as (
    select
        flight_date,
        dep_iata,
        arr_iata,
        dep_iata || '-' || arr_iata as route,
        airline_iata,
        count(*) as total_flights,
        avg(dep_delay_min) as avg_dep_delay_min,
        sum(case when delay_category in ('moderate_delay', 'severe_delay') then 1 else 0 end) * 100.0 / count(*) as delayed_pct
    from int_flights
    group by 1, 2, 3, 4, 5
),

avg_delayed_pct as (
    select
        flight_date,
        route,
        dep_iata,
        arr_iata,
        airline_iata,
        total_flights,
        round(avg_dep_delay_min, 2) as avg_dep_delay_min,
        round(delayed_pct, 2) as delayed_pct
    from agg
)