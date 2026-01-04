
  
    
    

    create  table
      "analytics"."main"."fct_route_daily__dbt_tmp"
  
    as (
      

with fct_flights as (
    select * from "analytics"."main"."fct_flights"
),

base as (
    select
        flight_date,
        dep_iata,
        arr_iata,
        airline_iata,
        dep_delay_min,
        delay_category
    from fct_flights
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
        sum(case when delay_category in ('medium_delay','high_delay') then 1 else 0 end) * 100.0 / count(*) as delayed_pct
    from base
    group by 1,2,3,4,5
)

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
    );
  
  