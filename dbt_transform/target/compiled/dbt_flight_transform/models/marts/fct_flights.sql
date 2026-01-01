

with int_flights_kpis as (
    select * from "analytics"."main"."int_flights_kpis"
)

select
    md5(cast(coalesce(cast(flight_iata_full as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(flight_date as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as flight_key,
    flight_date,
    dep_iata,
    arr_iata,
    airline_iata,
    delay_category,
    dep_delay_min,
    avg(avg_delay) over (
        partition by dep_iata, arr_iata
        order by flight_date
        rows between 6 preceding and current row
    ) as avg_delay_7d
from int_flights_kpis

    where flight_date::DATE >= (select max(flight_date)::DATE - 7 from "analytics"."main"."fct_flights")
