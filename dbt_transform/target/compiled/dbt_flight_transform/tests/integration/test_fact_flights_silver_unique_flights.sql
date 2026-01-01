

with fct_route_daily as (
    select * from "airflow"."flights_staging"."fct_route_daily"
),

duplicates as (
    select flight_iata_full, flight_date
    from fct_route_daily
    group by 1,2
    having count(*) > 1
)

select count(*) from duplicates