{{
    config(
        severity='error',
        error_if='> 0',
    )
}}

with fct_route_daily as (
    select * from {{ ref('fct_route_daily') }}
),

duplicates as (
    select flight_iata_full, flight_date
    from fct_route_daily
    group by 1,2
    having count(*) > 1
)

select count(*) from duplicates
