{{
    config(
        materialized='incremental',
        unique_key=['insert_date_ci'],
    )
}}

with int_flights_kpis as (
    select * from {{ ref('int_flights_kpis') }}
)

select * from int_flights_kpis
