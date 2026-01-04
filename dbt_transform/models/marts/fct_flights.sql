{{
    config(
        materialized='incremental',
        unique_key='flight_key',
    )
}}

with int_flights as (
    select * from {{ ref('int_flights') }}
)

select * from int_flights
