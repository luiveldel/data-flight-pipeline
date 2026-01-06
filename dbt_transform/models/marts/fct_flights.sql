{{
    config(
        materialized='incremental',
        unique_key='flight_key',
    )
}}

with int_flights as (
    select * from {{ ref('int_flights') }}
)

select
    '{{ var("execution_date") }}'::date as insert_date_ci,
    *
from int_flights
