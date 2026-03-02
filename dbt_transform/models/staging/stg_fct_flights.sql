with source as (
    select * from {{ source('analytics', 'fct_flights') }}
)

select * from source
