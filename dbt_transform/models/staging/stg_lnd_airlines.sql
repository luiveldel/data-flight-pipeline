{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from {{ source('openflights_raw', 'lnd_airlines') }}
)

select
    