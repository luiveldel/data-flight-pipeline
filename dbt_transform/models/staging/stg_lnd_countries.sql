{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from {{ source('openflights_raw', 'lnd_countries') }}
)

select
    name::varchar(100) as country_name,
    iso_code::char(2) as country_iso_code
from source
