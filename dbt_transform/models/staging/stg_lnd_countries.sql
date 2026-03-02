{{
    config(
        materialized="view"
    )
}}

with source as (
    select * from read_parquet('s3://flights-data-lake/bronze/openflights/countries.parquet/*.parquet')
)

select
    name::varchar(100) as country_name,
    iso_code::char(2) as country_iso_code
from source
