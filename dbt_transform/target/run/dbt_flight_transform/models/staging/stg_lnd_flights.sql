
  
  create view "analytics"."main"."stg_lnd_flights__dbt_tmp" as (
    

with source as (
    select * from 's3://flights-data-lake/bronze/insert_date=2025-01-01/*.parquet'
)

select * from source
  );
