select
      count(*) as failures,
      count(*) > 0 as should_warn,
      count(*) > 0 as should_error
    from (
      
        select *
        from "analytics"."main_metadata"."test_stg_flights_bronze_not_null_flight_date"
    
      
    ) dbt_internal_test