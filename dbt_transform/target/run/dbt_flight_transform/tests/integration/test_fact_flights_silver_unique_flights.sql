select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) > 0 as should_error
    from (
      
        select *
        from "analytics"."main_metadata"."test_fact_flights_silver_unique_flights"
    
      
    ) dbt_internal_test