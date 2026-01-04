select
      count(*) as failures,
      count(*) > 5 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from "analytics"."main_metadata"."test_delay_category_consistency"
    
      
    ) dbt_internal_test