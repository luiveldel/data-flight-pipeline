
  
    
    

    create  table
      "analytics"."main"."fct_flights__dbt_tmp"
  
    as (
      

with int_flights as (
    select * from "analytics"."main"."int_flights"
)

select * from int_flights
    );
  
  
  