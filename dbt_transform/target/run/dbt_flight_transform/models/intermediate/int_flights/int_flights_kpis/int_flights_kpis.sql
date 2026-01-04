
  
  create view "analytics"."main"."int_flights_kpis__dbt_tmp" as (
    with stg_lnd_flights as (
    select * from "analytics"."main"."stg_lnd_flights"
)

select
    *,
    case
        when dep_delay_min is null then 'unknown'
        when dep_delay_min > 30 then 'high_delay'
        when dep_delay_min > 15 then 'medium_delay'
        when dep_delay_min <= -15 then 'early'
        else 'ontime'
    end as delay_category,
    dep_delay_min as avg_delay
from stg_lnd_flights
  );
