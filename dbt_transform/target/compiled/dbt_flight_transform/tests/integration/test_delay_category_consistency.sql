

select *
from "analytics"."main"."int_flights_kpis"
where not (
    (dep_delay_min is null and delay_category = 'unknown') or
    (dep_delay_min > 30 and delay_category = 'high_delay') or
    (dep_delay_min > 15 and dep_delay_min <= 30 and delay_category = 'medium_delay') or
    (dep_delay_min <= -15 and delay_category = 'early') or
    (dep_delay_min > -15 and dep_delay_min <= 15 and delay_category = 'ontime')
)