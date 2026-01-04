

select
    flight_key
from "analytics"."main"."fct_flights"
group by 1
having count(*) > 1