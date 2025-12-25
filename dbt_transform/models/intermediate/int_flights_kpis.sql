with stg_flights as (
    select * from {{ ref('stg_flights') }}
)

select
    *,
    case
        when dep_delay_min > 15 then 'delayed'
        when dep_delay_min <= 15 then 'on time'
        else 'unknown'
    end as departure_performance
from stg_flights
