{{
    config(
        severity='error',
        error_if='> 0',
    )
}}

select
    flight_key
from {{ ref('fct_flights') }}
group by 1
having count(*) > 1
