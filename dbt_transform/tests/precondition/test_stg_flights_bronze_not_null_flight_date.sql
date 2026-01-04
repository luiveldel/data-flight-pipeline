{{
    config(
        error_if='> 0',
        warn_if='> 0',
        tags=["precondition-test"],
    )
}}

select *
from {{ ref('stg_lnd_flights') }}
where flight_date is null
