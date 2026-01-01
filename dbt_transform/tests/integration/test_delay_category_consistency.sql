{{
    config(
        warn_if='> 5',
    )
}}

select count(*) * 100.0 / count(*) as inconsistent_pct
from {{ ref('fact_flights_silver') }}
where (
    dep_delay_min > 30 and delay_category != 'high_delay' or
    dep_delay_min > 15 and delay_category != 'medium_delay' or
    dep_delay_min <= 15 and delay_category != 'ontime'
)