{{
    config(
        warn_if='> 5',
    )
}}

-- Test that delay_category is consistent with dep_delay_min thresholds
-- Categories: severe_delay (>45), moderate_delay (>15), minor_delay (>0), ontime, early (<=-15), unknown (null)
select * from {{ ref('int_flights_kpis') }}
where not (
    (dep_delay_min is null and delay_category = 'unknown') or
    (dep_delay_min > 45 and delay_category = 'severe_delay') or
    (dep_delay_min > 15 and dep_delay_min <= 45 and delay_category = 'moderate_delay') or
    (dep_delay_min > 0 and dep_delay_min <= 15 and delay_category = 'minor_delay') or
    (dep_delay_min <= -15 and delay_category = 'early') or
    (dep_delay_min > -15 and dep_delay_min <= 0 and delay_category = 'ontime')
)
