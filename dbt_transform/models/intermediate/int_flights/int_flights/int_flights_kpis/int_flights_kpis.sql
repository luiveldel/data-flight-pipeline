with stg_lnd_flights as (
    select * from {{ ref('stg_lnd_flights') }}
)

select
    *,
    date_part('dow', flight_date) as day_of_week,
    dayname(flight_date) as day_of_week_name,
    coalesce(date_part('dow', flight_date) in (0, 6), false) as is_weekend,
    date_part('hour', dep_scheduled) as dep_hour,
    case
        when date_part('hour', dep_scheduled) between 5 and 11 then 'Morning'
        when date_part('hour', dep_scheduled) between 12 and 16 then 'Afternoon'
        when date_part('hour', dep_scheduled) between 17 and 21 then 'Evening'
        else 'Night'
    end as time_of_day,
    date_diff('minute', dep_scheduled, arr_scheduled) as scheduled_duration_min,
    date_diff('minute', dep_actual, arr_actual) as actual_duration_min,
    (date_diff('minute', dep_actual, arr_actual) - date_diff('minute', dep_scheduled, arr_scheduled)) as duration_diff_min,
    case
        when dep_delay_min is null then 'unknown'
        when dep_delay_min > 45 then 'severe_delay'
        when dep_delay_min > 15 then 'moderate_delay'
        when dep_delay_min > 0 then 'minor_delay'
        when dep_delay_min <= -15 then 'early'
        else 'ontime'
    end as delay_category,
    coalesce(dep_delay_min > 15, false) as is_delayed_15,
    dep_delay_min as avg_delay
from stg_lnd_flights
