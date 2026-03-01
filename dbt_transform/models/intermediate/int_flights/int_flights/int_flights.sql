with int_flights_kpis as (
    select * from {{ ref('int_flights_kpis') }}
),

avg_delay_7d as (
    select
        {{ dbt_utils.generate_surrogate_key(['flight_iata_full', 'flight_date']) }} as flight_key,
        flight_iata_full,
        flight_number,
        airline_iata,
        dep_iata,
        arr_iata,
        flight_date,
        dep_scheduled,
        dep_actual,
        arr_scheduled,
        arr_actual,
        flight_status,
        dep_delay_min,
        arr_delay_min,
        day_of_week,
        day_of_week_name,
        is_weekend,
        dep_hour,
        time_of_day,
        scheduled_duration_min,
        actual_duration_min,
        duration_diff_min,
        delay_category,
        is_delayed_15,
        avg(avg_delay) over (
            partition by dep_iata, arr_iata
            order by flight_date
            rows between 6 preceding and current row
        ) as avg_delay_7d
    from int_flights_kpis
    qualify
        row_number() over (
            partition by flight_iata_full, flight_date
            order by ingestion_date desc
        ) = 1
    {% if is_incremental() %}
        where flight_date::date >= (select max(flight_date)::date - 7 from {{ this }})
    {% endif %}
)

select * from avg_delay_7d
