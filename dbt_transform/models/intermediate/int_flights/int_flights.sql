with int_flights_kpis as (
    select * from {{ ref('int_flights_kpis') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['flight_iata_full', 'flight_date']) }} as flight_key,
    flight_date,
    dep_iata,
    arr_iata,
    airline_iata,
    delay_category,
    dep_delay_min,
    avg(avg_delay) over (
        partition by dep_iata, arr_iata
        order by flight_date
        rows between 6 preceding and current row
    ) as avg_delay_7d
from int_flights_kpis
    qualify row_number() over (
        partition by flight_iata_full, flight_date
        order by ingestion_date desc
    ) = 1
{% if is_incremental() %}
    where flight_date::date >= (select max(flight_date)::date - 7 from {{ this }})
{% endif %}
