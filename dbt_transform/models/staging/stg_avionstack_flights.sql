with source as (
    select * from {{ source('flights_staging', 'avionstack_flights') }}
)

select
    flight_date,
    flight_status,
    dep_airport,
    dep_iata,
    arr_airport,
    arr_iata,
    airline_name,
    flight_number,
    dep_delay_min::int as dep_delay_min,
    arr_delay_min::int as arr_delay_min,
    dep_scheduled_ts::timestamp as dep_scheduled_at,
    arr_scheduled_ts::timestamp as arr_scheduled_at,
    "{{ var('execution_date') }}" as insert_date_ci
from source
