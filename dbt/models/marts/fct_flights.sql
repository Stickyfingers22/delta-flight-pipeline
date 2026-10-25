{#
  Fact table. Grain: one row per scheduled flight leg (completed, cancelled or diverted).
  Holds keys to the dimensions plus the measurements. Descriptive text (city names,
  airline names) lives in the dimensions, not here.
#}

select
    flight_id,
    cast(strftime(flight_date, '%Y%m%d') as integer)   as date_key,
    carrier_code,
    origin_airport_code,
    dest_airport_code,
    flight_number,

    scheduled_dep_time,
    hour(scheduled_dep_time)                            as scheduled_dep_hour,
    scheduled_arr_time,
    actual_dep_time,
    actual_arr_time,

    flight_status,
    cancellation_reason,

    dep_delay_min,
    arr_delay_min,
    is_dep_delayed_15,
    is_arr_delayed_15,

    distance_miles,
    taxi_out_min,
    taxi_in_min,
    air_time_min,
    scheduled_elapsed_min,
    actual_elapsed_min,

    carrier_delay_min,
    weather_delay_min,
    nas_delay_min,
    security_delay_min,
    late_aircraft_delay_min,

    period

from {{ ref('stg_flights') }}
