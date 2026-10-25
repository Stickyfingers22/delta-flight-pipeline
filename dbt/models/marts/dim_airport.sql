{#
  One row per airport, from both origins and destinations (an airport can appear
  only as a destination in a given month). If a city name ever changes, the most
  recent flight's value wins: SCD Type 1, overwrite with no history.
#}

with sightings as (

    select origin_airport_code as airport_code, origin_city_name as city_name,
           origin_state_code as state_code, flight_date
    from {{ ref('stg_flights') }}

    union all

    select dest_airport_code, dest_city_name, dest_state_code, flight_date
    from {{ ref('stg_flights') }}

)

select
    airport_code,
    arg_max(city_name, flight_date)                     as city_name,
    arg_max(state_code, flight_date)                    as state_code
from sightings
group by airport_code
