-- A flight can't depart and arrive at the same airport (diversions keep the planned destination).
select flight_id, origin_airport_code, dest_airport_code
from {{ ref('fct_flights') }}
where origin_airport_code = dest_airport_code
