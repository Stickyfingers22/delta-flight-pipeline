-- A cancellation reason must exist for every cancelled flight and for no other flight.
select flight_id, flight_status, cancellation_reason
from {{ ref('fct_flights') }}
where (flight_status = 'cancelled') <> (cancellation_reason is not null)
