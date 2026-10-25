-- Cancelled and diverted flights never reached their gate on schedule, so an arrival delay
-- for them means a parsing or status bug. Completed flights must always have one.
select flight_id, flight_status, arr_delay_min
from {{ ref('fct_flights') }}
where (flight_status <> 'completed' and arr_delay_min is not null)
   or (flight_status =  'completed' and arr_delay_min is null)
