-- BTS reports delay causes exactly for flights arriving 15+ minutes late.
select flight_id, is_arr_delayed_15, carrier_delay_min
from {{ ref('fct_flights') }}
where coalesce(is_arr_delayed_15, false) <> (carrier_delay_min is not null)
