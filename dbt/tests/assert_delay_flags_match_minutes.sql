-- The 15-minute flags must agree with the delay minutes they summarize.
select flight_id, dep_delay_min, is_dep_delayed_15, arr_delay_min, is_arr_delayed_15
from {{ ref('fct_flights') }}
where (dep_delay_min is not null and is_dep_delayed_15 <> (dep_delay_min >= 15))
   or (arr_delay_min is not null and is_arr_delayed_15 <> (arr_delay_min >= 15))
   or (dep_delay_min is null and is_dep_delayed_15 is not null)
   or (arr_delay_min is null and is_arr_delayed_15 is not null)
