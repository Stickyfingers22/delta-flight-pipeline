{#
  Sanity bounds: more than 2 hours early or more than ~2 days late is almost certainly
  a data error. Warn only: extreme but real delays exist (max seen: 2,733 minutes).
#}
{{ config(severity = 'warn') }}

select flight_id, dep_delay_min, arr_delay_min
from {{ ref('fct_flights') }}
where dep_delay_min not between -120 and 3000
   or arr_delay_min not between -120 and 3000
