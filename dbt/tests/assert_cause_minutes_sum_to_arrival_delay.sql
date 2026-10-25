{#
  The five cause buckets should add up to the arrival delay.
  November 2025 has 11 known exceptions: Southwest flights 93-820 minutes late that
  reported 0 for every cause. That's a gap in the source data, not a pipeline bug, so
  the test warns on a handful of rows and fails only if the problem grows past 100.
#}
{{ config(severity = 'error', warn_if = '>0', error_if = '>100') }}

select flight_id, carrier_code, arr_delay_min,
       carrier_delay_min + weather_delay_min + nas_delay_min
         + security_delay_min + late_aircraft_delay_min as cause_minutes
from {{ ref('fct_flights') }}
where is_arr_delayed_15
  and abs(carrier_delay_min + weather_delay_min + nas_delay_min
          + security_delay_min + late_aircraft_delay_min - arr_delay_min) > 0.5
