{#
  Staging: one row per raw row, nothing added or removed.
  Job: rename to snake_case, cast text to real types, and decode flags.
  CAST (not TRY_CAST) on purpose: a value that can't be converted fails the build
  loudly instead of becoming a silent NULL.
#}

with source as (

    select * from {{ source('raw', 'flights') }}

),

typed as (

    select
        -- Natural key and its hash (see docs/data-model.md: grain)
        md5(concat_ws('|', FlightDate, Reporting_Airline, Flight_Number_Reporting_Airline, Origin))
                                                        as flight_id,
        cast(FlightDate as date)                        as flight_date,
        Reporting_Airline                               as carrier_code,
        Flight_Number_Reporting_Airline                 as flight_number,

        -- Route
        Origin                                          as origin_airport_code,
        OriginCityName                                  as origin_city_name,
        OriginState                                     as origin_state_code,
        Dest                                            as dest_airport_code,
        DestCityName                                    as dest_city_name,
        DestState                                       as dest_state_code,
        cast(Distance as double)                        as distance_miles,

        -- Times (local airport time, no time zone in the source)
        {{ hhmm_to_time('CRSDepTime') }}                as scheduled_dep_time,
        {{ hhmm_to_time('DepTime') }}                   as actual_dep_time,
        {{ hhmm_to_time('CRSArrTime') }}                as scheduled_arr_time,
        {{ hhmm_to_time('ArrTime') }}                   as actual_arr_time,

        -- Delays in minutes (negative = early)
        cast(DepDelay as double)                        as dep_delay_min,
        cast(ArrDelay as double)                        as arr_delay_min,
        cast(DepDel15 as double) = 1                    as is_dep_delayed_15,
        cast(ArrDel15 as double) = 1                    as is_arr_delayed_15,

        -- Status
        cast(Cancelled as double) = 1                   as is_cancelled,
        cast(Diverted as double) = 1                    as is_diverted,
        CancellationCode                                as cancellation_code,

        -- Time on the ground and in the air
        cast(TaxiOut as double)                         as taxi_out_min,
        cast(TaxiIn as double)                          as taxi_in_min,
        cast(AirTime as double)                         as air_time_min,
        cast(CRSElapsedTime as double)                  as scheduled_elapsed_min,
        cast(ActualElapsedTime as double)               as actual_elapsed_min,

        -- Cause minutes: only reported for flights arriving 15+ minutes late
        cast(CarrierDelay as double)                    as carrier_delay_min,
        cast(WeatherDelay as double)                    as weather_delay_min,
        cast(NASDelay as double)                        as nas_delay_min,
        cast(SecurityDelay as double)                   as security_delay_min,
        cast(LateAircraftDelay as double)               as late_aircraft_delay_min,

        -- Lineage
        period,
        _source                                         as source,
        cast(_ingested_at as timestamptz)               as ingested_at

    from source

)

select
    *,
    case
        when is_cancelled then 'cancelled'
        when is_diverted  then 'diverted'
        else 'completed'
    end                                                 as flight_status,
    case cancellation_code
        when 'A' then 'carrier'
        when 'B' then 'weather'
        when 'C' then 'national air system'
        when 'D' then 'security'
    end                                                 as cancellation_reason
from typed
