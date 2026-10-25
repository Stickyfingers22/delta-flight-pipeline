{#
  One row per calendar day, generated rather than taken from flights so that
  every day exists even if it had no flights. Covers full years around the data.
#}

with bounds as (

    select
        date_trunc('year', min(flight_date))                          as first_day,
        date_trunc('year', max(flight_date)) + interval 1 year - interval 1 day as last_day
    from {{ ref('stg_flights') }}

),

days as (

    select cast(d as date) as full_date
    from bounds, generate_series(first_day, last_day, interval 1 day) as t(d)

)

select
    cast(strftime(full_date, '%Y%m%d') as integer)     as date_key,
    full_date,
    year(full_date)                                     as year,
    quarter(full_date)                                  as quarter,
    month(full_date)                                    as month,
    strftime(full_date, '%B')                           as month_name,
    day(full_date)                                      as day_of_month,
    isodow(full_date)                                   as day_of_week,      -- 1 = Monday
    strftime(full_date, '%A')                           as day_name,
    isodow(full_date) in (6, 7)                         as is_weekend,
    h.holiday_name,
    h.holiday_name is not null                          as is_holiday
from days
left join {{ ref('us_holidays') }} as h on h.holiday_date = days.full_date
