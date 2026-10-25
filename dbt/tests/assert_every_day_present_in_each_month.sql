-- Completeness: a loaded month must contain every calendar day.
-- A missing day usually means a truncated or partial export.
with days_seen as (
    select period, count(distinct date_key) as days_with_flights
    from {{ ref('fct_flights') }}
    group by period
)
select period, days_with_flights,
       day(last_day(cast(period || '-01' as date))) as days_in_month
from days_seen
where days_with_flights <> day(last_day(cast(period || '-01' as date)))
