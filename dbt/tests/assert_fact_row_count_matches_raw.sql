-- Reconciliation: every raw row becomes exactly one fact row. Nothing dropped, nothing duplicated.
-- Returns a row (= failure) only if the counts differ.
with counts as (
    select
        (select count(*) from {{ source('raw', 'flights') }}) as raw_rows,
        (select count(*) from {{ ref('fct_flights') }})      as fact_rows
)
select * from counts where raw_rows <> fact_rows
