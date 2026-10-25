{#
  One row per airline seen in the data. Names come from the carriers seed.
  Built from the codes in the data (not from the seed alone) so a new airline
  appears as 'Unknown' instead of disappearing from joins. Step 6 tests for that.
#}

with codes as (

    select distinct carrier_code from {{ ref('stg_flights') }}

)

select
    codes.carrier_code,
    coalesce(seed.carrier_name, 'Unknown')              as carrier_name,
    coalesce(seed.carrier_type, 'unknown')              as carrier_type
from codes
left join {{ ref('carriers') }} as seed using (carrier_code)
