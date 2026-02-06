select
  insured_id
from {{ ref('dim_insured_scd2_incremental') }}
where is_current = true
group by insured_id
having count(*) > 1
