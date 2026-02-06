{{ config(materialized='view') }}

select
  claim_id,
  claim_number,
  policy_id,
  loss_date,
  claim_status,
  reported_at,
  updated_at as src_updated_at,
  current_timestamp() as dbt_loaded_at
from {{ source('raw_pc','claim') }}
