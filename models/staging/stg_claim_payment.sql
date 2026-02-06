{{ config(materialized='view') }}

select
  payment_id,
  claim_id,
  payment_ts,
  payment_amount,
  payment_type,
  updated_at as src_updated_at,
  current_timestamp() as dbt_loaded_at
from {{ source('raw_pc','claim_payment') }}
