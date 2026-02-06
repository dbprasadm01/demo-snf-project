{{ config(materialized='view') }}

select
  {{ surrogate_key(['policy_id']) }} as policy_sk,
  policy_id,
  policy_number,
  account_id,
  lob_code,
  lob_description,
  policy_status,
  effective_date,
  expiration_date,
  written_premium,
  src_created_at,
  src_updated_at,
  dbt_loaded_at
from {{ ref('stg_policy') }}
