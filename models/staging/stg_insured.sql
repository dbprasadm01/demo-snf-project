{{ config(materialized='view') }}

select
  insured_id,
  account_id,
  first_name,
  last_name,
  email,
  phone,
  address_line1,
  city,
  state_code,
  postal_code,
  {{ surrogate_key(['insured_id','account_id','email']) }} as insured_nk_hash,
  {{ add_audit_columns('created_at','updated_at') }}
from {{ source('raw_pc','insured') }}
