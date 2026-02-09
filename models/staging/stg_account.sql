{{ config(materialized='view') }}
select
  account_id,
  account_number,
  account_name,
  segment,
  state_code,
  {{ add_audit_columns('created_at','updated_at') }}
from {{ source('raw_pc','account') }}
