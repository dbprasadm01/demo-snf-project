{{ config(materialized='view') }}

with base as (
  select
    policy_id,
    policy_number,
    account_id,
    lob_code,
    policy_status,
    effective_date,
    expiration_date,
    written_premium,
    {{ add_audit_columns('created_at','updated_at') }}
  from {{ source('raw_pc','policy') }}
),
lob as (
  select lob_code, lob_description
  from {{ ref('lob_codes') }}   -- seed
)
select b.*, l.lob_description
from base b
left join lob l using (lob_code)
