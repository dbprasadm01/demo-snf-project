{{ config(materialized='table') }}

with stg as (select * from {{ ref('stg_account') }}),
state as (
  select state_code, state_name, region
  from {{ ref('state_codes') }}  -- seed
)
select
  {{ surrogate_key(['account_id']) }} as account_sk,
  s.account_id,
  s.account_number,
  s.account_name,
  s.segment,
  s.state_code,
  st.state_name,
  st.region,
  s.src_created_at,
  s.src_updated_at,
  s.dbt_loaded_at
from stg s
left join state st using (state_code)
