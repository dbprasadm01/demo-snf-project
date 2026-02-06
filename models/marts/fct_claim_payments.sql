{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id'
) }}

select
  p.payment_id,
  p.claim_id,
  c.policy_id,
  p.payment_ts,
  p.payment_amount,
  p.payment_type,
  p.src_updated_at,
  p.dbt_loaded_at
from {{ ref('stg_claim_payment') }} p
join {{ ref('stg_claim') }} c on p.claim_id = c.claim_id

{% if is_incremental() %}
where p.src_updated_at > (select coalesce(max(src_updated_at), '1900-01-01') from {{ this }})
{% endif %}
