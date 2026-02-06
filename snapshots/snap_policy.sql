{% snapshot snap_policy %}
{{
  config(
    target_database='dbt_cloud_snowflake_db',
    target_schema='analytics',
    unique_key='policy_id',
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=true
  )
}}

select
  policy_id,
  policy_number,
  account_id,
  lob_code,
  policy_status,
  effective_date,
  expiration_date,
  written_premium,
  updated_at
from {{ source('raw_pc','policy') }}

{% endsnapshot %}
