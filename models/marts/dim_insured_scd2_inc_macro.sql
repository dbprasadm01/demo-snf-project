{{ config(
  materialized='table'
) }}


select 1 as id
{#

{{ config(
  materialized='table',
  post_hook=[
    "{{ scd2_merge_snowflake(
        source_relation=this,
        target_relation='dbt_cloud_snowflake_db.analytics.dim_insured_scd2',
        natural_key_cols=['insured_id'],
        attribute_cols=['account_id','first_name','last_name','email','phone','address_line1','city','state_code','postal_code'],
        src_updated_at_col='src_updated_at',
        scd2_pk_col='insured_scd2_key',
        valid_from_col='valid_from',
        valid_to_col='valid_to',
        is_current_col='is_current',
        created_at_col='created_at',
        updated_at_col='updated_at'
    ) }}"
  ]
) }}

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
  src_updated_at
from {{ ref('stg_insured') }}

#}