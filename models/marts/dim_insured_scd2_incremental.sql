{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='insured_scd2_key',
    schema='analytics'
) }}

with src as (
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
    src_created_at,
    src_updated_at,
    {{ surrogate_key([
      "insured_id","account_id","first_name","last_name","email","phone",
      "address_line1","city","state_code","postal_code"
    ]) }} as record_hash
  from {{ ref('stg_insured') }}
),

current_tgt as (
  {% if is_incremental() %}
    select * from {{ this }} where is_current = true
  {% else %}
    select * from {{ this }} where 1=0
  {% endif %}
),

changed as (
  select s.*
  from src s
  left join current_tgt t on s.insured_id = t.insured_id
  where t.insured_id is null
     or s.record_hash <> t.record_hash
),

to_close as (
  select
    t.insured_scd2_key,
    t.insured_id,
    t.account_id,
    t.first_name,
    t.last_name,
    t.email,
    t.phone,
    t.address_line1,
    t.city,
    t.state_code,
    t.postal_code,
    t.record_hash,
    t.valid_from,
    c.src_updated_at as valid_to,
    false as is_current,
    t.created_at,
    c.src_updated_at as updated_at
  from current_tgt t
  join changed c on t.insured_id = c.insured_id
),

to_insert as (
  select
    concat(cast(c.insured_id as string), '_', cast(c.src_updated_at as string)) as insured_scd2_key,
    c.insured_id,
    c.account_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.address_line1,
    c.city,
    c.state_code,
    c.postal_code,
    c.record_hash,
    c.src_updated_at as valid_from,
    null::timestamp_ntz as valid_to,
    true as is_current,
    c.src_created_at as created_at,
    c.src_updated_at as updated_at
  from changed c
)

select * from to_close
union all
select * from to_insert
