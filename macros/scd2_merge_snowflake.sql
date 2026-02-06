{% macro scd2_merge_snowflake(
    source_relation,
    target_relation,
    natural_key_cols,
    attribute_cols,
    src_updated_at_col,
    scd2_pk_col=None,
    scd2_pk_expr=None,
    record_hash_col='record_hash',
    record_hash_expr=None,
    valid_from_col='valid_from',
    valid_to_col='valid_to',
    is_current_col='is_current',
    created_at_col=None,
    created_at_expr=None,
    updated_at_col=None,
    updated_at_expr=None,
    additional_insert_cols=[],
    source_filter_sql=None
) %}

{# -----------------------------
  Validate inputs
------------------------------ #}
{% if natural_key_cols is string %}
  {% do exceptions.raise_compiler_error("natural_key_cols must be a list of column names (not a string).") %}
{% endif %}
{% if attribute_cols is string %}
  {% do exceptions.raise_compiler_error("attribute_cols must be a list of column names (not a string).") %}
{% endif %}
{% if natural_key_cols | length == 0 %}
  {% do exceptions.raise_compiler_error("natural_key_cols must contain at least one column.") %}
{% endif %}

{% set nk = natural_key_cols %}
{% set attrs = attribute_cols %}
{% set extras = additional_insert_cols %}

{# NK join predicate: tgt.nk1 = src.nk1 AND tgt.nk2 = src.nk2 ... #}
{% set nk_join = [] %}
{% for c in nk %}
  {% do nk_join.append("tgt." ~ c ~ " = src." ~ c) %}
{% endfor %}
{% set nk_join_sql = nk_join | join(" and ") %}

{# Default record hash expression (if not provided) #}
{% if record_hash_expr is none %}
  {% set hash_parts = [] %}
  {% for c in nk + attrs %}
    {% do hash_parts.append("coalesce(cast(src." ~ c ~ " as string), '')") %}
  {% endfor %}
  {% set record_hash_expr = "md5(concat_ws('||'," ~ (hash_parts | join(", ")) ~ "))" %}
{% endif %}

{# Default SCD2 PK expression if PK column provided but expr not provided #}
{% if scd2_pk_col is not none and scd2_pk_expr is none %}
  {% set pk_parts = [] %}
  {% for c in nk %}
    {% do pk_parts.append("cast(src." ~ c ~ " as string)") %}
  {% endfor %}
  {% do pk_parts.append("cast(src." ~ src_updated_at_col ~ " as string)") %}
  {% set scd2_pk_expr = "concat_ws('_'," ~ (pk_parts | join(", ")) ~ ")" %}
{% endif %}

{# Default updated_at expression if not provided (only used when updated_at_col is passed) #}
{% if updated_at_expr is none %}
  {% set updated_at_expr = "src." ~ src_updated_at_col %}
{% endif %}

{# -----------------------------
  Source CTE (optionally filtered)
  - Projects only columns required for merge
  - Adds record_hash and optional scd2_pk/audit cols
------------------------------ #}
{% set projection_cols = nk + attrs + extras %}
{% set projection_sql = projection_cols | join(", ") %}

{% set source_cte %}
with src as (
  select
    {{ projection_sql }}{% if projection_cols | length > 0 %},{% endif %}
    {{ src_updated_at_col }} as {{ src_updated_at_col }},
    {{ record_hash_expr }} as {{ record_hash_col }}
    {%- if scd2_pk_col is not none -%},
    {{ scd2_pk_expr }} as {{ scd2_pk_col }}
    {%- endif -%}
    {%- if created_at_col is not none and created_at_expr is not none -%},
    {{ created_at_expr }} as {{ created_at_col }}
    {%- endif -%}
  from {{ source_relation }}
  {%- if source_filter_sql is not none -%}
  where {{ source_filter_sql }}
  {%- endif -%}
),
current_tgt as (
  select *
  from {{ target_relation }}
  where {{ is_current_col }} = true
)
{% endset %}

{# -----------------------------
  1) Close changed current rows
------------------------------ #}
{% set update_sql %}
{{ source_cte }}
update {{ target_relation }} tgt
set
  {{ valid_to_col }} = src.{{ src_updated_at_col }},
  {{ is_current_col }} = false
  {%- if updated_at_col is not none -%}
  , {{ updated_at_col }} = {{ updated_at_expr }}
  {%- endif -%}
from src
where tgt.{{ is_current_col }} = true
  and {{ nk_join_sql }}
  and coalesce(tgt.{{ record_hash_col }}, '') <> coalesce(src.{{ record_hash_col }}, '');
{% endset %}

{# -----------------------------
  2) Insert new current rows
     - New NK or changed hash vs current
------------------------------ #}
{% set insert_cols = [] %}
{% set select_cols = [] %}

{# Optional PK #}
{% if scd2_pk_col is not none %}
  {% do insert_cols.append(scd2_pk_col) %}
  {% do select_cols.append("src." ~ scd2_pk_col) %}
{% endif %}

{# NK + attributes + extras #}
{% for c in nk + attrs + extras %}
  {% do insert_cols.append(c) %}
  {% do select_cols.append("src." ~ c) %}
{% endfor %}

{# record_hash #}
{% do insert_cols.append(record_hash_col) %}
{% do select_cols.append("src." ~ record_hash_col) %}

{# SCD2 metadata #}
{% do insert_cols.append(valid_from_col) %}
{% do select_cols.append("src." ~ src_updated_at_col) %}

{% do insert_cols.append(valid_to_col) %}
{% do select_cols.append("null::timestamp_ntz") %}

{% do insert_cols.append(is_current_col) %}
{% do select_cols.append("true") %}

{# optional audit cols #}
{% if created_at_col is not none %}
  {% do insert_cols.append(created_at_col) %}
  {% if created_at_expr is not none %}
    {% do select_cols.append("src." ~ created_at_col) %}
  {% else %}
    {% do select_cols.append("src." ~ src_updated_at_col) %}
  {% endif %}
{% endif %}

{% if updated_at_col is not none %}
  {% do insert_cols.append(updated_at_col) %}
  {% do select_cols.append(updated_at_expr) %}
{% endif %}

{% set insert_sql %}
{{ source_cte }}
insert into {{ target_relation }} ({{ insert_cols | join(", ") }})
select
  {{ select_cols | join(",\n  ") }}
from src
left join current_tgt tgt
  on {{ nk_join_sql }}
where
  tgt.{{ nk[0] }} is null
  or coalesce(tgt.{{ record_hash_col }}, '') <> coalesce(src.{{ record_hash_col }}, '');
{% endset %}

{# Execute statements #}
{% do run_query(update_sql) %}
{% do run_query(insert_sql) %}
{% do log("scd2_merge_snowflake executed against target=" ~ target_relation, info=True) %}

{% endmacro %}
