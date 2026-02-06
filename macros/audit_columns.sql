{% macro add_audit_columns(created_col, updated_col) -%}
  {{ created_col }} as src_created_at,
  {{ updated_col }} as src_updated_at,
  current_timestamp() as dbt_loaded_at
{%- endmacro %}
