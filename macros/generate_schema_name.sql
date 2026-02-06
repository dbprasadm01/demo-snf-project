{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if custom_schema_name is not none -%}
    {% set name = custom_schema_name %}
  {%- else -%}
    {% set name = target.schema %}
  {%- endif -%}

  {# If name looks fully-qualified (contains '.'), return the last segment only #}
  {% if '.' in name -%}
    {{ return(name.split('.')[-1]) }}
  {% else -%}
    {{ return(name) }}
  {% endif -%}
{%- endmacro %}
