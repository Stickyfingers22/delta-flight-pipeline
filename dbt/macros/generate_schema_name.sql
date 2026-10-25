{#
  By default dbt names custom schemas "<target>_<custom>" (e.g. main_staging).
  Use the custom name as-is so tables live in clean schemas: staging, marts, reference.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
