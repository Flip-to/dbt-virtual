{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {{ return(target.schema) }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
{%- endmacro %}

{% macro ref() -%}
  {{ return(dbt_virtual.virtual_ref(*varargs, **kwargs)) }}
{%- endmacro %}
