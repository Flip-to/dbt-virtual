{% materialization function, default %}
  {#-
    Cross-adapter (BigQuery, Snowflake, DuckDB, Postgres) materialization for scalar SQL UDFs / macros.
  -#}
  {%- set target_relation = api.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier,
    type='view'
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    {{ dbt_virtual.create_routine_ddl_sql(
      'function',
      target_relation | string,
      config.get('parameters', ''),
      config.get('returns', ''),
      sql
    ) }}
  {%- endcall %}

  {%- if dbt_virtual.is_virtual_node(model) -%}
    {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'function', sql) -%}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
