{% materialization table_function, adapter='bigquery' %}
  {#-
    Virtual-environment-aware materialization for BigQuery table functions (TVFs).
    See `docs/specs/20261027_routine_virtualization_and_tvf_testing.md`.
  -#}
  {%- set target_relation = api.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier,
    type='external'
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('drop_existing_procedure') -%}
    BEGIN
      DROP PROCEDURE IF EXISTS {{ target_relation }};
    EXCEPTION WHEN ERROR THEN
      -- existing routine is already a TABLE FUNCTION or absent
    END
  {%- endcall %}

  {% call statement('main') -%}
    {{ dbt_virtual.create_routine_ddl_sql(
      'table_function',
      target_relation | string,
      config.get('parameters', ''),
      config.get('returns', ''),
      sql
    ) }}
  {%- endcall %}

  {%- if dbt_virtual.is_virtual_node(model) -%}
    {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'table_function', sql) -%}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{% materialization table_function, default %}
  {#-
    Cross-adapter (Snowflake, DuckDB, Postgres) materialization for table functions / table macros.
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
      'table_function',
      target_relation | string,
      config.get('parameters', ''),
      config.get('returns', ''),
      sql
    ) }}
  {%- endcall %}

  {%- if dbt_virtual.is_virtual_node(model) -%}
    {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'table_function', sql) -%}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
