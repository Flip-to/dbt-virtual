{% materialization virtual_function, default %}
  {#-
    Scalar SQL UDFs / DuckDB macros (BigQuery, Snowflake, DuckDB, Postgres).

    Use `materialized='virtual_function'`. dbt-core 1.11+ ships a built-in `function`
    materialization (for the `functions/` resource type), and dbt ignores a package's override
    of a built-in materialization (`require_explicit_package_overrides_for_builtin_materializations`),
    so models with `materialized='function'` run dbt-core's and fail
    ("'NoneType' object has no attribute 'type'", dbt-core 1.12.6).
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


{% materialization function, default %}
  {#-
    Kept for dbt-core < 1.11 and for projects that re-export it from their root project.
    On dbt-core 1.11+ use `virtual_function` (see above).
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
