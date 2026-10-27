{% materialization table_function, adapter='bigquery' %}

  {#-
    Virtual-environment-aware materialization for BigQuery table functions (TVFs).
    See SPEC 03 (`docs/specs/03_routine_virtualization_and_tvf_testing.md`).

    Config keys:
      parameters (required) — function parameter signature
      returns    (optional) — explicit `TABLE<...>` schema; inferred by BigQuery if omitted
      virtual_env (optional) — when true, registers routine DDL in _environments / _releases
  -#}

  {%- set target_relation = api.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier,
    type='external'
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#- Idempotent transition guard if a PROCEDURE previously had the same name -#}
  {% call statement('drop_existing_procedure') -%}
    BEGIN
      DROP PROCEDURE IF EXISTS {{ target_relation }};
    EXCEPTION WHEN ERROR THEN
      -- existing routine is already a TABLE FUNCTION or absent
    END
  {%- endcall %}

  {% call statement('main') -%}
    CREATE OR REPLACE TABLE FUNCTION {{ target_relation }}({{ config.get('parameters', '') }})
    {%- if config.get('returns') %}
    RETURNS {{ config.get('returns') }}
    {%- endif %}
    AS (
    {{ sql }}
    )
  {%- endcall %}

  {%- if dbt_virtual.is_virtual_node(model) -%}
    {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'table_function', sql) -%}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
