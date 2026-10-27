{% materialization function, adapter='bigquery' %}

  {#-
    Virtual-environment-aware materialization for BigQuery scalar SQL UDFs.
    See SPEC 03 (`docs/specs/03_routine_virtualization_and_tvf_testing.md`).

    Config keys:
      parameters (required) — function signature
      returns    (required) — scalar return type (e.g. 'ARRAY<STRING>', 'BOOL')
  -#}

  {%- set target_relation = api.Relation.create(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier,
    type='external'
  ) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') -%}
    CREATE OR REPLACE FUNCTION {{ target_relation }}({{ config.get('parameters', '') }})
    RETURNS {{ config.get('returns') }}
    AS (
    {{ sql }}
    )
  {%- endcall %}

  {%- if dbt_virtual.is_virtual_node(model) -%}
    {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'function', sql) -%}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
