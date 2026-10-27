{% materialization procedure, adapter='bigquery' %}

  {#-
    Virtual-environment-aware materialization for BigQuery stored procedures.
    See SPEC 03 (`docs/specs/03_routine_virtualization_and_tvf_testing.md`).
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
    CREATE OR REPLACE PROCEDURE {{ target_relation }}({{ config.get('parameters', '') }})
    {{ sql }}
  {%- endcall %}

  {%- if dbt_virtual.is_virtual_node(model) -%}
    {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'procedure', sql) -%}
  {%- endif -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
