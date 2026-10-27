{% macro ensure_registry() -%}
  {%- if execute -%}
    {%- set cfg = dbt_virtual.get_config() -%}
    {%- set phys_schema = cfg.get('physical_schema') -%}
    {%- call statement('ensure_registry', fetch_result=False) -%}
      {{ dbt_virtual.ensure_registry_tables_sql(phys_schema) }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}


{% macro clone_if_new_fp() -%}
  {#-
    Pre-hook for virtualized incremental models:
    If the fingerprinted physical table `<physical_schema>.<model>__<fp>` does not exist yet
    and `--full-refresh` is not active, seeds it via zero-copy `CLONE` from `base_environment` ('prod').
  -#}
  {%- if not execute or not dbt_virtual.is_virtual_node(model) -%}
    {{ return('') }}
  {%- endif -%}

  {%- set mat = model.get('config', {}).get('materialized', '') -%}
  {%- if mat != 'incremental' or flags.FULL_REFRESH -%}
    {{ return('') }}
  {%- endif -%}

  {%- set existing_target = adapter.get_relation(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier
  ) -%}
  {%- if existing_target is not none -%}
    {#- Fingerprint table already exists; normal incremental run in place -#}
    {{ return('') }}
  {%- endif -%}

  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}

  {%- do dbt_virtual.ensure_registry() -%}

  {%- set lookup_query -%}
    SELECT physical_identifier
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ base_env }}'
      AND model_name = '{{ model.name }}'
      AND resource_type = 'model'
    LIMIT 1
  {%- endset -%}

  {%- set results = run_query(lookup_query) -%}
  {%- if results and (results.rows | length) > 0 -%}
    {%- set base_phys_ident = results.rows[0][0] -%}
    {%- set base_table_name = base_phys_ident.split('.')[-1] -%}
    {%- set source_rel = api.Relation.create(
      database=this.database,
      schema=phys_schema,
      identifier=base_table_name,
      type='table'
    ) -%}
    {%- set source_exists = adapter.get_relation(
      database=source_rel.database,
      schema=source_rel.schema,
      identifier=source_rel.identifier
    ) -%}
    {%- if source_exists is not none -%}
      {{ log("dbt-virtual: Seeding " ~ this ~ " via zero-copy CLONE from " ~ source_rel, info=True) }}
      {#- Prime dbt's relation cache so subsequent is_incremental() checks during model execution return True -#}
      {%- do adapter.cache_added(this) -%}
      {{ return(dbt_virtual.clone_table_sql(source_rel, this)) }}
    {%- endif -%}
  {%- endif -%}

  {{ return('') }}
{%- endmacro %}


{% macro is_incremental() -%}
  {#-
    Virtual-environment-aware `is_incremental()` helper:
    Returns True if standard `builtins.is_incremental()` is True OR if `clone_if_new_fp` will seed
    `<this>` from `base_environment` (`prod`) in its pre-hook.
  -#}
  {%- if builtins.is_incremental() -%}
    {{ return(true) }}
  {%- endif -%}
  {%- if not execute or not dbt_virtual.is_virtual_node(model) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set mat = model.get('config', {}).get('materialized', '') -%}
  {%- if mat != 'incremental' or flags.FULL_REFRESH -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- set lookup_query -%}
    SELECT physical_identifier
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ base_env }}'
      AND model_name = '{{ model.name }}'
      AND resource_type = 'model'
    LIMIT 1
  {%- endset -%}
  {%- set results = run_query(lookup_query) -%}
  {%- if results and (results.rows | length) > 0 -%}
    {{ return(true) }}
  {%- endif -%}
  {{ return(false) }}
{%- endmacro %}


{% macro sync_env_view() -%}
  {#-
    Post-hook for virtualized table/incremental models:
    1. Creates/updates the virtual view `<env_schema>.<model_name>` -> `SELECT * FROM <this>`.
    2. Upserts the environment pointer in `<physical_schema>._environments`.
    3. Records the release entry in `<physical_schema>._releases` when `_dbt_virtual_release` is set.
  -#}
  {%- if not execute or not dbt_virtual.is_virtual_node(model) -%}
    {{ return('') }}
  {%- endif -%}

  {%- set mat = model.get('config', {}).get('materialized', '') -%}
  {%- if mat not in ['table', 'incremental'] -%}
    {{ return('') }}
  {%- endif -%}

  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set env_name = dbt_virtual.get_current_env() -%}
  {%- set env_schema = dbt_virtual.get_env_schema(env_name) -%}
  {%- set fp = dbt_virtual.get_fingerprint(model) -%}
  {%- set release_id = var('_dbt_virtual_release', '') -%}

  {%- do dbt_virtual.ensure_registry() -%}

  {%- set view_rel = api.Relation.create(
    database=this.database,
    schema=env_schema,
    identifier=model.name,
    type='view'
  ) -%}

  {%- set sql_statements -%}
    {{ dbt_virtual.create_or_replace_view_sql(view_rel, this) }};

    {{ dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env_name,
      model.name,
      'model',
      fp,
      phys_schema ~ '.' ~ this.identifier,
      release_id
    ) }};

    {%- if release_id != '' %}
    INSERT INTO {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} (
      release_id,
      source_environment,
      target_environment,
      model_name,
      resource_type,
      dependency_tier,
      fingerprint,
      physical_identifier,
      routine_ddl_template,
      status,
      created_at,
      promoted_at,
      rolled_back_at
    ) VALUES (
      '{{ release_id }}',
      '{{ env_name }}',
      NULL,
      '{{ model.name }}',
      'model',
      2,
      '{{ fp }}',
      '{{ phys_schema }}.{{ this.identifier }}',
      NULL,
      'candidate',
      CURRENT_TIMESTAMP(),
      NULL,
      NULL
    );
    {%- endif %}
  {%- endset -%}

  {{ return(sql_statements) }}
{%- endmacro %}


{% macro register_routine_deployment(node, target_relation, routine_type, body_sql) -%}
  {%- if execute -%}
    {%- set cfg = dbt_virtual.get_config() -%}
    {%- set phys_schema = cfg.get('physical_schema') -%}
    {%- set env_name = dbt_virtual.get_current_env() -%}
    {%- set env_schema = dbt_virtual.get_env_schema(env_name) -%}
    {%- set fp = dbt_virtual.get_fingerprint(node) -%}
    {%- set release_id = var('_dbt_virtual_release', '') -%}
    {%- do dbt_virtual.ensure_registry() -%}

    {%- set node_cfg = node.get('config', {}) -%}
    {%- set params_sig = node_cfg.get('parameters', '') -%}
    {%- set returns_sig = node_cfg.get('returns', '') -%}

    {%- if routine_type == 'function' -%}
      {%- set dep_tier = 1 -%}
      {%- set full_ddl -%}
CREATE OR REPLACE FUNCTION `{{ target_relation.database }}`.`{{ env_schema }}`.`{{ target_relation.identifier }}`({{ params_sig }})
RETURNS {{ returns_sig }}
AS (
{{ body_sql }}
)
      {%- endset -%}
    {%- elif routine_type == 'table_function' -%}
      {%- set dep_tier = 3 -%}
      {%- set full_ddl -%}
CREATE OR REPLACE TABLE FUNCTION `{{ target_relation.database }}`.`{{ env_schema }}`.`{{ target_relation.identifier }}`({{ params_sig }})
{%- if returns_sig %}
RETURNS {{ returns_sig }}
{%- endif %}
AS (
{{ body_sql }}
)
      {%- endset -%}
    {%- else -%}
      {%- set dep_tier = 4 -%}
      {%- set full_ddl -%}
CREATE OR REPLACE PROCEDURE `{{ target_relation.database }}`.`{{ env_schema }}`.`{{ target_relation.identifier }}`({{ params_sig }})
{{ body_sql }}
      {%- endset -%}
    {%- endif -%}

    {%- set ddl_template = full_ddl | replace(env_schema, '__DBT_VIRTUAL_ENV_SCHEMA__') -%}

    {%- call statement('register_routine_env', fetch_result=False) -%}
      {{ dbt_virtual.upsert_environment_pointer_sql(
        phys_schema,
        env_name,
        node.name,
        routine_type,
        fp,
        target_relation.schema ~ '.' ~ target_relation.identifier,
        release_id
      ) }};

      {%- if release_id != '' %}
      INSERT INTO {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} (
        release_id,
        source_environment,
        target_environment,
        model_name,
        resource_type,
        dependency_tier,
        fingerprint,
        physical_identifier,
        routine_ddl_template,
        status,
        created_at,
        promoted_at,
        rolled_back_at
      ) VALUES (
        '{{ release_id }}',
        '{{ env_name }}',
        NULL,
        '{{ node.name }}',
        '{{ routine_type }}',
        {{ dep_tier }},
        '{{ fp }}',
        '{{ target_relation.schema }}.{{ target_relation.identifier }}',
        {{ dbt.string_literal(ddl_template) }},
        'candidate',
        CURRENT_TIMESTAMP(),
        NULL,
        NULL
      );
      {%- endif %}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
