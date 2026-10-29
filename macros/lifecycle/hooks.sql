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

  {%- set node_cfg = dbt_virtual.node_attr(model, 'config', {}) -%}
  {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', '') -%}
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
      {%- set clone_sql = dbt_virtual.clone_table_sql(source_rel, this) -%}
      {%- set node_cfg = dbt_virtual.node_attr(model, 'config', {}) -%}
      {%- set add_cols = dbt_virtual.node_attr(node_cfg, 'virtual_add_columns', []) or [] -%}
      {%- set alter_stmts = [clone_sql] -%}
      {%- for col_spec in add_cols -%}
        {%- do alter_stmts.append("ALTER TABLE " ~ this ~ " ADD COLUMN IF NOT EXISTS " ~ col_spec) -%}
      {%- endfor -%}
      {{ return(alter_stmts | join(';\n')) }}
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
    {%- set base_phys_ident = results.rows[0][0] -%}
    {%- set base_table_name = base_phys_ident.split('.')[-1] -%}
    {%- set base_relation = adapter.get_relation(
      database=target.database,
      schema=phys_schema,
      identifier=base_table_name
    ) -%}
    {%- if base_relation is not none -%}
      {{ return(true) }}
    {%- endif -%}
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

  {%- set node_cfg = dbt_virtual.node_attr(model, 'config', {}) -%}
  {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', '') -%}
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

  {%- set existing_env_rel = adapter.get_relation(database=this.database, schema=env_schema, identifier=model.name) -%}
  {%- set drop_legacy_tbl_stmt = '' -%}
  {%- if existing_env_rel is not none and existing_env_rel.type is defined and existing_env_rel.type == 'table' -%}
    {%- set drop_legacy_tbl_stmt = dbt_virtual.drop_object_if_exists_sql(existing_env_rel, 'table') ~ ';\n' -%}
  {%- endif -%}

  {%- set view_rel = api.Relation.create(
    database=this.database,
    schema=env_schema,
    identifier=model.name,
    type='view'
  ) -%}

  {%- set sql_statements -%}
    {{ drop_legacy_tbl_stmt }}{{ dbt_virtual.create_or_replace_view_sql(view_rel, this) }};

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
      {{ dbt_virtual.current_timestamp_sql() }},
      NULL,
      NULL
    );
    {%- endif %}

    {%- set start_ts = var('_dbt_virtual_start', '') -%}
    {%- set end_ts = var('_dbt_virtual_end', '') -%}
    {%- if start_ts != '' and end_ts != '' %}
    INSERT INTO {{ dbt_virtual.registry_relation(phys_schema, '_intervals') }} (
      model_name,
      fingerprint,
      physical_identifier,
      start_ts,
      end_ts,
      recorded_at
    ) VALUES (
      '{{ model.name }}',
      '{{ fp }}',
      '{{ phys_schema }}.{{ this.identifier }}',
      '{{ start_ts }}',
      '{{ end_ts }}',
      {{ dbt_virtual.current_timestamp_sql() }}
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

    {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
    {%- set params_sig = dbt_virtual.node_attr(node_cfg, 'parameters', '') -%}
    {%- set returns_sig = dbt_virtual.node_attr(node_cfg, 'returns', '') -%}

    {%- if routine_type == 'function' -%}
      {%- set dep_tier = 1 -%}
    {%- elif routine_type == 'table_function' -%}
      {%- set dep_tier = 3 -%}
    {%- else -%}
      {%- set dep_tier = 4 -%}
    {%- endif -%}

    {%- set full_ddl = dbt_virtual.create_routine_ddl_sql(
      routine_type,
      target_relation | string,
      params_sig,
      returns_sig,
      body_sql
    ) -%}

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
        {{ dbt_virtual.current_timestamp_sql() }},
        NULL,
        NULL
      );
      {%- endif %}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
