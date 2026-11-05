{% macro ensure_registry() -%}
  {#- Runs DDL only when the registry is missing or predates `schema_key`. It used to run the
      four-table DDL script for every model, i.e. one extra BigQuery job per model per run. -#}
  {%- if execute -%}
    {%- set cfg = dbt_virtual.get_config() -%}
    {%- set phys_schema = cfg.get('physical_schema') -%}
    {%- set releases_rel = adapter.get_relation(database=target.database, schema=phys_schema, identifier='_releases') -%}
    {%- if releases_rel is none -%}
      {%- call statement('ensure_registry', fetch_result=False) -%}
        {{ dbt_virtual.ensure_registry_tables_sql(phys_schema) }}
      {%- endcall -%}
    {%- elif adapter.get_columns_in_relation is defined -%}
      {%- set column_names = adapter.get_columns_in_relation(releases_rel) | map(attribute='name') | map('lower') | list -%}
      {%- if 'schema_key' not in column_names -%}
        {%- call statement('migrate_registry', fetch_result=False) -%}
          {{ dbt_virtual.registry_migrations_sql(phys_schema) }}
        {%- endcall -%}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}


{% macro seed_incremental_clones(model_names=none) -%}
  {#-
    Seeds new fingerprint tables of virtualized incremental models from `base_environment` ('prod')
    via zero-copy `CLONE`, BEFORE dbt materializes them.

    Run it from `on-run-start` (`"{{ dbt_virtual.seed_incremental_clones() }}"`, scoped to the
    selected resources) or as `dbt run-operation virtual_seed_incremental_clones` (the CLI does
    this for SEED_INCREMENTAL plan actions).

    It cannot be a model pre-hook: dbt's incremental materializations (dbt-core and dbt-bigquery
    1.12) load `existing_relation` before `run_hooks(pre_hooks)`, so a clone made in a pre-hook is
    ignored and then replaced (`create or replace table` on BigQuery, "already exists" elsewhere).
  -#}
  {%- if not execute or flags.FULL_REFRESH -%}
    {{ return('') }}
  {%- endif -%}
  {#- `dbt compile` (and `dbt-virtual plan`) also run on-run-start hooks; only clone for runs that build. -#}
  {%- if model_names is none and flags.WHICH is defined and flags.WHICH not in ['run', 'build', 'retry'] -%}
    {{ return('') }}
  {%- endif -%}
  {%- set selected = none -%}
  {%- if model_names is none and selected_resources is defined -%}
    {%- set selected = selected_resources -%}
  {%- endif -%}

  {%- set candidates = [] -%}
  {%- set nodes_dict = graph.nodes if (graph is defined and graph.nodes is defined) else {} -%}
  {%- for node in nodes_dict.values() -%}
    {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
    {%- set node_name = dbt_virtual.node_attr(node, 'name', '') -%}
    {%- if dbt_virtual.is_virtual_node(node) and dbt_virtual.node_attr(node_cfg, 'materialized', '') == 'incremental' -%}
      {%- if model_names is not none -%}
        {%- if node_name in model_names -%}
          {%- do candidates.append(node) -%}
        {%- endif -%}
      {%- elif selected is none or dbt_virtual.node_attr(node, 'unique_id', '') in selected -%}
        {%- do candidates.append(node) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if (candidates | length) == 0 -%}
    {{ return('') }}
  {%- endif -%}

  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- do dbt_virtual.ensure_registry() -%}

  {%- set base_query -%}
    SELECT model_name, physical_identifier
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ base_env }}'
      AND resource_type = 'model'
  {%- endset -%}
  {%- set base_rows = run_query(base_query) -%}
  {%- set base_map = {} -%}
  {%- if base_rows -%}
    {%- for row in base_rows.rows -%}
      {%- do base_map.update({row[0]: row[1]}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- for node in candidates -%}
    {%- set node_name = dbt_virtual.node_attr(node, 'name', '') -%}
    {%- set node_db = dbt_virtual.node_attr(node, 'database', target.database) -%}
    {%- set target_rel = api.Relation.create(
      database=node_db,
      schema=dbt_virtual.node_attr(node, 'schema', phys_schema),
      identifier=dbt_virtual.node_attr(node, 'alias', node_name),
      type='table'
    ) -%}
    {%- set base_ident = base_map.get(node_name, '') -%}
    {%- set base_table_name = base_ident.split('.')[-1] if base_ident else '' -%}
    {%- if base_table_name != '' and base_table_name != target_rel.identifier
          and adapter.get_relation(database=target_rel.database, schema=target_rel.schema, identifier=target_rel.identifier) is none -%}
      {%- set source_rel = api.Relation.create(database=node_db, schema=phys_schema, identifier=base_table_name, type='table') -%}
      {%- if adapter.get_relation(database=source_rel.database, schema=source_rel.schema, identifier=source_rel.identifier) is not none -%}
        {{ log("dbt-virtual: Seeding " ~ target_rel ~ " via zero-copy CLONE from " ~ source_rel, info=True) }}
        {%- do dbt_virtual.exec_sql_statement(dbt_virtual.clone_table_sql(source_rel, target_rel)) -%}
        {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
        {%- for col_spec in (dbt_virtual.node_attr(node_cfg, 'virtual_add_columns', []) or []) -%}
          {%- do dbt_virtual.exec_sql_statement("ALTER TABLE " ~ target_rel ~ " ADD COLUMN IF NOT EXISTS " ~ col_spec) -%}
        {%- endfor -%}
        {#- dbt has already cached the schema listing; register the clone so the materialization
            and `is_incremental()` see it as an existing table. -#}
        {%- do adapter.cache_added(target_rel) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return('') }}
{%- endmacro %}


{% macro clone_if_new_fp() -%}
  {#-
    Deprecated pre-hook kept so existing `+pre-hook` configs keep working. Seeding moved to
    `seed_incremental_clones()` (on-run-start / CLI), because a clone made here is ignored by the
    incremental materialization. When the table is still missing at this point, dbt does a full
    build, which is correct but not zero-copy; say so.
  -#}
  {%- if not execute or not dbt_virtual.is_virtual_node(model) -%}
    {{ return('') }}
  {%- endif -%}
  {%- set node_cfg = dbt_virtual.node_attr(model, 'config', {}) -%}
  {%- if dbt_virtual.node_attr(node_cfg, 'materialized', '') != 'incremental' or flags.FULL_REFRESH -%}
    {{ return('') }}
  {%- endif -%}
  {%- if adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier) is none -%}
    {{ log("dbt-virtual: " ~ this ~ " was not seeded before the run; dbt will fully build it. Add `{{ dbt_virtual.seed_incremental_clones() }}` to on-run-start or use `dbt-virtual build` to clone from the base environment.", info=True) }}
  {%- endif -%}
  {{ return('') }}
{%- endmacro %}


{% macro is_incremental() -%}
  {#-
    Kept for backward compatibility. Once `seed_incremental_clones()` has cloned the base table
    before the run, dbt's own `is_incremental()` is already True. Returning True for a table that
    does not exist yet made BigQuery's `create or replace table` keep only the incremental delta.
  -#}
  {{ return(builtins.is_incremental()) }}
{%- endmacro %}


{% macro virtual_is_incremental() -%}
  {#- Name used in docs/guides/getting_started.md. -#}
  {{ return(dbt_virtual.is_incremental()) }}
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
  {%- set schema_key = dbt_virtual.node_schema_key(model) -%}
  {%- set env_schema = dbt_virtual.env_schema_for(env_name, schema_key) -%}
  {#- Register the fingerprint that is actually in the physical name. Without CLI-precomputed
      fingerprints, the alias is generated at parse time (`execute` is False, so no parent walk)
      while `get_fingerprint` at run time walks parents; the two differ for any model with parents. -#}
  {%- set fp_prefix = model.name ~ '__' -%}
  {%- if this.identifier.startswith(fp_prefix) -%}
    {%- set fp = this.identifier[(fp_prefix | length):] -%}
  {%- else -%}
    {%- set fp = dbt_virtual.get_fingerprint(model) -%}
  {%- endif -%}
  {%- set release_id = var('_dbt_virtual_release', '') -%}

  {%- do dbt_virtual.ensure_registry() -%}

  {%- set existing_env_rel = adapter.get_relation(database=this.database, schema=env_schema, identifier=model.name) -%}
  {%- if existing_env_rel is none and adapter.check_schema_exists is defined and not adapter.check_schema_exists(this.database, env_schema) -%}
    {#- Unmapped environments (`pr_42` -> `<base_schema>_pr_42`) may not have a schema yet. -#}
    {%- do adapter.create_schema(api.Relation.create(database=this.database, schema=env_schema)) -%}
  {%- endif -%}
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

  {#- Column descriptions are set through the adapter, which needs the view to exist: only then
      create the view now. Otherwise the view DDL stays in the returned hook SQL. -#}
  {%- set create_now = target.type == 'bigquery' and (dbt_virtual.env_view_column_docs(model) | length) > 0 -%}
  {%- if create_now -%}
    {%- if (drop_legacy_tbl_stmt | trim) != '' -%}
      {%- do dbt_virtual.exec_sql_statement(drop_legacy_tbl_stmt) -%}
    {%- endif -%}
    {%- do dbt_virtual.create_env_view(view_rel, this, model, phys_schema, []) -%}
  {%- else -%}
    {%- do dbt_virtual.finalize_env_view(view_rel, model, phys_schema, []) -%}
  {%- endif -%}

  {%- set sql_statements -%}
    {%- if not create_now -%}
    {{ drop_legacy_tbl_stmt }}{{ dbt_virtual.create_or_replace_view_sql(view_rel, this, model) }};
    {%- endif %}

    {{ dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env_name,
      model.name,
      'model',
      fp,
      phys_schema ~ '.' ~ this.identifier,
      release_id,
      schema_key
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
      rolled_back_at,
      schema_key
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
      NULL,
      {{ dbt_virtual.sql_nullable_literal(schema_key) }}
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


{% macro register_routine_deployment(node, target_relation, routine_type, body_sql, ddl=none) -%}
  {#-
    Records a deployed routine in `_environments` (and `_releases` when a release is being built)
    with a DDL template that `virtual_promote` / `virtual_rollback` replay in other environments.

    Public API for projects with their own routine materializations: call it after the CREATE
    statement, passing the executed statement as `ddl` when it differs from what
    `create_routine_ddl_sql` would generate (e.g. a minified body). No-op for non-virtual nodes.
  -#}
  {%- if execute and dbt_virtual.is_virtual_node(node) -%}
    {%- set cfg = dbt_virtual.get_config() -%}
    {%- set phys_schema = cfg.get('physical_schema') -%}
    {%- set env_name = dbt_virtual.get_current_env() -%}
    {%- set schema_key = dbt_virtual.node_schema_key(node) -%}
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

    {%- if ddl is not none and (ddl | trim) != '' -%}
      {%- set full_ddl = ddl | trim -%}
    {%- else -%}
      {%- set full_ddl = dbt_virtual.create_routine_ddl_sql(
        routine_type,
        target_relation | string,
        params_sig,
        returns_sig,
        body_sql
      ) -%}
    {%- endif -%}

    {%- set ddl_template = dbt_virtual.env_placeholders_sql(full_ddl, env_name) -%}

    {%- call statement('register_routine_env', fetch_result=False) -%}
      {{ dbt_virtual.upsert_environment_pointer_sql(
        phys_schema,
        env_name,
        node.name,
        routine_type,
        fp,
        target_relation.schema ~ '.' ~ target_relation.identifier,
        release_id,
        schema_key
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
        rolled_back_at,
        schema_key
      ) VALUES (
        '{{ release_id }}',
        '{{ env_name }}',
        NULL,
        '{{ node.name }}',
        '{{ routine_type }}',
        {{ dep_tier }},
        '{{ fp }}',
        '{{ target_relation.schema }}.{{ target_relation.identifier }}',
        {{ dbt_virtual.sql_string_literal(ddl_template) }},
        'candidate',
        {{ dbt_virtual.current_timestamp_sql() }},
        NULL,
        NULL,
        {{ dbt_virtual.sql_nullable_literal(schema_key) }}
      );
      {%- endif %}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
