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


{% macro on_run_start() -%}
  {#- The package's on-run-start hook: `on-run-start: ["{{ dbt_virtual.on_run_start() }}"]`.
      Checks `require_cli_fingerprints`, then seeds incremental clones for the selected models. -#}
  {%- do dbt_virtual.assert_cli_fingerprints() -%}
  {{ return(dbt_virtual.seed_incremental_clones()) }}
{%- endmacro %}


{% macro assert_cli_fingerprints() -%}
  {#-
    Guard for TECH_DEBT(bare_dbt_run_parse_time_fingerprint): without `_dbt_virtual_fps`, the
    physical name `<model>__<fp>` of a virtual table is generated at parse time and omits upstream
    fingerprints, so a bare `dbt run` after an upstream change rebuilds a downstream physical table
    in place, possibly one another environment's view points at.

    With `vars.dbt_virtual.require_cli_fingerprints: true`, a `run` / `build` / `retry` whose
    selection contains a virtual table or incremental model fails here, in on-run-start, before
    any model is built, unless `_dbt_virtual_fps` is set (`dbt-virtual build` always sets it).
    `compile`, `parse`, `ls`, `docs` and `run-operation` are not affected (`dbt-virtual plan`
    runs `dbt compile` without vars).
  -#}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}
  {%- if flags.WHICH is not defined or flags.WHICH not in ['run', 'build', 'retry'] -%}
    {{ return('') }}
  {%- endif -%}
  {#- The flag may be a rendered string, e.g. `"{{ env_var('DBT_VIRTUAL_STRICT', 'false') }}"`. -#}
  {%- set required = dbt_virtual.get_config().get('require_cli_fingerprints', false) -%}
  {%- if (required | string | trim | lower) not in ['true', '1', 'yes'] -%}
    {{ return('') }}
  {%- endif -%}
  {%- if var('_dbt_virtual_fps', none) -%}
    {{ return('') }}
  {%- endif -%}

  {%- set selected = selected_resources if selected_resources is defined else none -%}
  {%- set unguarded = [] -%}
  {%- set nodes_dict = graph.nodes if (graph is defined and graph.nodes is defined) else {} -%}
  {%- for node in nodes_dict.values() -%}
    {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
    {%- if dbt_virtual.node_attr(node_cfg, 'materialized', '') in ['table', 'incremental']
          and (selected is none or dbt_virtual.node_attr(node, 'unique_id', '') in selected)
          and dbt_virtual.is_virtual_node(node) -%}
      {%- do unguarded.append(dbt_virtual.node_attr(node, 'name', '')) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if (unguarded | length) == 0 -%}
    {{ return('') }}
  {%- endif -%}

  {%- set names = unguarded | sort -%}
  {%- set shown = (names[:5] | join(', ')) ~ (', ...' if (names | length) > 5 else '') -%}
  {{ exceptions.raise_compiler_error(
    "dbt-virtual: require_cli_fingerprints is true but var('_dbt_virtual_fps') is not set, so "
    ~ (names | length) ~ " selected virtual table model(s) (" ~ shown ~ ") would get physical "
    ~ "names without upstream fingerprints and could rebuild a table another environment points "
    ~ "at. Run `dbt-virtual build` (it passes _dbt_virtual_fps), or set "
    ~ "vars.dbt_virtual.require_cli_fingerprints to false."
  ) }}
{%- endmacro %}


{% macro seed_incremental_clones(model_names=none) -%}
  {#-
    Seeds new fingerprint tables of virtualized incremental models from `base_environment` ('prod')
    via zero-copy `CLONE`, BEFORE dbt materializes them.

    Run it from `on-run-start` (`"{{ dbt_virtual.on_run_start() }}"` or
    `"{{ dbt_virtual.seed_incremental_clones() }}"`, scoped to the selected resources) or as
    `dbt run-operation virtual_seed_incremental_clones` (the CLI does this for SEED_INCREMENTAL
    plan actions).

    It cannot be a model pre-hook: dbt's incremental materializations (dbt-core and dbt-bigquery
    1.12) load `existing_relation` before `run_hooks(pre_hooks)`, so a clone made in a pre-hook is
    ignored and then replaced (`create or replace table` on BigQuery, "already exists" elsewhere).
  -#}
  {#- As an on-run-start hook (no `model_names`), also enforce `require_cli_fingerprints`, so
      projects configured with this hook before `on_run_start()` existed are protected too. The
      CLI's run-operation passes `model_names` and its own `_dbt_virtual_fps`. -#}
  {%- if model_names is none -%}
    {%- do dbt_virtual.assert_cli_fingerprints() -%}
  {%- endif -%}
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

  {#- Physical tables some environment or release points at, i.e. written by a build that finished. -#}
  {%- set registered_query -%}
    SELECT DISTINCT physical_identifier FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }} WHERE resource_type = 'model'
    {{ 'UNION DISTINCT' if target.type == 'bigquery' else 'UNION' }}
    SELECT DISTINCT physical_identifier FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} WHERE resource_type = 'model'
  {%- endset -%}
  {%- set registered_rows = run_query(registered_query) -%}
  {%- set registered = [] -%}
  {%- if registered_rows -%}
    {%- for row in registered_rows.rows -%}
      {%- do registered.append(row[0].split('.')[-1]) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- for node in candidates -%}
    {%- set node_name = dbt_virtual.node_attr(node, 'name', '') -%}
    {%- set node_db = dbt_virtual.node_attr(node, 'database', target.database) -%}
    {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
    {%- set target_rel = api.Relation.create(
      database=node_db,
      schema=dbt_virtual.node_attr(node, 'schema', phys_schema),
      identifier=dbt_virtual.node_attr(node, 'alias', node_name),
      type='table'
    ) -%}
    {%- set existing_target = adapter.get_relation(database=target_rel.database, schema=target_rel.schema, identifier=target_rel.identifier) -%}
    {%- if existing_target is not none and target_rel.identifier not in registered -%}
      {#- A table nobody points at is a leftover of a run that failed after seeding (only a
          finished build registers its table): it holds base data without this version's delta. -#}
      {{ log("dbt-virtual: " ~ target_rel ~ " exists but was never registered (a previous run failed after seeding); re-seeding it.", info=True) }}
      {%- do dbt_virtual.exec_sql_statement(dbt_virtual.drop_object_if_exists_sql(existing_target, 'table')) -%}
      {%- if adapter.cache_dropped is defined -%}
        {%- do adapter.cache_dropped(existing_target) -%}
      {%- endif -%}
      {%- set existing_target = none -%}
    {%- endif -%}
    {%- set base_ident = base_map.get(node_name, '') -%}
    {%- set base_table_name = base_ident.split('.')[-1] if base_ident else '' -%}
    {%- if base_table_name != '' and base_table_name != target_rel.identifier and existing_target is none -%}
      {%- set source_rel = api.Relation.create(database=node_db, schema=phys_schema, identifier=base_table_name, type='table') -%}
      {%- set shape_matches = true -%}
      {%- if adapter.is_replaceable is defined and adapter.parse_partition_by is defined -%}
        {#- A CLONE keeps the base table's partitioning and clustering; when the model changed
            them, clone + incremental would leave the old layout under the new fingerprint. -#}
        {%- set shape_matches = adapter.is_replaceable(
          source_rel,
          adapter.parse_partition_by(dbt_virtual.node_attr(node_cfg, 'partition_by', none)),
          dbt_virtual.node_attr(node_cfg, 'cluster_by', none)
        ) -%}
      {%- endif -%}
      {%- if not shape_matches -%}
        {{ log("dbt-virtual: not seeding " ~ target_rel ~ " from " ~ source_rel ~ ": partitioning or clustering changed, dbt will build it in full.", info=True) }}
      {%- elif adapter.get_relation(database=source_rel.database, schema=source_rel.schema, identifier=source_rel.identifier) is not none -%}
        {{ log("dbt-virtual: Seeding " ~ target_rel ~ " via zero-copy CLONE from " ~ source_rel, info=True) }}
        {%- do dbt_virtual.exec_sql_statement(dbt_virtual.clone_table_sql(source_rel, target_rel)) -%}
        {%- for col_spec in (dbt_virtual.node_attr(node_cfg, 'virtual_add_columns', []) or []) -%}
          {%- do dbt_virtual.exec_sql_statement("ALTER TABLE " ~ target_rel ~ " ADD COLUMN IF NOT EXISTS " ~ col_spec) -%}
        {%- endfor -%}
        {%- if target.type == 'bigquery' -%}
          {#- Table options the model sets are not carried over from the base table by CLONE. -#}
          {%- set table_options = [] -%}
          {%- if dbt_virtual.node_attr(node_cfg, 'require_partition_filter', none) is not none -%}
            {%- do table_options.append('require_partition_filter=' ~ (dbt_virtual.node_attr(node_cfg, 'require_partition_filter', false) | string | lower)) -%}
          {%- endif -%}
          {%- if dbt_virtual.node_attr(node_cfg, 'partition_expiration_days', none) is not none -%}
            {%- do table_options.append('partition_expiration_days=' ~ dbt_virtual.node_attr(node_cfg, 'partition_expiration_days', none)) -%}
          {%- endif -%}
          {%- if (table_options | length) > 0 -%}
            {%- do dbt_virtual.exec_sql_statement("ALTER TABLE " ~ target_rel ~ " SET OPTIONS (" ~ (table_options | join(', ')) ~ ")") -%}
          {%- endif -%}
        {%- endif -%}
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
  {#- `builtins` has no `is_incremental` (it raised "'dict object' has no attribute 'is_incremental'"). -#}
  {{ return(dbt.is_incremental()) }}
{%- endmacro %}


{% macro virtual_is_incremental() -%}
  {#- Name used in docs/guides/getting_started.md. -#}
  {{ return(dbt_virtual.is_incremental()) }}
{%- endmacro %}


{% macro sync_env_view() -%}
  {#-
    Post-hook for virtualized table/incremental models:
    For virtualized `view` models, only registers the view (see `register_view_deployment`).
    1. Creates/updates the virtual view `<env_schema>.<model_name>` -> `SELECT * FROM <this>`.
    2. Upserts the environment pointer in `<physical_schema>._environments`.
    3. Records the release entry in `<physical_schema>._releases` when `_dbt_virtual_release` is set.
  -#}
  {%- if not execute or not dbt_virtual.is_virtual_node(model) -%}
    {{ return('') }}
  {%- endif -%}

  {%- set node_cfg = dbt_virtual.node_attr(model, 'config', {}) -%}
  {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', '') -%}
  {%- if mat == 'view' -%}
    {#- A virtual view is built in the environment schema by dbt; record it so promote, rollback
        and init-env can create it in other environments. -#}
    {{ return(dbt_virtual.register_view_deployment(model, this)) }}
  {%- endif -%}
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

  {%- set sql_statements -%}
    {{ drop_legacy_tbl_stmt }}{{ dbt_virtual.create_or_replace_view_sql(view_rel, this) }};

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
    {%- if graph is defined and graph.nodes is defined -%}
      {%- for parent_uid in (dbt_virtual.node_attr(dbt_virtual.node_attr(node, 'depends_on', {}), 'nodes', []) or []) -%}
        {%- set parent = graph.nodes.get(parent_uid) -%}
        {%- if parent is not none and dbt_virtual.node_attr(parent, 'resource_type', '') == 'model' and not dbt_virtual.is_virtual_node(parent) -%}
          {{ log("dbt-virtual WARNING: routine " ~ node.name ~ " reads non-virtual model " ~ dbt_virtual.node_attr(parent, 'name', parent_uid) ~ "; after promotion it still reads the copy built in env " ~ env_name ~ ". Virtualize its dependencies.", info=True) }}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}

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


{% macro register_view_deployment(node, target_relation, body_sql=none) -%}
  {#-
    Returns the SQL (for a post-hook) that records a virtual `view` model in `_environments` (and
    `_releases` when a release is being built) with a `CREATE OR REPLACE VIEW` template, which
    `virtual_promote` / `virtual_rollback` / `virtual_init_env` replay in other environments.
    References to virtual objects in the template are per-object placeholders, resolved per target
    environment (see `env_placeholders_sql`). `resource_type` is `view`.
  -#}
  {%- if not execute or not dbt_virtual.is_virtual_node(node) -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set env_name = dbt_virtual.get_current_env() -%}
  {%- set schema_key = dbt_virtual.node_schema_key(node) -%}
  {%- set fp = dbt_virtual.get_fingerprint(node) -%}
  {%- set release_id = var('_dbt_virtual_release', '') -%}
  {%- do dbt_virtual.ensure_registry() -%}

  {%- set body = body_sql if body_sql is not none else (dbt_virtual.node_attr(node, 'compiled_code', '') or dbt_virtual.node_attr(node, 'compiled_sql', '')) -%}
  {%- set body = (body | string | trim).rstrip(';') -%}
  {%- if body == '' -%}
    {{ exceptions.raise_compiler_error("dbt-virtual: no compiled SQL available to register view '" ~ dbt_virtual.node_attr(node, 'name', '') ~ "'.") }}
  {%- endif -%}
  {%- set ddl = 'CREATE OR REPLACE VIEW ' ~ target_relation ~ ' AS\n' ~ body -%}
  {%- set ddl_template = dbt_virtual.env_placeholders_sql(ddl, env_name) -%}
  {%- set phys_ident = target_relation.schema ~ '.' ~ target_relation.identifier -%}

  {%- set sql_statements -%}
    {{ dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env_name,
      node.name,
      'view',
      fp,
      phys_ident,
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
      'view',
      2,
      '{{ fp }}',
      '{{ phys_ident }}',
      {{ dbt_virtual.sql_string_literal(ddl_template) }},
      'candidate',
      {{ dbt_virtual.current_timestamp_sql() }},
      NULL,
      NULL,
      {{ dbt_virtual.sql_nullable_literal(schema_key) }}
    );
    {%- endif %}
  {%- endset -%}
  {{ return(sql_statements) }}
{%- endmacro %}
