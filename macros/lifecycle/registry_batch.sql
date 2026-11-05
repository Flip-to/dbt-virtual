{#-
  DECISION(batched_registry_writes): registry rows are written once per run, in `on-run-end`.

  Per-model post-hooks used to run a `MERGE` into `_environments` and `INSERT`s into `_releases` /
  `_intervals` for every model. With `threads: 8-16` those mutating statements hit one unpartitioned
  BigQuery table concurrently: BigQuery runs at most 2 mutating DML statements per table at once,
  queues the rest and fails conflicting ones after retries ("Could not serialize access to table ...
  due to concurrent update"), failing a model whose table was already built. It also cost one extra
  job per model.

  In `registry_write_mode: batch` (default) the post-hook (`sync_env_view`) only creates/replaces the
  environment view, which must stay immediate because downstream models read it. The rows are NOT
  kept in a shared in-run buffer (dbt gives every node its own Jinja context and threads share no
  mutable state). They are recomputed in `on-run-end` from `results` (which nodes succeeded),
  the node objects in `results` (alias = `<name>__<fingerprint>`, schema, config, compiled code) and
  vars, i.e. from data dbt already hands the hook. A failed node is not in the "success" set, so its
  pointer is not written.
-#}

{% macro registry_write_mode() -%}
  {#- `batch` (default) or `per_model` (`vars.dbt_virtual.registry_write_mode`). -#}
  {%- set mode = (dbt_virtual.get_config().get('registry_write_mode', 'batch') | string | trim | lower) -%}
  {%- if mode not in ['batch', 'per_model'] -%}
    {{ exceptions.raise_compiler_error("dbt-virtual: registry_write_mode must be 'batch' or 'per_model', got '" ~ mode ~ "'.") }}
  {%- endif -%}
  {{ return(mode) }}
{%- endmacro %}


{% macro flush_hook_defined() -%}
  {#- True when some project (root or package) defines an on-run-end hook calling `flush_registry`. -#}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- if (dbt_virtual.node_attr(node, 'resource_type', '') | string) == 'operation' -%}
        {%- set code = dbt_virtual.node_attr(node, 'raw_code', '') or dbt_virtual.node_attr(node, 'raw_sql', '') or '' -%}
        {%- if 'flush_registry' in code -%}
          {{ return(true) }}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(false) }}
{%- endmacro %}


{% macro use_batched_registry() -%}
  {#-
    Whether post-hooks should leave registry writes to `flush_registry`. Batch mode without the
    on-run-end hook would silently lose every pointer, so it falls back to per-model writes and says so.
  -#}
  {%- if dbt_virtual.registry_write_mode() != 'batch' -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if dbt_virtual.flush_hook_defined() -%}
    {{ return(true) }}
  {%- endif -%}
  {{ log("dbt-virtual WARNING: registry_write_mode is 'batch' but no on-run-end hook calls dbt_virtual.flush_registry(results); writing registry rows per model (slow, and concurrent DML on one BigQuery table can fail). Add `on-run-end: \"{{ dbt_virtual.flush_registry(results) }}\"` to dbt_project.yml or set registry_write_mode: per_model.", info=True) }}
  {{ return(false) }}
{%- endmacro %}


{% macro release_insert_sql(physical_schema, values) -%}
  {#- One multi-row INSERT into `_releases`; `values` are rendered `(...)` row tuples. -#}
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} (
    release_id, source_environment, target_environment, model_name, resource_type, dependency_tier,
    fingerprint, physical_identifier, routine_ddl_template, status, created_at, promoted_at, rolled_back_at,
    schema_key, build_quality
  ) VALUES
  {{ values | join(',\n') }}
{%- endmacro %}


{% macro collect_registry_rows(results, env_name, phys_schema) -%}
  {#-
    Registry rows for the virtual nodes in `results` that were built successfully:
    a list of dicts (`name`, `resource_type`, `fingerprint`, `physical_identifier`, `schema_key`,
    `tier`, `routine_ddl_template`).
  -#}
  {%- set rows = [] -%}
  {%- set routine_types = {'table_function': 'table_function', 'function': 'function', 'virtual_function': 'function', 'procedure': 'procedure'} -%}
  {%- set tiers = {'function': 1, 'table_function': 3, 'procedure': 4} -%}
  {%- for r in results -%}
    {%- set node = r.node -%}
    {%- set status = (r.status | string | lower) -%}
    {%- if (status == 'success' or status.endswith('.success'))
          and (dbt_virtual.node_attr(node, 'resource_type', '') | string) == 'model'
          and dbt_virtual.is_virtual_node(node) -%}
      {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
      {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', '') | string -%}
      {%- set name = dbt_virtual.node_attr(node, 'name', '') -%}
      {%- set alias = dbt_virtual.node_attr(node, 'alias', name) or name -%}
      {%- set schema_key = dbt_virtual.node_schema_key(node) -%}
      {#- Each location has its own registry and physical schema (see macros/routing/locations.sql). -#}
      {%- set node_loc = dbt_virtual.node_location(node) -%}
      {%- set node_phys = dbt_virtual.physical_schema_for(node_loc) -%}
      {%- if mat in ['table', 'incremental'] -%}
        {#- Same rule as `sync_env_view`: with CLI/env fingerprints, the one in the physical name; on a bare run, the run-time Merkle fingerprint. -#}
        {%- set fp_prefix = name ~ '__' -%}
        {%- if alias.startswith(fp_prefix) and name in dbt_virtual.get_precomputed_fps() -%}
          {%- set fp = alias[(fp_prefix | length):] -%}
        {%- else -%}
          {%- set fp = dbt_virtual.runtime_fingerprint(node) -%}
        {%- endif -%}
        {%- do rows.append({
          'name': name, 'resource_type': 'model', 'fingerprint': fp,
          'physical_identifier': node_phys ~ '.' ~ alias, 'schema_key': schema_key,
          'tier': 2, 'routine_ddl_template': none, 'location': node_loc
        }) -%}
      {%- elif mat in routine_types -%}
        {%- set routine_type = routine_types[mat] -%}
        {%- set target_rel = api.Relation.create(
          database=dbt_virtual.node_attr(node, 'database', target.database),
          schema=dbt_virtual.node_attr(node, 'schema', phys_schema),
          identifier=alias,
          type='view'
        ) -%}
        {%- set body = dbt_virtual.node_attr(node, 'compiled_code', '') or dbt_virtual.node_attr(node, 'compiled_sql', '') or '' -%}
        {%- set full_ddl = dbt_virtual.create_routine_ddl_sql(
          routine_type,
          target_rel | string,
          dbt_virtual.node_attr(node_cfg, 'parameters', ''),
          dbt_virtual.node_attr(node_cfg, 'returns', ''),
          body
        ) -%}
        {%- do rows.append({
          'name': name, 'resource_type': routine_type, 'fingerprint': dbt_virtual.get_fingerprint(node),
          'physical_identifier': target_rel.schema ~ '.' ~ target_rel.identifier, 'schema_key': schema_key,
          'tier': tiers[routine_type],
          'routine_ddl_template': dbt_virtual.env_placeholders_sql(full_ddl, env_name),
          'location': node_loc
        }) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(rows) }}
{%- endmacro %}


{% macro write_registry_rows(rows, env_name, release_id, start_ts='', end_ts='', chunk_size=200) -%}
  {#- Writes `rows` (see `collect_registry_rows`) into the registry of each row's location. -#}
  {%- set by_location = {} -%}
  {%- for row in rows -%}
    {%- set loc_key = row.get('location') or '' -%}
    {%- if loc_key not in by_location -%}
      {%- do by_location.update({loc_key: []}) -%}
    {%- endif -%}
    {%- do by_location[loc_key].append(row) -%}
  {%- endfor -%}
  {%- for loc_key, loc_rows in by_location.items() -%}
    {%- do dbt_virtual.write_location_registry_rows(loc_rows, (loc_key or none), env_name, release_id, start_ts, end_ts, chunk_size) -%}
  {%- endfor -%}
{%- endmacro %}


{% macro write_location_registry_rows(rows, location, env_name, release_id, start_ts='', end_ts='', chunk_size=200) -%}
  {#- Per chunk: one MERGE (or DELETE+INSERT) into `_environments`, one INSERT into `_releases`
      and one into `_intervals`, all in `location`'s registry. -#}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {%- do dbt_virtual.ensure_registry(location) -%}
  {%- for chunk in rows | batch(chunk_size) -%}
    {%- set env_rows = [] -%}
    {%- for row in chunk -%}
      {%- do env_rows.append({
        'name': row['name'], 'resource_type': row['resource_type'], 'fingerprint': row['fingerprint'],
        'physical_identifier': row['physical_identifier'], 'release_id': release_id, 'schema_key': row['schema_key']
      }) -%}
    {%- endfor -%}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointers_sql(phys_schema, env_name, env_rows)) -%}
    {#- Promotion guards read `build_quality` ('partial' for windowed / --empty builds); stamp the
        whole chunk at once, as the per-model path does for one row. -#}
    {%- set quality = dbt_virtual.build_quality() -%}
    {%- set chunk_names = [] -%}
    {%- for row in chunk -%}
      {%- do chunk_names.append(dbt_virtual.sql_string_literal(row['name'])) -%}
    {%- endfor -%}
    {%- do dbt_virtual.exec_sql_statement(
      "UPDATE " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
      ~ " SET build_quality = '" ~ quality ~ "' WHERE environment = " ~ dbt_virtual.sql_string_literal(env_name)
      ~ " AND model_name IN (" ~ (chunk_names | join(', ')) ~ ")"
    ) -%}

    {%- if release_id != '' -%}
      {%- set values = [] -%}
      {%- for row in chunk -%}
        {%- do values.append(
          "(" ~ dbt_virtual.sql_string_literal(release_id) ~ ", " ~ dbt_virtual.sql_string_literal(env_name)
          ~ ", NULL, " ~ dbt_virtual.sql_string_literal(row['name']) ~ ", " ~ dbt_virtual.sql_string_literal(row['resource_type'])
          ~ ", " ~ row['tier'] ~ ", " ~ dbt_virtual.sql_string_literal(row['fingerprint'])
          ~ ", " ~ dbt_virtual.sql_string_literal(row['physical_identifier']) ~ ", "
          ~ (dbt_virtual.sql_string_literal(row['routine_ddl_template']) if row['routine_ddl_template'] is not none else 'NULL')
          ~ ", 'candidate', " ~ dbt_virtual.current_timestamp_sql() ~ ", NULL, NULL, "
          ~ dbt_virtual.sql_nullable_literal(row['schema_key']) ~ ", '" ~ dbt_virtual.build_quality() ~ "')"
        ) -%}
      {%- endfor -%}
      {#- Idempotent: a re-run (or a project that also lists `flush_registry` in its own on-run-end)
          replaces this release's still-`candidate` rows for these objects instead of duplicating them. -#}
      {%- set names = [] -%}
      {%- for row in chunk -%}
        {%- do names.append(dbt_virtual.sql_string_literal(row['name'])) -%}
      {%- endfor -%}
      {%- do dbt_virtual.exec_atomic([
        "DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_releases')
        ~ " WHERE release_id = " ~ dbt_virtual.sql_string_literal(release_id)
        ~ " AND source_environment = " ~ dbt_virtual.sql_string_literal(env_name)
        ~ " AND status = 'candidate' AND model_name IN (" ~ (names | join(', ')) ~ ")",
        dbt_virtual.release_insert_sql(phys_schema, values)
      ]) -%}
    {%- endif -%}

    {%- if start_ts != '' and end_ts != '' -%}
      {%- set interval_values = [] -%}
      {%- for row in chunk -%}
        {%- if row['resource_type'] == 'model' -%}
          {%- do interval_values.append(
            "(" ~ dbt_virtual.sql_string_literal(row['name']) ~ ", " ~ dbt_virtual.sql_string_literal(row['fingerprint'])
            ~ ", " ~ dbt_virtual.sql_string_literal(row['physical_identifier']) ~ ", " ~ dbt_virtual.sql_string_literal(start_ts)
            ~ ", " ~ dbt_virtual.sql_string_literal(end_ts) ~ ", " ~ dbt_virtual.current_timestamp_sql() ~ ")"
          ) -%}
        {%- endif -%}
      {%- endfor -%}
      {%- if (interval_values | length) > 0 -%}
        {%- do dbt_virtual.exec_sql_statement(
          "INSERT INTO " ~ dbt_virtual.registry_relation(phys_schema, '_intervals')
          ~ " (model_name, fingerprint, physical_identifier, start_ts, end_ts, recorded_at) VALUES\n"
          ~ (interval_values | join(',\n'))
        ) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro snapshot_models_map() -%}
  {#- Current virtual nodes as `{name: {fingerprint, materialized}}`, from the graph and `_dbt_virtual_fps`. -#}
  {%- set fps = dbt_virtual.get_precomputed_fps() -%}
  {%- set out = {} -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- if (dbt_virtual.node_attr(node, 'resource_type', '') | string) == 'model' and dbt_virtual.is_virtual_node(node) -%}
        {%- set name = dbt_virtual.node_attr(node, 'name', '') -%}
        {%- do out.update({name: {
          'fingerprint': fps.get(name, ''),
          'materialized': dbt_virtual.node_attr(dbt_virtual.node_attr(node, 'config', {}), 'materialized', '') | string
        }}) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(out) }}
{%- endmacro %}


{% macro flush_registry(results=none) -%}
  {#-
    `on-run-end` hook (add `"{{ dbt_virtual.flush_registry(results) }}"` to `on-run-end` in dbt_project.yml):
    writes the registry rows of every virtual node that built successfully in ONE `_environments`
    MERGE and one INSERT each for `_releases` and `_intervals`, instead of DML per model post-hook.
    Then, when `dbt-virtual build` asked for it (`_dbt_virtual_snapshot`) and nothing failed, completes
    the release with the objects this run did not build (`virtual_snapshot_release`).
    No-op for `dbt compile`/`dbt test` and when `registry_write_mode` is `per_model`.
  -#}
  {%- if not execute or results is none -%}
    {{ return('') }}
  {%- endif -%}
  {%- if flags.WHICH is defined and flags.WHICH not in ['run', 'build', 'retry'] -%}
    {{ return('') }}
  {%- endif -%}
  {%- if not dbt_virtual.get_config().get('enabled', true) -%}
    {{ return('') }}
  {%- endif -%}
  {#- Companion state is repaired in both registry write modes. -#}
  {%- do dbt_virtual.companion_repair_failed(results, dbt_virtual.get_current_env()) -%}
  {%- if dbt_virtual.registry_write_mode() != 'batch' -%}
    {{ return('') }}
  {%- endif -%}

  {%- set env_name = dbt_virtual.get_current_env() -%}
  {%- set phys_schema = dbt_virtual.get_config().get('physical_schema') -%}
  {%- set release_id = var('_dbt_virtual_release', '') -%}
  {%- set rows = dbt_virtual.collect_registry_rows(results, env_name, phys_schema) -%}
  {%- if (rows | length) > 0 -%}
    {{ log("dbt-virtual: registering " ~ (rows | length) ~ " object(s) in " ~ phys_schema ~ " (env=" ~ env_name ~ ", batched).", info=True) }}
    {%- do dbt_virtual.write_registry_rows(rows, env_name, release_id, var('_dbt_virtual_start', ''), var('_dbt_virtual_end', '')) -%}
  {%- endif -%}

  {%- if var('_dbt_virtual_snapshot', false) and release_id != '' -%}
    {%- set failed = [] -%}
    {%- for r in results -%}
      {%- set status = (r.status | string | lower) -%}
      {%- if status.split('.')[-1] in ['error', 'fail'] -%}
        {%- do failed.append(r) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if (failed | length) > 0 -%}
      {{ log("dbt-virtual: " ~ (failed | length) ~ " node(s) failed; release " ~ release_id ~ " is not completed.", info=True) }}
    {%- else -%}
      {%- do dbt_virtual.virtual_snapshot_release(env=env_name, release_id=release_id, models_json=dbt_virtual.snapshot_models_map()) -%}
    {%- endif -%}
  {%- endif -%}
  {{ return('') }}
{%- endmacro %}


{% macro run_start() -%}
  {#-
    `on-run-start` hook for `dbt-virtual build`: syncs the views of REUSE models (`_dbt_virtual_reuse`,
    `{name: fingerprint}`) and clones the base tables of SEED_INCREMENTAL models (`_dbt_virtual_seed`,
    a list of model names) so the CLI needs no separate `dbt run-operation` (each one parses the project).
    No-op unless those vars are set.
  -#}
  {%- if not execute or (flags.WHICH is defined and flags.WHICH not in ['run', 'build', 'retry']) -%}
    {{ return('') }}
  {%- endif -%}
  {%- set reuse = var('_dbt_virtual_reuse', {}) -%}
  {%- if reuse -%}
    {%- do dbt_virtual.virtual_sync_reused_views(env=dbt_virtual.get_current_env(), release_id=var('_dbt_virtual_release', ''), models_json=reuse) -%}
  {%- endif -%}
  {%- set seed = var('_dbt_virtual_seed', []) -%}
  {%- if seed -%}
    {%- do dbt_virtual.seed_incremental_clones(model_names=seed) -%}
  {%- endif -%}
  {{ return('') }}
{%- endmacro %}


{% macro virtual_finalize_plan(env='staging', release_id='', reuse={}, snapshot=false) -%}
  {#-
    run-operation for a plan with nothing to build (`dbt build` of an empty selection parses the
    project and then runs no hooks): syncs REUSE views, then completes the release.
  -#}
  {%- if reuse -%}
    {%- do dbt_virtual.virtual_sync_reused_views(env=env, release_id=release_id, models_json=reuse) -%}
  {%- endif -%}
  {%- if snapshot and release_id != '' -%}
    {%- do dbt_virtual.virtual_snapshot_release(env=env, release_id=release_id, models_json=dbt_virtual.snapshot_models_map()) -%}
  {%- endif -%}
{%- endmacro %}
