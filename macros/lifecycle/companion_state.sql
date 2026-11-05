{#-
  DECISION(companion_state_for_manifest_tracked_models)
  TECH_DEBT(companion_state_gaps): remaining gaps (bare runs, stale snapshots, shared tables, key shape), see docs/tech_debt/companion_state_gaps.md

  Companion state: rows in a table outside the virtual models (e.g. the Snowplow incremental
  manifest, one row per model name with `last_success`) that say how much input a model's table
  has already consumed. The physical table `<model>__<fp>` and that row describe the same thing, so
  they must move together: seeded together, reset together, promoted together, rolled back together.

  Configuration (`vars.dbt_virtual`):

    companion_state:
      - table: snowplow_unified_incremental_manifest   # a model name, `schema.table` or `db.schema.table`
        key_column: model                               # default `model`
        tag: snowplow_unified_incremental               # optional: only models with this tag
        models: [a, b]                                  # optional: only these models
        env_tables:                                     # optional: where each environment keeps it
          prod: rt_pipeline_prod1_snowplow_manifest.snowplow_unified_incremental_manifest
    companion_on_full_build: error                      # or `reset`

  Only virtual incremental models are tracked. Model of the state: while an environment points at
  a fingerprint, its live rows ARE that table's state; they are saved (`_companion_state`) when
  the environment leaves the fingerprint and written back when it returns or another environment
  adopts the table. Nothing depends on hook order: package on-run-end hooks (Snowplow's manifest
  update) run AFTER the root project's (observed in tests/test_dbt_duckdb_companion_state.py), so
  recording state at the end of a run would read stale rows.
  See docs/guides/companion_state.md.
-#}


{% macro companion_entries() -%}
  {%- set raw = dbt_virtual.get_config().get('companion_state', none) or [] -%}
  {%- if raw is mapping -%}
    {%- set raw = [raw] -%}
  {%- endif -%}
  {%- set entries = [] -%}
  {%- for item in raw -%}
    {%- set entry_cfg = dbt_virtual.render_config_value({'table': item} if item is string else item) -%}
    {%- if not entry_cfg.get('table') -%}
      {{ exceptions.raise_compiler_error("dbt-virtual: every companion_state entry needs a `table`.") }}
    {%- endif -%}
    {%- do entries.append({
      'table': entry_cfg['table'] | string,
      'key_column': entry_cfg.get('key_column') or 'model',
      'env_tables': entry_cfg.get('env_tables') or {},
      'tag': entry_cfg.get('tag'),
      'models': entry_cfg.get('models') or []
    }) -%}
  {%- endfor -%}
  {{ return(entries) }}
{%- endmacro %}


{% macro companion_applies(entry, node) -%}
  {#- True when `entry` tracks `node`: a virtual incremental model matching the entry's filters. -#}
  {%- if node is none or not dbt_virtual.is_virtual_node(node) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- if dbt_virtual.node_attr(node_cfg, 'materialized', '') != 'incremental' -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if (entry['models'] | length) > 0 -%}
    {{ return(dbt_virtual.node_attr(node, 'name', '') in entry['models']) }}
  {%- endif -%}
  {%- if entry['tag'] -%}
    {{ return(entry['tag'] in (dbt_virtual.node_attr(node, 'tags', []) or [])) }}
  {%- endif -%}
  {{ return(true) }}
{%- endmacro %}


{% macro companion_relation(entry, env) -%}
  {#- The relation holding `env`'s state. A bare name is a model of the project, resolved in the
      current target; use `env_tables` when an operation runs from a different target. -#}
  {%- set ref_str = (entry['env_tables'].get(env) or entry['table']) | string -%}
  {%- set parts = ref_str.split('.') -%}
  {%- if (parts | length) == 3 -%}
    {{ return(api.Relation.create(database=parts[0], schema=parts[1], identifier=parts[2], type='table')) }}
  {%- elif (parts | length) == 2 -%}
    {{ return(api.Relation.create(database=target.database, schema=parts[0], identifier=parts[1], type='table')) }}
  {%- endif -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- if dbt_virtual.node_attr(node, 'resource_type', '') == 'model' and dbt_virtual.node_attr(node, 'name', '') == ref_str -%}
        {{ return(api.Relation.create(
          database=dbt_virtual.node_attr(node, 'database', target.database),
          schema=dbt_virtual.node_attr(node, 'schema', target.schema),
          identifier=dbt_virtual.node_attr(node, 'alias', none) or ref_str,
          type='table'
        )) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ exceptions.raise_compiler_error("dbt-virtual: companion_state table '" ~ ref_str ~ "' is neither `schema.table` nor a model of this project.") }}
{%- endmacro %}


{% macro companion_registry_sql(physical_schema) -%}
  {{ return(adapter.dispatch('companion_registry_sql', 'dbt_virtual')(physical_schema)) }}
{%- endmacro %}

{% macro bigquery__companion_registry_sql(physical_schema) -%}
  CREATE TABLE IF NOT EXISTS {{ dbt_virtual.registry_relation(physical_schema, '_companion_state') }} (
    companion STRING NOT NULL,
    model_name STRING NOT NULL,
    fingerprint STRING NOT NULL,
    state_json STRING NOT NULL,
    environment STRING,
    release_id STRING,
    recorded_at TIMESTAMP NOT NULL
  )
{%- endmacro %}

{% macro default__companion_registry_sql(physical_schema) -%}
  CREATE TABLE IF NOT EXISTS {{ dbt_virtual.registry_relation(physical_schema, '_companion_state') }} (
    companion VARCHAR,
    model_name VARCHAR,
    fingerprint VARCHAR,
    state_json VARCHAR,
    environment VARCHAR,
    release_id VARCHAR,
    recorded_at TIMESTAMP
  )
{%- endmacro %}


{% macro ensure_companion_registry() -%}
  {#- `_companion_state` is created on first use, only for projects that configure companion state. -#}
  {%- set phys_schema = dbt_virtual.get_config().get('physical_schema') -%}
  {%- do dbt_virtual.ensure_registry() -%}
  {%- if adapter.get_relation(database=target.database, schema=phys_schema, identifier='_companion_state') is none -%}
    {%- do dbt_virtual.exec_sql_statement(dbt_virtual.companion_registry_sql(phys_schema)) -%}
  {%- endif -%}
{%- endmacro %}


{% macro companion_read(entry, env, model_name) -%}
  {#- The model's rows in `env`'s table as a list of {lower_case_column: string or none}. -#}
  {%- set rel = dbt_virtual.companion_relation(entry, env) -%}
  {%- set found = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) -%}
  {%- set rows = [] -%}
  {%- if found is none -%}
    {{ return(rows) }}
  {%- endif -%}
  {%- set res = run_query("SELECT * FROM " ~ found ~ " WHERE " ~ entry['key_column'] ~ " = " ~ dbt_virtual.sql_string_literal(model_name)) -%}
  {%- for r in res.rows -%}
    {%- set values = {} -%}
    {%- for col_name in res.column_names -%}
      {%- do values.update({(col_name | lower): (none if r[loop.index0] is none else (r[loop.index0] | string))}) -%}
    {%- endfor -%}
    {%- do rows.append(values) -%}
  {%- endfor -%}
  {{ return(rows) }}
{%- endmacro %}


{% macro companion_delete(entry, env, model_name) -%}
  {%- set rel = dbt_virtual.companion_relation(entry, env) -%}
  {%- set found = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) -%}
  {%- if found is not none -%}
    {%- do dbt_virtual.exec_sql_statement("DELETE FROM " ~ found ~ " WHERE " ~ entry['key_column'] ~ " = " ~ dbt_virtual.sql_string_literal(model_name)) -%}
  {%- endif -%}
{%- endmacro %}


{% macro companion_write(entry, env, model_name, rows, like_env=none) -%}
  {#- Makes `env`'s table hold exactly `rows` for the model: delete, then insert. Creates the table
      empty from `like_env`'s when it does not exist yet. Returns false when it could not write. -#}
  {%- set rel = dbt_virtual.companion_relation(entry, env) -%}
  {%- set found = adapter.get_relation(database=rel.database, schema=rel.schema, identifier=rel.identifier) -%}
  {%- if found is none -%}
    {%- if (rows | length) == 0 -%}
      {{ return(true) }}
    {%- endif -%}
    {%- set like = none -%}
    {%- if like_env is not none -%}
      {%- set like_rel = dbt_virtual.companion_relation(entry, like_env) -%}
      {%- set like = adapter.get_relation(database=like_rel.database, schema=like_rel.schema, identifier=like_rel.identifier) -%}
    {%- endif -%}
    {%- if like is none -%}
      {{ log("dbt-virtual WARNING: companion table " ~ rel ~ " does not exist and there is no table to copy its layout from; not writing state for " ~ model_name ~ ".", info=True) }}
      {{ return(false) }}
    {%- endif -%}
    {%- do dbt_virtual.ensure_schema_exists(rel.schema) -%}
    {%- do dbt_virtual.exec_sql_statement("CREATE TABLE " ~ rel ~ " AS SELECT * FROM " ~ like ~ " WHERE 1 = 0") -%}
    {%- do adapter.cache_added(rel) -%}
    {%- set found = rel -%}
  {%- endif -%}
  {%- do dbt_virtual.companion_delete(entry, env, model_name) -%}
  {%- if (rows | length) == 0 -%}
    {{ return(true) }}
  {%- endif -%}
  {%- set columns = adapter.get_columns_in_relation(found) -%}
  {%- set names = [] -%}
  {%- for col in columns -%}
    {%- do names.append(adapter.quote(col.name)) -%}
  {%- endfor -%}
  {%- set tuples = [] -%}
  {%- for row in rows -%}
    {%- set cells = [] -%}
    {%- for col in columns -%}
      {%- set cell = row.get(col.name | lower) -%}
      {%- if cell is none -%}
        {%- do cells.append('NULL') -%}
      {%- else -%}
        {%- do cells.append('CAST(' ~ dbt_virtual.sql_string_literal(cell) ~ ' AS ' ~ col.data_type ~ ')') -%}
      {%- endif -%}
    {%- endfor -%}
    {%- do tuples.append('(' ~ (cells | join(', ')) ~ ')') -%}
  {%- endfor -%}
  {%- do dbt_virtual.exec_sql_statement("INSERT INTO " ~ found ~ " (" ~ (names | join(', ')) ~ ") VALUES " ~ (tuples | join(', '))) -%}
  {{ return(true) }}
{%- endmacro %}


{% macro companion_snapshot_save(entry, model_name, fp, env, release_id, rows) -%}
  {%- set phys_schema = dbt_virtual.get_config().get('physical_schema') -%}
  {%- set snap_rel = dbt_virtual.registry_relation(phys_schema, '_companion_state') -%}
  {%- do dbt_virtual.exec_sql_statement(
    "DELETE FROM " ~ snap_rel ~ " WHERE companion = " ~ dbt_virtual.sql_string_literal(entry['table'])
    ~ " AND model_name = " ~ dbt_virtual.sql_string_literal(model_name)
    ~ " AND fingerprint = " ~ dbt_virtual.sql_string_literal(fp)
  ) -%}
  {%- do dbt_virtual.exec_sql_statement(
    "INSERT INTO " ~ snap_rel ~ " (companion, model_name, fingerprint, state_json, environment, release_id, recorded_at) VALUES ("
    ~ dbt_virtual.sql_string_literal(entry['table']) ~ ", " ~ dbt_virtual.sql_string_literal(model_name) ~ ", "
    ~ dbt_virtual.sql_string_literal(fp) ~ ", " ~ dbt_virtual.sql_string_literal(rows | tojson) ~ ", "
    ~ dbt_virtual.sql_nullable_literal(env) ~ ", " ~ dbt_virtual.sql_nullable_literal(release_id) ~ ", "
    ~ dbt_virtual.current_timestamp_sql() ~ ")"
  ) -%}
{%- endmacro %}


{% macro companion_snapshot_load(entry, model_name, fp, source_database=none, source_schema=none) -%}
  {#- The rows recorded for `<model>__<fp>`, or none when nothing was recorded. With `source_schema`
      (a cross-location promote) they are read from that registry, which may not hold the table. -#}
  {%- set phys_schema = source_schema or dbt_virtual.get_config().get('physical_schema') -%}
  {%- if source_schema is not none and adapter.get_relation(database=(source_database or dbt_virtual.active_database()), schema=source_schema, identifier='_companion_state') is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set res = run_query(
    "SELECT state_json FROM " ~ dbt_virtual.registry_relation(phys_schema, '_companion_state', source_database)
    ~ " WHERE companion = " ~ dbt_virtual.sql_string_literal(entry['table'])
    ~ " AND model_name = " ~ dbt_virtual.sql_string_literal(model_name)
    ~ " AND fingerprint = " ~ dbt_virtual.sql_string_literal(fp)
    ~ " ORDER BY recorded_at DESC"
  ) -%}
  {%- if not res or (res.rows | length) == 0 -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return(fromjson(res.rows[0][0])) }}
{%- endmacro %}


