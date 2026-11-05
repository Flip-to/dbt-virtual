{% macro exec_sql_statement(sql_text) -%}
  {%- if statement is defined -%}
    {%- call statement('dbt_virtual_ddl_dml', fetch_result=False, auto_begin=True) -%}
      {{ sql_text }}
    {%- endcall -%}
    {%- if adapter.commit is defined -%}
      {%- do adapter.commit() -%}
    {%- endif -%}
  {%- else -%}
    {%- do run_query(sql_text) -%}
  {%- endif -%}
{%- endmacro %}


{% macro row_value(row, index, default=none) -%}
  {#- Column `index` of a result row, or `default` for rows from registries without that column. -#}
  {{ return(row[index] if (row | length) > index else default) }}
{%- endmacro %}


{% macro ensure_schema_exists(schema_name) -%}
  {#- Creates `schema_name` through the adapter (dataset location and permissions as dbt uses them). -#}
  {%- if adapter.check_schema_exists is defined and not adapter.check_schema_exists(target.database, schema_name) -%}
    {%- do adapter.create_schema(api.Relation.create(database=target.database, schema=schema_name)) -%}
  {%- endif -%}
{%- endmacro %}


{% macro exec_ddl_dml(sql_text) -%}
  {%- for stmt in sql_text.split(';') -%}
    {%- if (stmt | trim) != '' -%}
      {%- do dbt_virtual.exec_sql_statement(stmt) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_acquire_lock(env='prod', owner='dbt-virtual', ttl_minutes=15) -%}
  {#-
    Concurrency guard / lease lock:
    Clears expired locks for `env`, verifies no active lock is held by another owner,
    and acquires a lease in `<physical_schema>._locks`.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set locks_rel = dbt_virtual.registry_relation(phys_schema, '_locks') -%}

  {%- do dbt_virtual.ensure_registry() -%}

  {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ locks_rel ~ " WHERE environment = '" ~ env ~ "' AND expires_at <= " ~ dbt_virtual.current_timestamp_sql()) -%}

  {%- set active_query -%}
    SELECT owner, expires_at
    FROM {{ locks_rel }}
    WHERE environment = '{{ env }}'
      AND owner != '{{ owner }}'
      AND expires_at > {{ dbt_virtual.current_timestamp_sql() }}
    LIMIT 1
  {%- endset -%}

  {%- set active_res = run_query(active_query) -%}
  {%- if active_res and (active_res.rows | length) > 0 -%}
    {%- set holder = active_res.rows[0][0] -%}
    {%- set exp = active_res.rows[0][1] -%}
    {{ exceptions.raise_compiler_error("dbt-virtual lock conflict: Environment '" ~ env ~ "' is currently locked by '" ~ holder ~ "' (expires at " ~ exp ~ ").") }}
  {%- endif -%}

  {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ locks_rel ~ " WHERE environment = '" ~ env ~ "' AND owner = '" ~ owner ~ "'") -%}
  {%- do dbt_virtual.exec_ddl_dml(
    "INSERT INTO " ~ locks_rel ~ " (environment, owner, acquired_at, expires_at) VALUES ("
    ~ "'" ~ env ~ "', '" ~ owner ~ "', "
    ~ dbt_virtual.current_timestamp_sql() ~ ", "
    ~ dbt_virtual.timestamp_add_minutes_sql(ttl_minutes) ~ ")"
  ) -%}
  {{ log("dbt-virtual lock [" ~ env ~ "]: Acquired lock for owner='" ~ owner ~ "' (ttl=" ~ ttl_minutes ~ "m)", info=True) }}
{%- endmacro %}


{% macro virtual_release_lock(env='prod', owner='dbt-virtual') -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set locks_rel = dbt_virtual.registry_relation(phys_schema, '_locks') -%}
  {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ locks_rel ~ " WHERE environment = '" ~ env ~ "' AND owner = '" ~ owner ~ "'") -%}
  {{ log("dbt-virtual lock [" ~ env ~ "]: Released lock for owner='" ~ owner ~ "'", info=True) }}
{%- endmacro %}


{% macro virtual_audit(from_env='staging', to_env='prod', release_id='', allow_empty=false, max_row_drop_pct=100.0) -%}
  {#-
    SQLMesh-style pre-promotion quality gate:
    1. Resolves candidate `model` tables for `release_id` (or `from_env`).
    2. Compares candidate table row counts against active `to_env` (`prod`) physical tables.
    3. Fails fast via `exceptions.raise_compiler_error` if:
       - A candidate table has 0 rows while `to_env` has > 0 rows (unless `allow_empty=true`).
       - Candidate row count drops by more than `max_row_drop_pct` percent compared to `to_env`.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}

  {%- if release_id != '' -%}
    {%- set cand_query -%}
      SELECT model_name, physical_identifier
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
      WHERE release_id = '{{ release_id }}'
        AND resource_type = 'model'
      ORDER BY model_name ASC
    {%- endset -%}
  {%- else -%}
    {%- set cand_query -%}
      SELECT model_name, physical_identifier
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
      WHERE environment = '{{ from_env }}'
        AND resource_type = 'model'
      ORDER BY model_name ASC
    {%- endset -%}
  {%- endif -%}

  {%- set cand_rows = run_query(cand_query) -%}
  {%- if not cand_rows or (cand_rows.rows | length) == 0 -%}
    {{ log("dbt-virtual audit: No candidate models found for audit.", info=True) }}
    {{ return('') }}
  {%- endif -%}

  {%- set base_query -%}
    SELECT model_name, physical_identifier
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ to_env }}'
      AND resource_type = 'model'
  {%- endset -%}
  {%- set base_rows = run_query(base_query) -%}
  {%- set base_map = {} -%}
  {%- if base_rows -%}
    {%- for brow in base_rows.rows -%}
      {%- do base_map.update({brow[0]: brow[1]}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- for row in cand_rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set cand_ident = row[1] -%}
    {%- set cand_tbl = cand_ident.split('.')[-1] -%}
    {%- set cand_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=cand_tbl, type='table') -%}
    {%- set cand_cnt_res = run_query(dbt_virtual.row_count_sql(cand_rel)) -%}
    {%- set cand_cnt = cand_cnt_res.rows[0][0] | int if (cand_cnt_res and (cand_cnt_res.rows | length) > 0) else 0 -%}

    {%- set base_ident = base_map.get(model_name, '') -%}
    {%- if base_ident != '' -%}
      {%- set base_tbl = base_ident.split('.')[-1] -%}
      {%- set base_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=base_tbl, type='table') -%}
      {%- set base_cnt_res = run_query(dbt_virtual.row_count_sql(base_rel)) -%}
      {%- set base_cnt = base_cnt_res.rows[0][0] | int if (base_cnt_res and (base_cnt_res.rows | length) > 0) else 0 -%}

      {%- if cand_cnt == 0 and base_cnt > 0 and not allow_empty -%}
        {{ exceptions.raise_compiler_error(
          "dbt-virtual audit failed for model '" ~ model_name ~ "': candidate table " ~ cand_rel
          ~ " is empty (0 rows) while " ~ to_env ~ " has " ~ base_cnt ~ " rows."
        ) }}
      {%- endif -%}

      {%- if base_cnt > 0 and (max_row_drop_pct | float) < 100.0 -%}
        {%- set drop_pct = ((base_cnt - cand_cnt) * 100.0) / base_cnt -%}
        {%- if drop_pct > (max_row_drop_pct | float) -%}
          {{ exceptions.raise_compiler_error(
            "dbt-virtual audit failed for model '" ~ model_name ~ "': row count dropped by "
            ~ (drop_pct | round(2)) ~ "% (" ~ base_cnt ~ " -> " ~ cand_cnt
            ~ "), exceeding max_row_drop_pct=" ~ max_row_drop_pct ~ "%."
          ) }}
        {%- endif -%}
      {%- endif -%}
      {{ log("dbt-virtual audit [" ~ model_name ~ "]: OK (candidate=" ~ cand_cnt ~ " rows, " ~ to_env ~ "=" ~ base_cnt ~ " rows)", info=True) }}
    {%- else -%}
      {%- if cand_cnt == 0 and not allow_empty -%}
        {{ exceptions.raise_compiler_error(
          "dbt-virtual audit failed for new model '" ~ model_name ~ "': candidate table " ~ cand_rel ~ " has 0 rows."
        ) }}
      {%- endif -%}
      {{ log("dbt-virtual audit [" ~ model_name ~ "]: OK (new model, candidate=" ~ cand_cnt ~ " rows)", info=True) }}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_export_state(env='staging', base_env='prod') -%}
  {#-
    Live warehouse state introspection:
    Queries `_environments`, `_intervals`, and physical fingerprint tables, then logs a
    machine-readable `DBT_VIRTUAL_STATE_JSON:{...}` line that `dbt-virtual status` / `plan` can consume.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}

  {%- set env_query -%}
    SELECT environment, model_name, fingerprint
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment IN ('{{ env }}', '{{ base_env }}')
  {%- endset -%}
  {%- set env_rows = run_query(env_query) -%}

  {%- set env_state = {} -%}
  {%- set base_env_state = {} -%}
  {%- if env_rows -%}
    {%- for r in env_rows.rows -%}
      {%- if r[0] == env -%}
        {%- do env_state.update({r[1]: r[2]}) -%}
      {%- endif -%}
      {%- if r[0] == base_env -%}
        {%- do base_env_state.update({r[1]: r[2]}) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set tbl_rows = run_query(dbt_virtual.list_physical_tables_sql(phys_schema)) -%}
  {%- set existing_tables = [] -%}
  {%- if tbl_rows -%}
    {%- for r in tbl_rows.rows -%}
      {%- do existing_tables.append(r[0]) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set payload = {
    'env': env,
    'base_env': base_env,
    'physical_schema': phys_schema,
    'env_state': env_state,
    'base_env_state': base_env_state,
    'existing_physical_tables': existing_tables
  } -%}
  {{ log("DBT_VIRTUAL_STATE_JSON:" ~ tojson(payload), info=True) }}
{%- endmacro %}


{% macro virtual_promote(from_env='staging', to_env='prod', release_id='', audit=false, max_row_drop_pct=100.0) -%}
  {#-
    Zero-rebuild promotion:
    1. Optionally runs `virtual_audit` quality gate when `audit=true`.
    2. Verifies every physical table in `release_id` (or `from_env` active state) exists.
    3. Deploys objects in strict dependency tier order:
       - Environment views (`model`) -> `<physical_schema>.<model>__<fp>` first: they are
         `SELECT *` over physical tables and depend on no routine, while routines may read them
         (BigQuery and DuckDB check referenced tables when a routine is created).
       - Scalar UDFs (`function` / `virtual_function`)
       - Tier 3: table functions (`table_function`)
       - Tier 4: stored procedures (`procedure`)
    4. Validates promoted routines via `validate_routine_exists_sql` and updates `_environments` / `_releases`.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set ensured_schemas = [] -%}

  {%- if audit -%}
    {%- do dbt_virtual.virtual_audit(from_env=from_env, to_env=to_env, release_id=release_id, allow_empty=false, max_row_drop_pct=max_row_drop_pct) -%}
  {%- endif -%}

  {%- if release_id != '' -%}
    {%- set query -%}
      SELECT
        model_name,
        resource_type,
        fingerprint,
        physical_identifier,
        routine_ddl_template,
        dependency_tier,
        schema_key
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
      WHERE release_id = '{{ release_id }}'
      ORDER BY CASE resource_type WHEN 'model' THEN 1 WHEN 'function' THEN 2 WHEN 'table_function' THEN 3 ELSE 4 END ASC, model_name ASC
    {%- endset -%}
  {%- else -%}
    {%- set query -%}
      {#- Routines need their DDL template to be deployed into `to_env` (e.g. `init-env`); take it
          from any release that recorded the same routine fingerprint. `CAST(NULL AS STRING)` also
          failed on PostgreSQL. -#}
      SELECT
        e.model_name,
        e.resource_type,
        e.fingerprint,
        e.physical_identifier,
        (
          SELECT MAX(r.routine_ddl_template)
          FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} AS r
          WHERE r.model_name = e.model_name
            AND r.resource_type = e.resource_type
            AND r.fingerprint = e.fingerprint
        ) AS routine_ddl_template,
        CASE e.resource_type
          WHEN 'function' THEN 1
          WHEN 'model' THEN 2
          WHEN 'table_function' THEN 3
          ELSE 4
        END AS dependency_tier,
        e.schema_key
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }} AS e
      WHERE e.environment = '{{ from_env }}'
      ORDER BY CASE e.resource_type WHEN 'model' THEN 1 WHEN 'function' THEN 2 WHEN 'table_function' THEN 3 ELSE 4 END ASC, e.model_name ASC
    {%- endset -%}
  {%- endif -%}

  {%- set rows = run_query(query) -%}
  {%- if not rows or (rows.rows | length) == 0 -%}
    {{ exceptions.raise_compiler_error("dbt-virtual promote: No models found for env=" ~ from_env ~ " release=" ~ release_id) }}
  {%- endif -%}

  {#- Phase 1: Pre-flight verification that every referenced physical table exists -#}
  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set phys_ident = row[3] -%}
    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set exists_rel = adapter.get_relation(database=target.database, schema=phys_schema, identifier=phys_table) -%}
      {%- if exists_rel is none -%}
        {{ exceptions.raise_compiler_error("dbt-virtual promote pre-flight failed: physical table " ~ phys_schema ~ "." ~ phys_table ~ " for model " ~ model_name ~ " does not exist.") }}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}

  {#- Phase 1.5: Query existing pointers in `to_env` for partial promotion & demotion detection -#}
  {%- set existing_env_query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier, schema_key
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ to_env }}'
  {%- endset -%}
  {%- set existing_env_res = run_query(existing_env_query) -%}
  {%- set existing_fp_map = {} -%}
  {%- set existing_ident_map = {} -%}
  {%- set existing_type_map = {} -%}
  {%- set existing_key_map = {} -%}
  {%- if existing_env_res -%}
    {%- for erow in existing_env_res.rows -%}
      {%- do existing_fp_map.update({erow[0]: erow[2]}) -%}
      {%- do existing_ident_map.update({erow[0]: erow[3]}) -%}
      {%- do existing_type_map.update({erow[0]: erow[1]}) -%}
      {%- do existing_key_map.update({erow[0]: dbt_virtual.row_value(erow, 4)}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set promoted_names = [] -%}

  {#- Phase 2: Ordered promotion (views -> UDFs -> TVFs -> procedures), by resource type, not the stored tier. -#}
  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set fp = row[2] -%}
    {%- set phys_ident = row[3] -%}
    {%- set routine_ddl = row[4] -%}
    {%- set schema_key = dbt_virtual.row_value(row, 6) -%}
    {%- set to_schema = dbt_virtual.env_schema_for(to_env, schema_key) -%}
    {%- do promoted_names.append(model_name) -%}

    {%- if resource_type == 'model' -%}
      {%- set target_phys_ident = phys_ident -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set target_phys_ident = to_schema ~ '.' ~ model_name -%}
    {%- else -%}
      {%- set target_phys_ident = phys_ident -%}
    {%- endif -%}

    {%- if existing_fp_map.get(model_name, '') == fp and existing_ident_map.get(model_name, '') == target_phys_ident -%}
      {{ log("dbt-virtual promote [" ~ to_env ~ "]: Skipping unchanged " ~ model_name ~ " (fp=" ~ fp ~ " already promoted)", info=True) }}
    {%- else -%}
      {%- if to_schema not in ensured_schemas -%}
        {%- do dbt_virtual.ensure_schema_exists(to_schema) -%}
        {%- do ensured_schemas.append(to_schema) -%}
      {%- endif -%}
      {%- if resource_type == 'model' -%}
        {%- set phys_table = phys_ident.split('.')[-1] -%}
        {%- set existing_target_rel = adapter.get_relation(database=target.database, schema=to_schema, identifier=model_name) -%}
        {%- if existing_target_rel is not none and existing_target_rel.type is defined and existing_target_rel.type == 'table' -%}
          {{ log("dbt-virtual promote [" ~ to_env ~ "]: Dropping legacy physical table " ~ existing_target_rel ~ " before creating virtual view", info=True) }}
          {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(existing_target_rel, 'table')) -%}
        {%- endif -%}
        {%- set view_rel = api.Relation.create(database=target.database, schema=to_schema, identifier=model_name, type='view') -%}
        {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}

        {{ log("dbt-virtual promote [" ~ to_env ~ "]: " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
        {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}
      {%- elif routine_ddl is not none and routine_ddl != '' -%}
        {%- set rendered_ddl = dbt_virtual.resolve_env_placeholders_sql(routine_ddl, to_env) -%}
        {{ log("dbt-virtual promote [" ~ to_env ~ "]: Deploying routine " ~ to_schema ~ "." ~ model_name, info=True) }}
        {%- do dbt_virtual.exec_sql_statement(rendered_ddl) -%}
        {%- do run_query(dbt_virtual.validate_routine_exists_sql(to_schema, model_name)) -%}
      {%- endif -%}
    {%- endif -%}

    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      to_env,
      model_name,
      resource_type,
      fp,
      target_phys_ident,
      release_id,
      schema_key
    )) -%}
  {%- endfor -%}

  {#- Phase 3: Demote removed objects that were previously in `to_env` but are no longer in the promoted set.
      A model that is still a virtual node in this project is never demoted: a release written by a
      partial build (REUSE models, `--select`) does not list it, and demoting it dropped live views. -#}
  {%- set project_virtual_names = [] -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- if dbt_virtual.is_virtual_node(node) -%}
        {%- do project_virtual_names.append(dbt_virtual.node_attr(node, 'name', '')) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- for old_name, old_type in existing_type_map.items() -%}
    {%- if old_name not in promoted_names and old_name in project_virtual_names -%}
      {{ log("dbt-virtual promote [" ~ to_env ~ "]: Keeping " ~ old_type ~ " " ~ old_name ~ " (not in release " ~ release_id ~ " but still a virtual model in this project)", info=True) }}
    {%- elif old_name not in promoted_names -%}
      {%- set demote_schema = dbt_virtual.env_schema_for(to_env, existing_key_map.get(old_name)) -%}
      {%- set demote_rel = api.Relation.create(database=target.database, schema=demote_schema, identifier=old_name, type='view') -%}
      {{ log("dbt-virtual demote [" ~ to_env ~ "]: Removing demoted " ~ old_type ~ " " ~ demote_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(demote_rel, old_type)) -%}
      {%- do dbt_virtual.exec_ddl_dml(
        "DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
        ~ " WHERE environment = '" ~ to_env ~ "' AND model_name = '" ~ old_name ~ "'"
      ) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- if release_id != '' -%}
    {%- do dbt_virtual.exec_ddl_dml("UPDATE " ~ dbt_virtual.registry_relation(phys_schema, '_releases') ~ " SET status = 'promoted', target_environment = '" ~ to_env ~ "', promoted_at = " ~ dbt_virtual.current_timestamp_sql() ~ " WHERE release_id = '" ~ release_id ~ "'") -%}
  {%- endif -%}
{%- endmacro %}


{% macro virtual_sync_reused_views(env='staging', release_id='', models_json='{}') -%}
  {#-
    Zero-compute view sync for `REUSE` models whose `<model>__<fp>` physical table already exists in
    `<physical_schema>`. Avoids invoking `dbt build` on unchanged models while keeping `<env_schema>.<model>`
    views and `_environments` / `_releases` up to date.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set models_map = fromjson(models_json) -%}
  {%- set ensured_schemas = [] -%}

  {%- do dbt_virtual.ensure_registry() -%}

  {%- for model_name, fp in models_map.items() -%}
    {%- set schema_key = dbt_virtual.node_schema_key(dbt_virtual.virtual_node_by_name(model_name)) -%}
    {%- set env_schema = dbt_virtual.env_schema_for(env, schema_key) -%}
    {%- if env_schema not in ensured_schemas -%}
      {%- do dbt_virtual.ensure_schema_exists(env_schema) -%}
      {%- do ensured_schemas.append(env_schema) -%}
    {%- endif -%}
    {%- set phys_table = model_name ~ '__' ~ fp -%}
    {%- set view_rel = api.Relation.create(database=target.database, schema=env_schema, identifier=model_name, type='view') -%}
    {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}

    {{ log("dbt-virtual reuse [" ~ env ~ "]: Pointing " ~ view_rel ~ " -> " ~ phys_rel ~ " (zero compute)", info=True) }}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}

    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env,
      model_name,
      'model',
      fp,
      phys_schema ~ '.' ~ phys_table,
      release_id,
      schema_key
    )) -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_init_env(env='pr_env', from_env='prod') -%}
  {#-
    SQLMesh-style instant virtual environment bootstrap:
    Creates schema `<env_schema>` and points all virtual views at `from_env`'s active physical tables
    in zero compute time.
  -#}
  {%- set env_schema = dbt_virtual.get_env_schema(env) -%}

  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(env_schema)) -%}
  {%- do dbt_virtual.virtual_promote(from_env=from_env, to_env=env, release_id='') -%}
{%- endmacro %}


{% macro virtual_drop_env(env) -%}
  {#-
    Tears down an ephemeral virtual environment's schemas (e.g. `pr_123`) and removes its pointers
    from `_environments` without touching shared `<physical_schema>` tables.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- if env == base_env -%}
    {{ exceptions.raise_compiler_error("dbt-virtual drop-env: Refusing to drop base_environment '" ~ base_env ~ "'.") }}
  {%- endif -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set env_rel = dbt_virtual.registry_relation(phys_schema, '_environments') -%}

  {#- Every custom schema seen in the registry or the project, plus the default (none). -#}
  {%- set keys = [none] -%}
  {%- set env_keys = [none] -%}
  {%- set key_rows = run_query("SELECT DISTINCT environment, schema_key FROM " ~ env_rel) -%}
  {%- if key_rows -%}
    {%- for row in key_rows.rows -%}
      {%- set row_key = dbt_virtual.row_value(row, 1) -%}
      {%- if row_key not in keys -%}
        {%- do keys.append(row_key) -%}
      {%- endif -%}
      {%- if row[0] == env and row_key not in env_keys -%}
        {%- do env_keys.append(row_key) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- set node_key = dbt_virtual.node_schema_key(node) if dbt_virtual.is_virtual_node(node) else none -%}
      {%- if node_key not in keys -%}
        {%- do keys.append(node_key) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {#- Never drop a schema that holds shared physical tables or belongs to another environment. -#}
  {%- set protected = {(phys_schema | lower): 'physical_schema'} -%}
  {%- set other_envs = [base_env] -%}
  {%- for other_env in (cfg.get('env_schemas', {}) or {}).keys() -%}
    {%- if other_env != env and other_env not in other_envs -%}
      {%- do other_envs.append(other_env) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- for other_env in other_envs -%}
    {%- for key in keys -%}
      {%- do protected.update({(dbt_virtual.env_schema_for(other_env, key) | lower): 'environment ' ~ other_env}) -%}
    {%- endfor -%}
  {%- endfor -%}

  {%- set env_schemas_to_drop = [] -%}
  {%- for key in env_keys -%}
    {%- set schema_name = dbt_virtual.env_schema_for(env, key) -%}
    {%- if (schema_name | lower) in protected -%}
      {{ exceptions.raise_compiler_error("dbt-virtual drop-env: Refusing to drop schema '" ~ schema_name ~ "' of env '" ~ env ~ "': it is the schema of " ~ protected[schema_name | lower] ~ ".") }}
    {%- endif -%}
    {%- if schema_name not in env_schemas_to_drop -%}
      {%- do env_schemas_to_drop.append(schema_name) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- for schema_name in env_schemas_to_drop -%}
    {{ log("dbt-virtual drop-env: Dropping schema " ~ schema_name ~ " of env=" ~ env, info=True) }}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_schema_cascade_sql(schema_name)) -%}
  {%- endfor -%}
  {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ env_rel ~ " WHERE environment = '" ~ env ~ "'") -%}
{%- endmacro %}


{% macro virtual_rollback(env='prod', to_release='') -%}
  {#-
    Instant rollback:
    Repoints all views in `env` back to `to_release` (or the most recent prior `promoted` release in `_releases`).
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set ensured_schemas = [] -%}

  {%- set target_release = to_release -%}
  {%- if target_release == '' -%}
    {%- set find_prev_query -%}
      SELECT release_id
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
      WHERE status = 'promoted'
        AND target_environment = '{{ env }}'
      GROUP BY release_id
      ORDER BY MAX(promoted_at) DESC
      LIMIT 1 OFFSET 1
    {%- endset -%}
    {%- set prev_res = run_query(find_prev_query) -%}
    {%- if not prev_res or (prev_res.rows | length) == 0 -%}
      {{ exceptions.raise_compiler_error("dbt-virtual rollback: No previous promoted release found for env=" ~ env) }}
    {%- endif -%}
    {%- set target_release = prev_res.rows[0][0] -%}
  {%- endif -%}

  {{ log("dbt-virtual rollback [" ~ env ~ "]: Rolling back to release " ~ target_release, info=True) }}

  {%- set current_env_query -%}
    SELECT model_name, resource_type, schema_key
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ env }}'
    ORDER BY CASE resource_type
      WHEN 'procedure' THEN 4
      WHEN 'table_function' THEN 3
      WHEN 'model' THEN 2
      WHEN 'function' THEN 1
      ELSE 0
    END DESC, model_name ASC
  {%- endset -%}
  {%- set current_env_res = run_query(current_env_query) -%}
  {%- set current_env_types = {} -%}
  {%- set current_env_keys = {} -%}
  {%- if current_env_res -%}
    {%- for current_row in current_env_res.rows -%}
      {%- do current_env_types.update({current_row[0]: current_row[1]}) -%}
      {%- do current_env_keys.update({current_row[0]: dbt_virtual.row_value(current_row, 2)}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set rows_query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, schema_key
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
    WHERE release_id = '{{ target_release }}'
    ORDER BY CASE resource_type WHEN 'model' THEN 1 WHEN 'function' THEN 2 WHEN 'table_function' THEN 3 ELSE 4 END ASC, model_name ASC
  {%- endset -%}

  {%- set rows = run_query(rows_query) -%}
  {%- if not rows or (rows.rows | length) == 0 -%}
    {{ exceptions.raise_compiler_error("dbt-virtual rollback: No models found for release " ~ target_release) }}
  {%- endif -%}
  {%- set rollback_names = [] -%}
  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set fp = row[2] -%}
    {%- set phys_ident = row[3] -%}
    {%- set routine_ddl = row[4] -%}
    {%- set schema_key = dbt_virtual.row_value(row, 5) -%}
    {%- set env_schema = dbt_virtual.env_schema_for(env, schema_key) -%}
    {%- do rollback_names.append(model_name) -%}
    {%- if env_schema not in ensured_schemas -%}
      {%- do dbt_virtual.ensure_schema_exists(env_schema) -%}
      {%- do ensured_schemas.append(env_schema) -%}
    {%- endif -%}

    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set view_rel = api.Relation.create(database=target.database, schema=env_schema, identifier=model_name, type='view') -%}
      {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Re-pointing " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set rendered_ddl = dbt_virtual.resolve_env_placeholders_sql(routine_ddl, env) -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Redeploying routine " ~ env_schema ~ "." ~ model_name, info=True) }}
      {%- do dbt_virtual.exec_sql_statement(rendered_ddl) -%}
      {#- Same identifier `virtual_promote` records for a deployed routine. -#}
      {%- set phys_ident = env_schema ~ '.' ~ model_name -%}
    {%- endif -%}

    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env,
      model_name,
      resource_type,
      fp,
      phys_ident,
      target_release,
      schema_key
    )) -%}
  {%- endfor -%}

  {#- Mark the releases being undone so the next default rollback walks further back instead of
      returning to the same "previous" release, then make the target the active release. -#}
  {%- set releases_rel = dbt_virtual.registry_relation(phys_schema, '_releases') -%}
  {%- do dbt_virtual.exec_ddl_dml(
    "UPDATE " ~ releases_rel
    ~ " SET status = 'rolled_back', rolled_back_at = " ~ dbt_virtual.current_timestamp_sql()
    ~ " WHERE target_environment = '" ~ env ~ "' AND status = 'promoted' AND release_id != '" ~ target_release ~ "'"
    ~ " AND promoted_at > (SELECT MAX(t.promoted_at) FROM " ~ releases_rel ~ " AS t WHERE t.release_id = '" ~ target_release ~ "')"
  ) -%}
  {%- do dbt_virtual.exec_ddl_dml(
    "UPDATE " ~ releases_rel
    ~ " SET status = 'promoted', target_environment = '" ~ env ~ "', promoted_at = " ~ dbt_virtual.current_timestamp_sql()
    ~ ", rolled_back_at = NULL WHERE release_id = '" ~ target_release ~ "'"
  ) -%}

  {#- Remove objects introduced after the selected release and clear their active pointers. -#}
  {%- for removed_name, removed_type in current_env_types.items() -%}
    {%- if removed_name not in rollback_names -%}
      {%- set removed_schema = dbt_virtual.env_schema_for(env, current_env_keys.get(removed_name)) -%}
      {%- set removed_rel = api.Relation.create(database=target.database, schema=removed_schema, identifier=removed_name, type='view') -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Removing post-release " ~ removed_type ~ " " ~ removed_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(removed_rel, removed_type)) -%}
      {%- do dbt_virtual.exec_ddl_dml(
        "DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
        ~ " WHERE environment = '" ~ env ~ "' AND model_name = '" ~ removed_name ~ "'"
      ) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_gc(dry_run=true, retention_releases=5, retention_days=7) -%}
  {#-
    Janitor GC:
    Identifies `<model>__<fp>` tables in `<physical_schema>` older than `retention_days`
    that are not referenced by any environment in `_environments` or by the last `retention_releases`
    releases in `_releases`.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}

  {%- set orphan_query = dbt_virtual.find_orphan_tables_sql(phys_schema, retention_releases, retention_days) -%}
  {%- set orphans = run_query(orphan_query) -%}
  {%- if not orphans or (orphans.rows | length) == 0 -%}
    {{ log("dbt-virtual gc: 0 orphaned physical tables eligible for deletion.", info=True) }}
    {{ return('') }}
  {%- endif -%}

  {%- for row in orphans.rows -%}
    {%- set tbl = row[0] -%}
    {%- set drop_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=tbl, type='table') -%}
    {%- if dry_run -%}
      {{ log("dbt-virtual gc [DRY-RUN]: Would drop " ~ drop_rel, info=True) }}
    {%- else -%}
      {{ log("dbt-virtual gc: Dropping " ~ drop_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml("DROP TABLE IF EXISTS " ~ drop_rel) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_adopt(env='prod', release_id='v_adopt_initial', drop_existing_table=true) -%}
  {#-
    Zero-Compute Migration / Adoption of an Existing dbt Project:
    For every virtualized model (`virtual_env: true`) in `graph.nodes`:
      1. Computes its current fingerprint `<fp>` and target physical table `<physical_schema>.<model>__<fp>`.
      2. If `<env_schema>.<model>` currently exists as a physical BASE TABLE in the warehouse:
         - Zero-copy CLONEs `<env_schema>.<model>` -> `<physical_schema>.<model>__<fp>` (0 compute!).
         - When `drop_existing_table=true`, drops the old physical table `<env_schema>.<model>` and
           immediately creates the virtual view `<env_schema>.<model>` -> `<physical_schema>.<model>__<fp>`
           so consumers experience zero downtime and zero table rebuilds.
      3. Records the adopted model or routine in `<physical_schema>._environments` and `<physical_schema>._releases`.
  -#}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}

  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}

  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(phys_schema)) -%}
  {#- `ensure_registry_tables_sql` returns one SQL string; iterating it ran one statement per character. -#}
  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.ensure_registry_tables_sql(phys_schema)) -%}

  {%- set nodes_dict = graph.get('nodes', {}) if graph is mapping else {} -%}
  {%- for unique_id, node in nodes_dict.items() -%}
    {%- if dbt_virtual.is_virtual_node(node) -%}
      {%- set model_name = dbt_virtual.node_attr(node, 'name', '') -%}
      {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
      {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', 'view') -%}
      {%- set fp = dbt_virtual.get_fingerprint(node) -%}
      {%- set schema_key = dbt_virtual.node_schema_key(node) -%}
      {%- set env_schema = dbt_virtual.env_schema_for(env, schema_key) -%}

      {%- if mat in ['table', 'incremental'] -%}
        {%- set phys_table = model_name ~ '__' ~ fp -%}
        {%- set phys_ident = phys_schema ~ '.' ~ phys_table -%}
        {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}
        {%- set existing_rel = adapter.get_relation(database=target.database, schema=env_schema, identifier=model_name) -%}
        {%- set phys_exists = adapter.get_relation(database=target.database, schema=phys_schema, identifier=phys_table) is not none -%}

        {%- if existing_rel is not none and existing_rel.type is defined and existing_rel.type == 'view' -%}
          {#- Already a view (adopted before, or a view model): a view cannot be cloned. -#}
          {{ log("dbt-virtual adopt [" ~ env ~ "]: " ~ existing_rel ~ " is already a view; not cloning.", info=True) }}
        {%- elif existing_rel is not none -%}
          {{ log("dbt-virtual adopt [" ~ env ~ "]: Cloning existing table " ~ existing_rel ~ " -> " ~ phys_rel ~ " (0 compute)", info=True) }}
          {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.clone_table_sql(existing_rel, phys_rel)) -%}
          {%- if drop_existing_table -%}
            {#- In BigQuery/Snowflake/Postgres, replacing a BASE TABLE with a VIEW of the same name requires dropping the table first -#}
            {%- if existing_rel.type is not defined or existing_rel.type != 'view' -%}
              {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(existing_rel, 'table')) -%}
            {%- endif -%}
            {%- set view_rel = api.Relation.create(database=target.database, schema=env_schema, identifier=model_name, type='view') -%}
            {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}
          {%- endif -%}
          {%- set phys_exists = true -%}
        {%- else -%}
          {{ log("dbt-virtual adopt [" ~ env ~ "]: Existing relation " ~ env_schema ~ "." ~ model_name ~ " not found; skipping clone.", info=True) }}
        {%- endif -%}

        {%- if not phys_exists -%}
          {#- Registering a pointer to a table that does not exist made `plan` report REUSE and
              `promote` fail its pre-flight. -#}
          {{ log("dbt-virtual adopt [" ~ env ~ "]: " ~ phys_rel ~ " does not exist; not registering " ~ model_name ~ ".", info=True) }}
        {%- else -%}
        {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
          phys_schema,
          env,
          model_name,
          'model',
          fp,
          phys_ident,
          release_id,
          schema_key
        )) -%}
        {%- set ins_rel_sql -%}
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
            '{{ env }}',
            '{{ env }}',
            '{{ model_name }}',
            'model',
            2,
            '{{ fp }}',
            '{{ phys_ident }}',
            NULL,
            'promoted',
            {{ dbt_virtual.current_timestamp_sql() }},
            {{ dbt_virtual.current_timestamp_sql() }},
            NULL,
            {{ dbt_virtual.sql_nullable_literal(schema_key) }}
          )
        {%- endset -%}
        {%- do dbt_virtual.exec_ddl_dml(ins_rel_sql) -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_seed_incremental_clones(model_names=[]) -%}
  {#- run-operation entry point for `seed_incremental_clones` (called by `dbt-virtual build`). -#}
  {%- do dbt_virtual.seed_incremental_clones(model_names=(model_names or [])) -%}
{%- endmacro %}


{% macro virtual_snapshot_release(env='staging', release_id='', models_json='{}') -%}
  {#-
    Completes `release_id` so it describes the whole environment, not only what this build ran.
    Hooks register a release row only for models dbt built; REUSE tables, unchanged routines and
    unselected models were missing, so promoting or rolling back to that release demoted them.

    For every current virtual node in `models_json` (`{name: {"materialized": ...}}`) that has no
    row in the release yet, copies the environment's active pointer (and, for routines, the latest
    recorded DDL template for that fingerprint) into `_releases`.
  -#}
  {%- if release_id == '' -%}
    {{ exceptions.raise_compiler_error("dbt-virtual snapshot-release: release_id is required.") }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set releases_rel = dbt_virtual.registry_relation(phys_schema, '_releases') -%}
  {%- set models_map = fromjson(models_json) if models_json is string else models_json -%}
  {%- do dbt_virtual.ensure_registry() -%}

  {%- set in_release = [] -%}
  {%- set rel_rows = run_query("SELECT model_name FROM " ~ releases_rel ~ " WHERE release_id = '" ~ release_id ~ "'") -%}
  {%- if rel_rows -%}
    {%- for row in rel_rows.rows -%}
      {%- do in_release.append(row[0]) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set env_query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier, schema_key
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ env }}'
  {%- endset -%}
  {%- set env_rows = run_query(env_query) -%}
  {%- set env_ptrs = {} -%}
  {%- if env_rows -%}
    {%- for row in env_rows.rows -%}
      {%- do env_ptrs.update({row[0]: {'resource_type': row[1], 'fingerprint': row[2], 'physical_identifier': row[3], 'schema_key': dbt_virtual.row_value(row, 4)}}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set template_query -%}
    SELECT model_name, fingerprint, routine_ddl_template
    FROM {{ releases_rel }}
    WHERE resource_type != 'model'
      AND routine_ddl_template IS NOT NULL
    ORDER BY created_at ASC
  {%- endset -%}
  {%- set template_rows = run_query(template_query) -%}
  {%- set templates = {} -%}
  {%- if template_rows -%}
    {%- for row in template_rows.rows -%}
      {%- do templates.update({row[0] ~ '|' ~ row[1]: row[2]}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set values = [] -%}
  {%- for model_name, info in models_map.items() | sort -%}
    {%- set mat = (info or {}).get('materialized', '') if info is mapping else '' -%}
    {%- set ptr = env_ptrs.get(model_name) -%}
    {%- if model_name in in_release or mat not in ['table', 'incremental', 'table_function', 'function', 'virtual_function', 'procedure'] -%}
    {%- elif ptr is none -%}
      {{ log("dbt-virtual snapshot-release [" ~ release_id ~ "]: " ~ model_name ~ " has no pointer in env=" ~ env ~ "; it is not part of the release.", info=True) }}
    {%- else -%}
      {%- set rtype = ptr['resource_type'] -%}
      {%- set tier = {'function': 1, 'model': 2, 'table_function': 3}.get(rtype, 4) -%}
      {%- set template = templates.get(model_name ~ '|' ~ ptr['fingerprint']) if rtype != 'model' else none -%}
      {%- if (info or {}).get('fingerprint') and info.get('fingerprint') != ptr['fingerprint'] -%}
        {{ log("dbt-virtual snapshot-release [" ~ release_id ~ "]: " ~ model_name ~ " in env=" ~ env ~ " is at " ~ ptr['fingerprint'] ~ ", not the planned " ~ info.get('fingerprint') ~ "; recording the environment's version.", info=True) }}
      {%- endif -%}
      {%- do values.append(
        "('" ~ release_id ~ "', '" ~ env ~ "', NULL, '" ~ model_name ~ "', '" ~ rtype ~ "', " ~ tier ~ ", '"
        ~ ptr['fingerprint'] ~ "', '" ~ ptr['physical_identifier'] ~ "', "
        ~ (dbt_virtual.sql_string_literal(template) if template is not none else 'NULL')
        ~ ", 'candidate', " ~ dbt_virtual.current_timestamp_sql() ~ ", NULL, NULL, "
        ~ dbt_virtual.sql_nullable_literal(ptr['schema_key']) ~ ")"
      ) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- for batch in values | batch(200) -%}
    {%- set insert_sql -%}
      INSERT INTO {{ releases_rel }} (
        release_id, source_environment, target_environment, model_name, resource_type, dependency_tier,
        fingerprint, physical_identifier, routine_ddl_template, status, created_at, promoted_at, rolled_back_at,
        schema_key
      ) VALUES
      {{ batch | join(',\n') }}
    {%- endset -%}
    {%- do dbt_virtual.exec_sql_statement(insert_sql) -%}
  {%- endfor -%}
  {{ log("dbt-virtual snapshot-release [" ~ release_id ~ "]: added " ~ (values | length) ~ " unchanged object(s) from env=" ~ env ~ ".", info=True) }}
{%- endmacro %}
