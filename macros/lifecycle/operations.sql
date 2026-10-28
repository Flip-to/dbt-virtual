{% macro exec_ddl_dml(sql_text) -%}
  {%- for stmt in sql_text.split(';') -%}
    {%- if (stmt | trim) != '' -%}
      {%- if statement is defined -%}
        {%- call statement('dbt_virtual_ddl_dml', fetch_result=False, auto_begin=True) -%}
          {{ stmt }}
        {%- endcall -%}
        {%- if adapter.commit is defined -%}
          {%- do adapter.commit() -%}
        {%- endif -%}
      {%- else -%}
        {%- do run_query(stmt) -%}
      {%- endif -%}
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
    {%- set cand_cnt_res = run_query("SELECT COUNT(*) FROM " ~ cand_rel) -%}
    {%- set cand_cnt = cand_cnt_res.rows[0][0] | int if (cand_cnt_res and (cand_cnt_res.rows | length) > 0) else 0 -%}

    {%- set base_ident = base_map.get(model_name, '') -%}
    {%- if base_ident != '' -%}
      {%- set base_tbl = base_ident.split('.')[-1] -%}
      {%- set base_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=base_tbl, type='table') -%}
      {%- set base_cnt_res = run_query("SELECT COUNT(*) FROM " ~ base_rel) -%}
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
       - Tier 1: scalar UDFs (`function`)
       - Tier 2: virtual views (`model`) -> `<physical_schema>.<model>__<fp>`
       - Tier 3: table functions (`table_function`)
       - Tier 4: stored procedures (`procedure`)
    4. Validates promoted routines via `validate_routine_exists_sql` and updates `_environments` / `_releases`.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set to_schema = dbt_virtual.get_env_schema(to_env) -%}

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
        dependency_tier
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
      WHERE release_id = '{{ release_id }}'
      ORDER BY dependency_tier ASC, model_name ASC
    {%- endset -%}
  {%- else -%}
    {%- set query -%}
      SELECT
        model_name,
        resource_type,
        fingerprint,
        physical_identifier,
        CAST(NULL AS STRING) AS routine_ddl_template,
        CASE resource_type
          WHEN 'function' THEN 1
          WHEN 'model' THEN 2
          WHEN 'table_function' THEN 3
          ELSE 4
        END AS dependency_tier
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
      WHERE environment = '{{ from_env }}'
      ORDER BY dependency_tier ASC, model_name ASC
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
    SELECT model_name, resource_type, fingerprint, physical_identifier
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ to_env }}'
  {%- endset -%}
  {%- set existing_env_res = run_query(existing_env_query) -%}
  {%- set existing_fp_map = {} -%}
  {%- set existing_ident_map = {} -%}
  {%- set existing_type_map = {} -%}
  {%- if existing_env_res -%}
    {%- for erow in existing_env_res.rows -%}
      {%- do existing_fp_map.update({erow[0]: erow[2]}) -%}
      {%- do existing_ident_map.update({erow[0]: erow[3]}) -%}
      {%- do existing_type_map.update({erow[0]: erow[1]}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set promoted_names = [] -%}

  {#- Phase 2: Tiered promotion (Tier 1 UDFs -> Tier 2 Views -> Tier 3 TVFs -> Tier 4 Procedures) -#}
  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set fp = row[2] -%}
    {%- set phys_ident = row[3] -%}
    {%- set routine_ddl = row[4] -%}
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
        {%- set rendered_ddl = routine_ddl | replace('__DBT_VIRTUAL_ENV_SCHEMA__', to_schema) -%}
        {{ log("dbt-virtual promote [" ~ to_env ~ "]: Deploying routine " ~ to_schema ~ "." ~ model_name, info=True) }}
        {%- do dbt_virtual.exec_ddl_dml(rendered_ddl) -%}
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
      release_id
    )) -%}
  {%- endfor -%}

  {#- Phase 3: Demote removed objects that were previously in `to_env` but are no longer in the promoted set -#}
  {%- for old_name, old_type in existing_type_map.items() -%}
    {%- if old_name not in promoted_names -%}
      {%- set demote_rel = api.Relation.create(database=target.database, schema=to_schema, identifier=old_name, type='view') -%}
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
  {%- set env_schema = dbt_virtual.get_env_schema(env) -%}
  {%- set models_map = fromjson(models_json) -%}

  {%- do dbt_virtual.ensure_registry() -%}

  {%- for model_name, fp in models_map.items() -%}
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
      release_id
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
    Tears down an ephemeral virtual environment schema (e.g. `pr_123`) and removes its pointers
    from `_environments` without touching shared `<physical_schema>` tables.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- if env == base_env -%}
    {{ exceptions.raise_compiler_error("dbt-virtual drop-env: Refusing to drop base_environment '" ~ base_env ~ "'.") }}
  {%- endif -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set env_schema = dbt_virtual.get_env_schema(env) -%}

  {{ log("dbt-virtual drop-env: Dropping schema " ~ env_schema ~ " and cleaning _environments for env=" ~ env, info=True) }}
  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_schema_cascade_sql(env_schema)) -%}
  {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments') ~ " WHERE environment = '" ~ env ~ "'") -%}
{%- endmacro %}


{% macro virtual_rollback(env='prod', to_release='') -%}
  {#-
    Instant rollback:
    Repoints all views in `env` back to `to_release` (or the most recent prior `promoted` release in `_releases`).
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set env_schema = dbt_virtual.get_env_schema(env) -%}

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
    SELECT model_name, resource_type
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
  {%- if current_env_res -%}
    {%- for current_row in current_env_res.rows -%}
      {%- do current_env_types.update({current_row[0]: current_row[1]}) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set rows_query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
    WHERE release_id = '{{ target_release }}'
    ORDER BY dependency_tier ASC, model_name ASC
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
    {%- do rollback_names.append(model_name) -%}

    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set view_rel = api.Relation.create(database=target.database, schema=env_schema, identifier=model_name, type='view') -%}
      {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Re-pointing " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set rendered_ddl = routine_ddl | replace('__DBT_VIRTUAL_ENV_SCHEMA__', env_schema) -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Redeploying routine " ~ env_schema ~ "." ~ model_name, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(rendered_ddl) -%}
    {%- endif -%}

    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env,
      model_name,
      resource_type,
      fp,
      phys_ident,
      target_release
    )) -%}
  {%- endfor -%}

  {#- Remove objects introduced after the selected release and clear their active pointers. -#}
  {%- for removed_name, removed_type in current_env_types.items() -%}
    {%- if removed_name not in rollback_names -%}
      {%- set removed_rel = api.Relation.create(database=target.database, schema=env_schema, identifier=removed_name, type='view') -%}
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
  {%- set env_schema = dbt_virtual.get_env_schema(env) -%}

  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(phys_schema)) -%}
  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(env_schema)) -%}
  {%- for stmt in dbt_virtual.ensure_registry_tables_sql(phys_schema) -%}
    {%- do dbt_virtual.exec_ddl_dml(stmt) -%}
  {%- endfor -%}

  {%- set nodes_dict = graph.get('nodes', {}) if graph is mapping else {} -%}
  {%- for unique_id, node in nodes_dict.items() -%}
    {%- if dbt_virtual.is_virtual_node(node) -%}
      {%- set model_name = dbt_virtual.node_attr(node, 'name', '') -%}
      {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
      {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', 'view') -%}
      {%- set fp = dbt_virtual.get_fingerprint(node) -%}

      {%- if mat in ['table', 'incremental'] -%}
        {%- set phys_table = model_name ~ '__' ~ fp -%}
        {%- set phys_ident = phys_schema ~ '.' ~ phys_table -%}
        {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}
        {%- set existing_rel = adapter.get_relation(database=target.database, schema=env_schema, identifier=model_name) -%}

        {%- if existing_rel is not none -%}
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
        {%- else -%}
          {{ log("dbt-virtual adopt [" ~ env ~ "]: Existing relation " ~ env_schema ~ "." ~ model_name ~ " not found; skipping clone.", info=True) }}
        {%- endif -%}

        {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
          phys_schema,
          env,
          model_name,
          'model',
          fp,
          phys_ident,
          release_id
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
            rolled_back_at
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
            NULL
          )
        {%- endset -%}
        {%- do dbt_virtual.exec_ddl_dml(ins_rel_sql) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}
