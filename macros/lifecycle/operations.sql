{% macro virtual_promote(from_env='staging', to_env='prod', release_id='') -%}
  {#-
    Zero-rebuild promotion:
    1. Verifies every physical table in `release_id` (or `from_env` active state) exists.
    2. Deploys objects in strict dependency tier order:
       - Tier 1: scalar UDFs (`function`)
       - Tier 2: virtual views (`model`) -> `<physical_schema>.<model>__<fp>`
       - Tier 3: table functions (`table_function`)
       - Tier 4: stored procedures (`procedure`)
    3. Updates `_environments` and `_releases` via multi-adapter dispatch macros.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set to_schema = dbt_virtual.get_env_schema(to_env) -%}

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

  {#- Phase 2: Tiered promotion (Tier 1 UDFs -> Tier 2 Views -> Tier 3 TVFs -> Tier 4 Procedures) -#}
  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set fp = row[2] -%}
    {%- set phys_ident = row[3] -%}
    {%- set routine_ddl = row[4] -%}

    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set view_rel = api.Relation.create(database=target.database, schema=to_schema, identifier=model_name, type='view') -%}
      {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}

      {{ log("dbt-virtual promote [" ~ to_env ~ "]: " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
      {%- do run_query(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}
      {%- set target_phys_ident = phys_ident -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set rendered_ddl = routine_ddl | replace('__DBT_VIRTUAL_ENV_SCHEMA__', to_schema) -%}
      {{ log("dbt-virtual promote [" ~ to_env ~ "]: Deploying routine " ~ to_schema ~ "." ~ model_name, info=True) }}
      {%- do run_query(rendered_ddl) -%}
      {%- set target_phys_ident = to_schema ~ '.' ~ model_name -%}
    {%- else -%}
      {%- set target_phys_ident = phys_ident -%}
    {%- endif -%}

    {%- do run_query(dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      to_env,
      model_name,
      resource_type,
      fp,
      target_phys_ident,
      release_id
    )) -%}
  {%- endfor -%}

  {%- if release_id != '' -%}
    {%- do run_query("UPDATE " ~ dbt_virtual.registry_relation(phys_schema, '_releases') ~ " SET status = 'promoted', target_environment = '" ~ to_env ~ "', promoted_at = " ~ dbt_virtual.current_timestamp_sql() ~ " WHERE release_id = '" ~ release_id ~ "'") -%}
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
    {%- do run_query(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}

    {%- do run_query(dbt_virtual.upsert_environment_pointer_sql(
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

  {%- do run_query(dbt_virtual.create_schema_if_not_exists_sql(env_schema)) -%}
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
  {%- do run_query(dbt_virtual.drop_schema_cascade_sql(env_schema)) -%}
  {%- do run_query("DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments') ~ " WHERE environment = '" ~ env ~ "'") -%}
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

  {%- set rows_query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }}
    WHERE release_id = '{{ target_release }}'
    ORDER BY dependency_tier ASC, model_name ASC
  {%- endset -%}

  {%- set rows = run_query(rows_query) -%}
  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set fp = row[2] -%}
    {%- set phys_ident = row[3] -%}
    {%- set routine_ddl = row[4] -%}

    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set view_rel = api.Relation.create(database=target.database, schema=env_schema, identifier=model_name, type='view') -%}
      {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Re-pointing " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
      {%- do run_query(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set rendered_ddl = routine_ddl | replace('__DBT_VIRTUAL_ENV_SCHEMA__', env_schema) -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Redeploying routine " ~ env_schema ~ "." ~ model_name, info=True) }}
      {%- do run_query(rendered_ddl) -%}
    {%- endif -%}

    {%- do run_query(dbt_virtual.upsert_environment_pointer_sql(
      phys_schema,
      env,
      model_name,
      resource_type,
      fp,
      phys_ident,
      target_release
    )) -%}
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
      {%- do run_query("DROP TABLE IF EXISTS " ~ drop_rel) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}
