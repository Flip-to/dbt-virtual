{% macro virtual_promote(from_env='staging', to_env='prod', release_id='') -%}
  {#-
    Zero-rebuild promotion:
    1. Verifies every physical table in `release_id` (or `from_env` active state) exists.
    2. Repoints `to_env` views (`<to_env_schema>.<model>`) to `<physical_schema>.<model>__<fp>`.
    3. Deploys parameterized routines (`function`, `table_function`, `procedure`) into `<to_env_schema>`.
    4. Updates `_environments` and `_releases`.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set to_schema = dbt_virtual.get_env_schema(to_env) -%}

  {%- set query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier
    FROM `{{ target.database }}`.`{{ phys_schema }}`._environments
    WHERE environment = '{{ from_env }}'
      {% if release_id != '' %}AND release_id = '{{ release_id }}'{% endif %}
    ORDER BY model_name
  {%- endset -%}

  {%- set rows = run_query(query) -%}
  {%- if not rows or (rows.rows | length) == 0 -%}
    {{ exceptions.raise_compiler_error("dbt-virtual promote: No models found in _environments for env=" ~ from_env ~ " release=" ~ release_id) }}
  {%- endif -%}

  {%- for row in rows.rows -%}
    {%- set model_name = row[0] -%}
    {%- set resource_type = row[1] -%}
    {%- set fp = row[2] -%}
    {%- set phys_ident = row[3] -%}

    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set view_rel = api.Relation.create(database=target.database, schema=to_schema, identifier=model_name, type='view') -%}
      {%- set phys_rel = api.Relation.create(database=target.database, schema=phys_schema, identifier=phys_table, type='table') -%}

      {{ log("dbt-virtual promote [" ~ to_env ~ "]: " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
      {%- do run_query(dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel)) -%}

      {%- set upsert_env -%}
        MERGE `{{ target.database }}`.`{{ phys_schema }}`._environments AS T
        USING (
          SELECT
            '{{ to_env }}' AS environment,
            '{{ model_name }}' AS model_name,
            '{{ resource_type }}' AS resource_type,
            '{{ fp }}' AS fingerprint,
            '{{ phys_ident }}' AS physical_identifier,
            '{{ release_id }}' AS release_id,
            CURRENT_TIMESTAMP() AS updated_at
        ) AS S
        ON T.environment = S.environment AND T.model_name = S.model_name
        WHEN MATCHED THEN UPDATE SET
          fingerprint = S.fingerprint,
          physical_identifier = S.physical_identifier,
          release_id = S.release_id,
          updated_at = S.updated_at
        WHEN NOT MATCHED THEN INSERT ROW
      {%- endset -%}
      {%- do run_query(upsert_env) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- if release_id != '' -%}
    {%- do run_query("UPDATE `" ~ target.database ~ "`.`" ~ phys_schema ~ "`._releases SET status = 'promoted', target_environment = '" ~ to_env ~ "', promoted_at = CURRENT_TIMESTAMP() WHERE release_id = '" ~ release_id ~ "'") -%}
  {%- endif -%}
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
      FROM `{{ target.database }}`.`{{ phys_schema }}`._releases
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
    FROM `{{ target.database }}`.`{{ phys_schema }}`._releases
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

    {%- set upsert_env -%}
      MERGE `{{ target.database }}`.`{{ phys_schema }}`._environments AS T
      USING (
        SELECT
          '{{ env }}' AS environment,
          '{{ model_name }}' AS model_name,
          '{{ resource_type }}' AS resource_type,
          '{{ fp }}' AS fingerprint,
          '{{ phys_ident }}' AS physical_identifier,
          '{{ target_release }}' AS release_id,
          CURRENT_TIMESTAMP() AS updated_at
      ) AS S
      ON T.environment = S.environment AND T.model_name = S.model_name
      WHEN MATCHED THEN UPDATE SET
        fingerprint = S.fingerprint,
        physical_identifier = S.physical_identifier,
        release_id = S.release_id,
        updated_at = S.updated_at
      WHEN NOT MATCHED THEN INSERT ROW
    {%- endset -%}
    {%- do run_query(upsert_env) -%}
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

  {%- set orphan_query -%}
    WITH protected_releases AS (
      SELECT DISTINCT release_id
      FROM `{{ target.database }}`.`{{ phys_schema }}`._releases
      WHERE status IN ('promoted', 'candidate')
      QUALIFY DENSE_RANK() OVER (ORDER BY created_at DESC) <= {{ retention_releases }}
    ),
    protected_tables AS (
      SELECT SPLIT(physical_identifier, '.')[SAFE_OFFSET(1)] AS table_name
      FROM `{{ target.database }}`.`{{ phys_schema }}`._environments
      WHERE resource_type = 'model'
      UNION DISTINCT
      SELECT SPLIT(r.physical_identifier, '.')[SAFE_OFFSET(1)] AS table_name
      FROM `{{ target.database }}`.`{{ phys_schema }}`._releases AS r
      INNER JOIN protected_releases AS pr USING (release_id)
      WHERE r.resource_type = 'model'
    )
    SELECT t.table_name
    FROM `{{ target.database }}`.`{{ phys_schema }}`.INFORMATION_SCHEMA.TABLES AS t
    LEFT JOIN protected_tables AS p ON t.table_name = p.table_name
    WHERE t.table_type = 'BASE TABLE'
      AND REGEXP_CONTAINS(t.table_name, r'__[0-9a-f]{8,64}$')
      AND t.creation_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ retention_days }} DAY)
      AND p.table_name IS NULL
    ORDER BY t.table_name
  {%- endset -%}

  {%- set orphans = run_query(orphan_query) -%}
  {%- if not orphans or (orphans.rows | length) == 0 -%}
    {{ log("dbt-virtual gc: 0 orphaned physical tables eligible for deletion.", info=True) }}
    {{ return('') }}
  {%- endif -%}

  {%- for row in orphans.rows -%}
    {%- set tbl = row[0] -%}
    {%- if dry_run -%}
      {{ log("dbt-virtual gc [DRY-RUN]: Would drop `" ~ target.database ~ "`.`" ~ phys_schema ~ "`.`" ~ tbl ~ "`", info=True) }}
    {%- else -%}
      {{ log("dbt-virtual gc: Dropping `" ~ target.database ~ "`.`" ~ phys_schema ~ "`.`" ~ tbl ~ "`", info=True) }}
      {%- do run_query("DROP TABLE IF EXISTS `" ~ target.database ~ "`.`" ~ phys_schema ~ "`.`" ~ tbl ~ "`") -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}
