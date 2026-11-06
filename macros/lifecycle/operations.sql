{% macro exec_sql_statement(sql_text, commit=true) -%}
  {#- `commit=false` leaves the statement in the open transaction (see `exec_atomic`). -#}
  {%- if statement is defined -%}
    {%- call statement('dbt_virtual_ddl_dml', fetch_result=False, auto_begin=True) -%}
      {{ sql_text }}
    {%- endcall -%}
    {%- if commit and adapter.commit is defined -%}
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


{% macro ensure_schema_exists(schema_name, location=none) -%}
  {#- Creates `schema_name` through the adapter (dataset location and permissions as dbt uses them).
      In a multi-location project the dataset is created in `location` explicitly. -#}
  {%- if adapter.check_schema_exists is defined and not adapter.check_schema_exists(dbt_virtual.active_database(), schema_name) -%}
    {%- if dbt_virtual.create_location_for(location) is not none -%}
      {%- do dbt_virtual.exec_sql_statement(dbt_virtual.create_schema_if_not_exists_sql(schema_name, dbt_virtual.create_location_for(location))) -%}
    {%- else -%}
      {%- do adapter.create_schema(api.Relation.create(database=dbt_virtual.active_database(), schema=schema_name)) -%}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}


{% macro exec_atomic(statements) -%}
  {#-
    DECISION(atomic_registry_upserts): runs `statements` (a list of SQL texts) in ONE transaction:
    committed together at the end, rolled back by dbt when one fails. Atomic where DML is
    transactional through dbt: PostgreSQL, DuckDB, Snowflake. Databricks and BigQuery run each
    statement as its own job (TECH_DEBT(registry_delete_insert_non_atomic)).
  -#}
  {%- for stmt in statements -%}
    {%- if (stmt | trim) != '' -%}
      {%- do dbt_virtual.exec_sql_statement(stmt, commit=false) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if statement is defined and adapter.commit is defined -%}
    {%- do adapter.commit() -%}
  {%- endif -%}
{%- endmacro %}


{% macro exec_ddl_dml(sql_text) -%}
  {#- A `;`-separated script runs in one transaction (`exec_atomic`), so DELETE + INSERT pairs do not half-apply. -#}
  {%- do dbt_virtual.exec_atomic(sql_text.split(';')) -%}
{%- endmacro %}


{% macro create_env_view(view_rel, phys_rel, node, phys_schema, authorized_schemas) -%}
  {#- Creates `view_rel` over `phys_rel`, then applies the node's metadata and authorization.
      A description may contain `;`, so a single statement must not go through `exec_ddl_dml`'s split. -#}
  {%- set view_sql = dbt_virtual.create_or_replace_view_sql(view_rel, phys_rel, node) -%}
  {%- if target.type == 'postgres' -%}
    {%- do dbt_virtual.exec_ddl_dml(view_sql) -%}
  {%- else -%}
    {%- do dbt_virtual.exec_sql_statement(view_sql) -%}
  {%- endif -%}
  {%- do dbt_virtual.finalize_env_view(view_rel, node, phys_schema, authorized_schemas) -%}
{%- endmacro %}


{% macro finalize_env_view(view_rel, node, phys_schema, authorized_schemas) -%}
  {#- After an env view is created: its metadata (column descriptions) and, with
      `authorize_env_views`, the env dataset's authorization on the physical dataset, once per
      env dataset (`authorized_schemas` is the caller's list; dataset updates are rate limited). -#}
  {%- do dbt_virtual.apply_env_view_metadata(view_rel, node) -%}
  {%- if dbt_virtual.get_config().get('authorize_env_views', false) and view_rel.schema not in authorized_schemas -%}
    {%- do dbt_virtual.authorize_env_dataset(view_rel.database, view_rel.schema, phys_schema) -%}
    {%- do authorized_schemas.append(view_rel.schema) -%}
  {%- endif -%}
{%- endmacro %}


{% macro virtual_acquire_lock(env='prod', owner='dbt-virtual', ttl_minutes=15, location=none) -%}
  {#-
    Lease lock on `env` in `<physical_schema>._locks`. DECISION(lease_lock_single_statement_acquire)
    (docs/decisions/20261105_lease_lock_single_statement_acquire.md).

    Protocol, two steps that each commit on their own:
    1. ONE conditional statement (`lock_upsert_sql`) writes this owner's lease only when the
       environment has no live lease of another owner (same-owner refresh and takeover of an expired
       lease are the same statement). The warehouse, not this macro, orders concurrent writers:
         DuckDB, PostgreSQL  `INSERT ... ON CONFLICT (environment) DO UPDATE ... WHERE` on a UNIQUE
                             index of `_locks(environment)`: at most one row per environment; a second
                             concurrent insert of the key waits (PostgreSQL) or fails with a unique
                             violation (DuckDB), so the loser never holds a lease.
         BigQuery            `MERGE`: mutating DML on one table is serialized, and a conflicting one is
                             retried by BigQuery, so the second MERGE sees the first one's row. (A plain
                             INSERT ... WHERE NOT EXISTS would NOT do: BigQuery runs INSERTs on one
                             table concurrently without conflict.)
         Snowflake           `MERGE`: takes the table's DML lock, the second waits, then sees the row.
                             No constraint backs it (Snowflake does not enforce keys):
                             TECH_DEBT(snowflake_lock_not_constraint_enforced).
         Databricks          `MERGE`: Delta conflict detection fails the later concurrent writer.
    2. Read the live leases back (after the commit of step 1). This runner holds the lock only if NO
       live lease of another owner is visible. This is the backstop for a warehouse that did not
       serialize step 1: each runner reads after its own commit, so of two runners that both wrote, the
       one that reads second sees the other's committed row; at most one can see "only me". Both may
       see the other and both yield (the caller retries), but two never win. The old protocol ordered
       the leases by their start time, which a reader cannot decide while the earlier row is not yet
       visible to it: two runners won (tests/test_registry_concurrency_duckdb.py, "2 runners hold the lock").
    A loser removes its own lease row (if it wrote one) and fails with "lock conflict". On DuckDB a
    runner that loses the race at step 1 fails with the unique-violation error instead (nothing to remove).
  -#}
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do dbt_virtual.virtual_acquire_lock(env=env, owner=owner, ttl_minutes=ttl_minutes, location=loc) -%}
    {%- endfor -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {%- set locks_rel = dbt_virtual.registry_relation(phys_schema, '_locks') -%}

  {%- do dbt_virtual.ensure_registry(location) -%}
  {%- do dbt_virtual.ensure_locks_unique_key(phys_schema, locks_rel) -%}

  {%- do dbt_virtual.exec_sql_statement(dbt_virtual.lock_upsert_sql(locks_rel, env, owner, ttl_minutes)) -%}

  {%- set live_query -%}
    SELECT owner, expires_at
    FROM {{ locks_rel }}
    WHERE environment = '{{ env }}'
      AND expires_at > {{ dbt_virtual.current_timestamp_sql() }}
    ORDER BY acquired_at ASC, owner ASC
  {%- endset -%}
  {%- set live = run_query(live_query) -%}
  {%- set ns = namespace(other=none, mine=false) -%}
  {%- if live -%}
    {%- for row in live.rows -%}
      {%- if row[0] == owner -%}
        {%- set ns.mine = true -%}
      {%- elif ns.other is none -%}
        {%- set ns.other = row -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- if ns.other is not none -%}
    {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ locks_rel ~ " WHERE environment = '" ~ env ~ "' AND owner = '" ~ owner ~ "'") -%}
    {{ exceptions.raise_compiler_error("dbt-virtual lock conflict: Environment '" ~ env ~ "' is currently locked by '" ~ ns.other[0] ~ "' (expires at " ~ ns.other[1] ~ ").") }}
  {%- endif -%}
  {%- if not ns.mine -%}
    {{ exceptions.raise_compiler_error("dbt-virtual lock conflict: Environment '" ~ env ~ "': the lease of '" ~ owner ~ "' is not live after the acquire statement (another runner holds or took over the lock); retry.") }}
  {%- endif -%}
  {{ log("dbt-virtual lock [" ~ env ~ "]: Acquired lock for owner='" ~ owner ~ "' (ttl=" ~ ttl_minutes ~ "m)", info=True) }}
{%- endmacro %}


{% macro virtual_release_lock(env='prod', owner='dbt-virtual', location=none) -%}
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do dbt_virtual.virtual_release_lock(env=env, owner=owner, location=loc) -%}
    {%- endfor -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {%- set locks_rel = dbt_virtual.registry_relation(phys_schema, '_locks') -%}
  {%- do dbt_virtual.exec_ddl_dml("DELETE FROM " ~ locks_rel ~ " WHERE environment = '" ~ env ~ "' AND owner = '" ~ owner ~ "'") -%}
  {{ log("dbt-virtual lock [" ~ env ~ "]: Released lock for owner='" ~ owner ~ "'", info=True) }}
{%- endmacro %}


{% macro fail_releasing_lock(lock, env, owner, location, message) -%}
  {#- Raises `message` after releasing this run's lease. A failed promote / rollback used to keep its
      lease until the TTL expired, so the retry needed `--no-lock`. Errors raised by the warehouse
      (not by this package) are covered by the CLI, which releases the lock when the operation fails. -#}
  {%- if lock -%}
    {%- do dbt_virtual.virtual_release_lock(env=env, owner=owner, location=location) -%}
  {%- endif -%}
  {{ exceptions.raise_compiler_error(message) }}
{%- endmacro %}


{% macro virtual_audit(from_env='staging', to_env='prod', release_id='', allow_empty=false, max_row_drop_pct=100.0, location=none, check_coverage=true, coverage_ignore_before='', source_database='', source_schema='') -%}
  {#-
    SQLMesh-style pre-promotion quality gate:
    1. Resolves candidate `model` tables for `release_id` (or `from_env`).
    2. Compares candidate table row counts against active `to_env` (`prod`) physical tables.
    3. Fails fast via `exceptions.raise_compiler_error` if:
       - A candidate table has 0 rows while `to_env` has > 0 rows (unless `allow_empty=true`).
       - Candidate row count drops by more than `max_row_drop_pct` percent compared to `to_env`.
       - (BigQuery, `check_coverage=true`) The candidate lacks partitions the `to_env` table has, ignoring
         `__NULL__` / `__UNPARTITIONED__` and partitions older than `coverage_ignore_before`
         (argument or `vars.dbt_virtual.coverage_ignore_before`, e.g. '2026-01-01'). Other adapters skip it.
  -#}
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do dbt_virtual.virtual_audit(from_env=from_env, to_env=to_env, release_id=release_id, allow_empty=allow_empty, max_row_drop_pct=max_row_drop_pct, location=loc, source_database=source_database, source_schema=source_schema) -%}
    {%- endfor -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {#- Candidates may live in another project (see `promotion_source`); the audited `to_env` is here. -#}
  {%- set source = dbt_virtual.promotion_source(from_env, to_env, release_id, source_database, source_schema, phys_schema) -%}
  {%- set src_schema = source['schema'] -%}
  {%- set src_db = source['database'] -%}
  {%- set ignore_before = coverage_ignore_before if coverage_ignore_before != '' else (cfg.get('coverage_ignore_before') or '') -%}
  {%- set coverage_skip_logged = namespace(value=false) -%}

  {%- if release_id != '' -%}
    {%- set cand_query -%}
      SELECT model_name, physical_identifier
      FROM {{ dbt_virtual.registry_relation(src_schema, '_releases', src_db) }}
      WHERE release_id = '{{ release_id }}'
        AND resource_type = 'model'
      ORDER BY model_name ASC
    {%- endset -%}
  {%- else -%}
    {%- set cand_query -%}
      SELECT model_name, physical_identifier
      FROM {{ dbt_virtual.registry_relation(src_schema, '_environments', src_db) }}
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

  {#- `to_env` is read from its own location (it differs from this run's under `physical_locations`). -#}
  {%- set base_loc = dbt_virtual.registry_location_for(to_env, phys_schema) -%}
  {%- set base_query -%}
    SELECT model_name, physical_identifier
    FROM {{ dbt_virtual.registry_relation(base_loc['schema'], '_environments', base_loc['database']) }}
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
    {%- set cand_rel = api.Relation.create(database=src_db, schema=src_schema, identifier=cand_tbl, type='table') -%}
    {%- set cand_cnt_res = run_query(dbt_virtual.row_count_sql(cand_rel)) -%}
    {%- set cand_cnt = cand_cnt_res.rows[0][0] | int if (cand_cnt_res and (cand_cnt_res.rows | length) > 0) else 0 -%}

    {%- set base_ident = base_map.get(model_name, '') -%}
    {%- if base_ident != '' -%}
      {%- set base_tbl = base_ident.split('.')[-1] -%}
      {%- set base_rel = api.Relation.create(database=base_loc['database'], schema=base_loc['schema'], identifier=base_tbl, type='table') -%}
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
      {%- if check_coverage and cand_tbl != base_tbl -%}
        {%- set coverage_sql = dbt_virtual.partition_coverage_sql(cand_rel, base_rel) -%}
        {%- if (coverage_sql | trim) == '' -%}
          {%- if not coverage_skip_logged.value -%}
            {{ log("dbt-virtual audit: partition coverage check skipped (only implemented for BigQuery).", info=True) }}
            {%- set coverage_skip_logged.value = true -%}
          {%- endif -%}
        {%- else -%}
          {%- set coverage_res = run_query(coverage_sql) -%}
          {%- set missing_ids = [] -%}
          {%- if coverage_res -%}
            {%- for crow in coverage_res.rows -%}
              {%- if not dbt_virtual.partition_is_before(crow[0], ignore_before) -%}
                {%- do missing_ids.append(crow[0] | string) -%}
              {%- endif -%}
            {%- endfor -%}
          {%- endif -%}
          {%- if missing_ids | length > 0 -%}
            {{ exceptions.raise_compiler_error(
              "dbt-virtual audit failed for model '" ~ model_name ~ "': candidate table " ~ cand_rel ~ " lacks "
              ~ (missing_ids | length) ~ " partition(s) that " ~ to_env ~ " has: "
              ~ (dbt_virtual.partition_ranges(missing_ids | sort) | join(', '))
              ~ (" (partitions before " ~ ignore_before ~ " ignored)" if ignore_before != '' else '')
              ~ ". A windowed or failed build is incomplete."
            ) }}
          {%- endif -%}
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


{% macro virtual_print_base_environment() -%}
  {#- Logs the rendered `base_environment` (it may be Jinja, e.g. on `target.name`), so the CLI can
      decide whether `promote --to <env>` audits by default. -#}
  {{ log("DBT_VIRTUAL_BASE_ENV:" ~ dbt_virtual.get_config().get('base_environment', 'prod'), info=True) }}
{%- endmacro %}


{% macro virtual_export_state(env='staging', base_env='prod') -%}
  {#-
    Live warehouse state introspection:
    Queries `_environments`, `_intervals`, and physical fingerprint tables, then logs a
    machine-readable `DBT_VIRTUAL_STATE_JSON:{...}` line that `dbt-virtual status` / `plan` can consume.
    With `physical_schemas_by_location` the state of every location's registry is merged.
  -#}
  {%- set parts = [] -%}
  {%- for location in dbt_virtual.physical_locations() -%}
    {%- do parts.append(dbt_virtual.export_state_payload(env, base_env, dbt_virtual.physical_schema_for(location))) -%}
  {%- endfor -%}
  {%- set payload = parts[0] -%}
  {%- for part in parts[1:] -%}
    {%- do payload['env_state'].update(part['env_state']) -%}
    {%- do payload['base_env_state'].update(part['base_env_state']) -%}
    {%- do payload['existing_physical_tables'].extend(part['existing_physical_tables']) -%}
    {%- do payload['registered_physical_tables'].extend(part['registered_physical_tables']) -%}
    {%- do payload['physical_schemas'].extend(part['physical_schemas']) -%}
    {%- if 'partial_physical_tables' in part -%}
      {%- do payload['partial_physical_tables'].extend(part['partial_physical_tables']) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- do payload.update({'registered_physical_tables': payload['registered_physical_tables'] | unique | list | sort}) -%}
  {{ log("DBT_VIRTUAL_STATE_JSON:" ~ tojson(payload), info=True) }}
{%- endmacro %}


{% macro export_state_payload(env, base_env, phys_schema) -%}
  {#- The state of one registry (see `virtual_export_state`). -#}

  {%- set env_query -%}
    SELECT environment, model_name, fingerprint
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment IN ('{{ env }}', '{{ base_env }}')
  {%- endset -%}
  {#- A project that has never built or promoted has no registry (and maybe no physical schema):
      report empty state instead of failing on the missing relation. -#}
  {%- set schema_exists = adapter.check_schema_exists(dbt_virtual.active_database(), phys_schema) -%}
  {%- set registry_exists = schema_exists and adapter.get_relation(database=dbt_virtual.active_database(), schema=phys_schema, identifier='_environments') is not none
      and adapter.get_relation(database=dbt_virtual.active_database(), schema=phys_schema, identifier='_releases') is not none -%}
  {#- A registry written by an older release lacks `build_quality` / `schema_key`; add them first. -#}
  {%- if registry_exists -%}
    {%- do dbt_virtual.migrate_registry_if_old(phys_schema=phys_schema) -%}
  {%- endif -%}
  {%- set env_rows = run_query(env_query) if registry_exists else none -%}

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

  {%- set tbl_rows = run_query(dbt_virtual.list_physical_tables_sql(phys_schema)) if schema_exists else none -%}
  {%- set existing_tables = [] -%}
  {%- if tbl_rows -%}
    {%- for r in tbl_rows.rows -%}
      {%- do existing_tables.append(r[0]) -%}
    {%- endfor -%}
  {%- endif -%}

  {#- Physical tables some environment or release points at, i.e. written by a successful build.
      A table that exists but is not registered (e.g. a clone whose build failed) must not be reused. -#}
  {%- set registered_query -%}
    SELECT DISTINCT physical_identifier FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }} WHERE resource_type = 'model'
    UNION DISTINCT
    SELECT DISTINCT physical_identifier FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} WHERE resource_type = 'model'
  {%- endset -%}
  {%- set registered_rows = run_query(registered_query | replace('UNION DISTINCT', 'UNION') if target.type != 'bigquery' else registered_query) if registry_exists else none -%}
  {%- set registered_tables = [] -%}
  {%- if registered_rows -%}
    {%- for r in registered_rows.rows -%}
      {%- do registered_tables.append(r[0].split('.')[-1]) -%}
    {%- endfor -%}
  {%- endif -%}

  {#- Tables whose latest registration is a windowed / `--empty` build: never reuse or clone them. -#}
  {%- set quality_query -%}
    SELECT physical_identifier, build_quality, ts FROM (
      SELECT physical_identifier, build_quality, updated_at AS ts FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }} WHERE resource_type = 'model'
      UNION ALL
      SELECT physical_identifier, build_quality, created_at AS ts FROM {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} WHERE resource_type = 'model'
    ) AS q
    ORDER BY ts ASC
  {%- endset -%}
  {%- set quality_rows = run_query(quality_query) if registry_exists else none -%}
  {%- set latest_quality = {} -%}
  {%- if quality_rows -%}
    {%- for r in quality_rows.rows -%}
      {%- do latest_quality.update({r[0].split('.')[-1]: r[1]}) -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set partial_tables = [] -%}
  {%- for tbl, quality in latest_quality.items() -%}
    {%- if quality == 'partial' -%}
      {%- do partial_tables.append(tbl) -%}
    {%- endif -%}
  {%- endfor -%}

  {#- DECISION(cross_project_promote_by_clone): the base environment's pointers and tables are read
      from its own location when `physical_locations` puts it elsewhere, so a CI run sees prod. -#}
  {%- set base_loc = dbt_virtual.seed_base_location(base_env, phys_schema) -%}
  {%- set base_tables = [] -%}
  {%- if base_loc['cross'] and adapter.get_relation(database=base_loc['database'], schema=base_loc['schema'], identifier='_environments') is not none -%}
    {%- set base_cross_rows = run_query("SELECT model_name, fingerprint FROM " ~ dbt_virtual.registry_relation(base_loc['schema'], '_environments', base_loc['database']) ~ " WHERE environment = '" ~ base_env ~ "' AND resource_type = 'model'") -%}
    {%- if base_cross_rows -%}
      {%- for r in base_cross_rows.rows -%}
        {%- do base_env_state.update({r[0]: r[1]}) -%}
      {%- endfor -%}
    {%- endif -%}
    {%- set base_tbl_rows = run_query(dbt_virtual.list_physical_tables_sql(base_loc['schema'], base_loc['database'])) -%}
    {%- if base_tbl_rows -%}
      {%- for r in base_tbl_rows.rows -%}
        {%- do base_tables.append(r[0]) -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
  {%- set payload = {
    'base_physical_tables': base_tables,
    'partial_physical_tables': partial_tables | sort,
    'registered_physical_tables': registered_tables | unique | list | sort,
    'env': env,
    'base_env': base_env,
    'physical_schema': phys_schema,
    'physical_schemas': [phys_schema],
    'env_state': env_state,
    'base_env_state': base_env_state,
    'existing_physical_tables': existing_tables
  } -%}
  {{ return(payload) }}
{%- endmacro %}


{% macro routine_depths() -%}
  {#- Depth of each virtual node in the project's DAG (0 = no virtual parents), used to deploy a
      routine after the routines and views it reads instead of alphabetically within its tier. -#}
  {%- set uid_names = {} -%}
  {%- set parents = {} -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for uid, node in graph.nodes.items() -%}
      {%- if dbt_virtual.is_virtual_node(node) -%}
        {%- do uid_names.update({uid: dbt_virtual.node_attr(node, 'name', '')}) -%}
      {%- endif -%}
    {%- endfor -%}
    {%- for uid, node in graph.nodes.items() -%}
      {%- if uid in uid_names -%}
        {%- set names = [] -%}
        {%- for parent_uid in (dbt_virtual.node_attr(dbt_virtual.node_attr(node, 'depends_on', {}), 'nodes', []) or []) -%}
          {%- if parent_uid in uid_names -%}
            {%- do names.append(uid_names[parent_uid]) -%}
          {%- endif -%}
        {%- endfor -%}
        {%- do parents.update({uid_names[uid]: names}) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set depths = {} -%}
  {%- for _ in range(16) -%}
    {%- for name, names in parents.items() -%}
      {%- set depth = namespace(value=0) -%}
      {%- for parent in names -%}
        {%- if depths.get(parent, 0) + 1 > depth.value -%}
          {%- set depth.value = depths.get(parent, 0) + 1 -%}
        {%- endif -%}
      {%- endfor -%}
      {%- do depths.update({name: depth.value}) -%}
    {%- endfor -%}
  {%- endfor -%}
  {{ return(depths) }}
{%- endmacro %}


{% macro ordered_registry_rows(rows, schema_key_index) -%}
  {#- Registry rows as dicts in deployment order: environment views of tables first (they are
      `SELECT *` over physical tables), then virtual views and routines together by DAG depth (a
      view can read another view or a UDF, a routine can read a view), then UDFs before TVFs before
      procedures within a depth. -#}
  {%- set depths = dbt_virtual.routine_depths() -%}
  {%- set items = [] -%}
  {%- for row in rows -%}
    {%- do items.append({
      'model_name': row[0],
      'resource_type': row[1],
      'fingerprint': row[2],
      'physical_identifier': row[3],
      'routine_ddl': row[4],
      'schema_key': dbt_virtual.row_value(row, schema_key_index),
      'build_quality': dbt_virtual.row_value(row, 7),
      'sealed_at': dbt_virtual.row_value(row, 8),
      'group': 0 if row[1] == 'model' else 1,
      'depth': depths.get(row[0], 0),
      'rank': {'view': 0, 'function': 1, 'virtual_function': 1, 'table_function': 2}.get(row[1], 3)
    }) -%}
  {%- endfor -%}
  {{ return(items | sort(attribute='model_name') | sort(attribute='rank') | sort(attribute='depth') | sort(attribute='group')) }}
{%- endmacro %}


{% macro virtual_promote(from_env='staging', to_env='prod', release_id='', audit=false, max_row_drop_pct=100.0, lock=false, lock_owner='', location=none, skip_empty=false, expected_fps_json='', allow_unsealed=false, require_sealed=false, check_coverage=true, coverage_ignore_before='', data_diff=false, data_diff_max_mismatch_pct=none, source_database='', source_schema='') -%}
  {%- do dbt_virtual.warn_if_experimental_adapter() -%}
  {#-
    Zero-rebuild promotion:
    -1. Cross-location (`physical_locations`, or `source_database` / `source_schema`): when the release
       lives in another project or schema than `to_env`'s location, each model table is CLONEd into
       the target location first (skipped when present), the release rows are copied into the target
       registry, and the target views point at the clones. DECISION(cross_project_promote_by_clone).
    0. Guards (guards.sql), checked before anything is changed; the macro raises listing every problem:
       - `expected_fps_json` (`{model: fingerprint}` of the `to_env` code; the CLI always passes it): a
         release row with another fingerprint was built from different code or variables.
       - rows marked `build_quality = 'partial'` (windowed or `--empty` build) are refused.
       - with `release_id`: the release must be sealed (`dbt-virtual build --release` seals it after a
         successful `dbt build`) unless `allow_unsealed=true`. Releases from before sealing existed carry
         no seal and no build_quality; they are refused only with `require_sealed=true` (the CLI default).
    1. Optionally runs `virtual_audit` quality gate when `audit=true`, and `virtual_data_diff`
       (row-level, gated by `data_diff_max_mismatch_pct`) when `data_diff=true`.
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
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {#- One job per location: each registry holds the objects of its own location. -#}
    {%- set outcomes = [] -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do outcomes.append(dbt_virtual.virtual_promote(from_env=from_env, to_env=to_env, release_id=release_id, audit=audit, max_row_drop_pct=max_row_drop_pct, lock=lock, lock_owner=lock_owner, location=loc, skip_empty=true) | trim) -%}
    {%- endfor -%}
    {%- if (outcomes | select('equalto', 'skipped') | list | length) == (outcomes | length) -%}
      {{ exceptions.raise_compiler_error("dbt-virtual promote: nothing found in any location (" ~ (dbt_virtual.physical_locations() | join(', ')) ~ ") for env=" ~ from_env ~ " release=" ~ release_id) }}
    {%- endif -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {%- set ensured_schemas = [] -%}
  {%- set source = dbt_virtual.promotion_source(from_env, to_env, release_id, source_database, source_schema, phys_schema) -%}
  {%- set cross = source['cross'] -%}
  {%- set src_schema = source['schema'] -%}
  {%- set src_db = source['database'] -%}
  {%- set cross_release_values = [] -%}
  {%- do dbt_virtual.ensure_registry() -%}
  {%- set authorized_schemas = [] -%}
  {%- set owner = lock_owner if lock_owner != '' else ('dbt-virtual:' ~ (invocation_id if invocation_id is defined else 'run')) -%}
  {%- if lock -%}
    {%- do dbt_virtual.virtual_acquire_lock(env=to_env, owner=owner, location=location) -%}
  {%- endif -%}

  {%- if audit -%}
    {%- do dbt_virtual.virtual_audit(from_env=from_env, to_env=to_env, release_id=release_id, allow_empty=false, max_row_drop_pct=max_row_drop_pct, location=location, check_coverage=check_coverage, coverage_ignore_before=coverage_ignore_before, source_database=source_database, source_schema=source_schema) -%}
  {%- endif -%}

  {%- if data_diff -%}
    {#- Row-level diff against `to_env`; fails above `data_diff_max_mismatch_pct` (report only when none). -#}
    {%- do dbt_virtual.virtual_data_diff(from_env=from_env, to_env=to_env, release_id=release_id, max_mismatch_pct=data_diff_max_mismatch_pct, source_database=(src_db if cross else ''), source_schema=(src_schema if cross else '')) -%}
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
        schema_key,
        build_quality,
        sealed_at
      FROM {{ dbt_virtual.registry_relation(src_schema, '_releases', src_db) }}
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
        e.schema_key,
        e.build_quality
      FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }} AS e
      WHERE e.environment = '{{ from_env }}'
      ORDER BY CASE e.resource_type WHEN 'model' THEN 1 WHEN 'function' THEN 2 WHEN 'table_function' THEN 3 ELSE 4 END ASC, e.model_name ASC
    {%- endset -%}
  {%- endif -%}

  {%- set rows = run_query(query) -%}
  {%- if not rows or (rows.rows | length) == 0 -%}
    {%- if skip_empty -%}
      {{ log("dbt-virtual promote [" ~ to_env ~ "]: nothing to promote in " ~ phys_schema ~ " (location " ~ location ~ ").", info=True) }}
      {%- if lock -%}
        {%- do dbt_virtual.virtual_release_lock(env=to_env, owner=owner, location=location) -%}
      {%- endif -%}
      {{ return('skipped') }}
    {%- endif -%}
    {{ dbt_virtual.fail_releasing_lock(lock, to_env, owner, location, "dbt-virtual promote: No models found for env=" ~ from_env ~ " release=" ~ release_id) }}
  {%- endif -%}

  {%- set guard_problems = dbt_virtual.promotion_guard_problems(
    rows.rows, to_env, release_id, expected_fps_json, allow_unsealed, require_sealed, 7, 8 if release_id != '' else none
  ) -%}
  {%- if guard_problems | length > 0 -%}
    {{ dbt_virtual.fail_releasing_lock(lock, to_env, owner, location, "dbt-virtual promote refused (nothing was changed): " ~ (guard_problems | join(' | '))) }}
  {%- endif -%}

  {%- set ordered = dbt_virtual.ordered_registry_rows(rows.rows, 6) -%}

  {#- Phase 1: Pre-flight verification that every referenced physical table exists -#}
  {%- for item in ordered -%}
    {%- set model_name = item['model_name'] -%}
    {%- set resource_type = item['resource_type'] -%}
    {%- set phys_ident = item['physical_identifier'] -%}
    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set exists_rel = adapter.get_relation(database=src_db, schema=src_schema, identifier=phys_table) -%}
      {%- if exists_rel is none -%}
        {{ dbt_virtual.fail_releasing_lock(lock, to_env, owner, location, "dbt-virtual promote pre-flight failed: physical table " ~ src_db ~ "." ~ src_schema ~ "." ~ phys_table ~ " for model " ~ model_name ~ " does not exist.") }}
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
  {%- for item in ordered -%}
    {%- set model_name = item['model_name'] -%}
    {%- set resource_type = item['resource_type'] -%}
    {%- set fp = item['fingerprint'] -%}
    {%- set phys_ident = item['physical_identifier'] -%}
    {%- set routine_ddl = item['routine_ddl'] -%}
    {%- set schema_key = item['schema_key'] -%}
    {%- set to_schema = dbt_virtual.env_schema_for(to_env, schema_key) -%}
    {%- do promoted_names.append(model_name) -%}

    {%- if resource_type == 'model' -%}
      {%- set target_phys_ident = (phys_schema ~ '.' ~ phys_ident.split('.')[-1]) if cross else phys_ident -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set target_phys_ident = to_schema ~ '.' ~ model_name -%}
    {%- else -%}
      {%- set target_phys_ident = phys_ident -%}
    {%- endif -%}

    {%- set unchanged = existing_fp_map.get(model_name, '') == fp and existing_ident_map.get(model_name, '') == target_phys_ident -%}
    {#- A table still at the environment path (kept by `adopt --keep-existing-table`) is not the view
        the pointer says: replace it, or a later rollback to an adopt release cannot create the view. -#}
    {%- set legacy_table = false -%}
    {%- if unchanged and resource_type == 'model' -%}
      {%- set legacy_rel = adapter.get_relation(database=dbt_virtual.active_database(), schema=to_schema, identifier=model_name) -%}
      {%- set legacy_table = legacy_rel is not none and legacy_rel.type is defined and legacy_rel.type == 'table' -%}
    {%- endif -%}
    {%- if unchanged and not legacy_table -%}
      {{ log("dbt-virtual promote [" ~ to_env ~ "]: Skipping unchanged " ~ model_name ~ " (fp=" ~ fp ~ " already promoted)", info=True) }}
    {%- else -%}
      {%- if to_schema not in ensured_schemas -%}
        {%- do dbt_virtual.ensure_schema_exists(to_schema, location) -%}
        {%- do ensured_schemas.append(to_schema) -%}
      {%- endif -%}
      {%- if resource_type == 'model' -%}
        {%- set phys_table = phys_ident.split('.')[-1] -%}
        {%- set existing_target_rel = adapter.get_relation(database=dbt_virtual.active_database(), schema=to_schema, identifier=model_name) -%}
        {%- if existing_target_rel is not none and existing_target_rel.type is defined and existing_target_rel.type == 'table' -%}
          {{ log("dbt-virtual promote [" ~ to_env ~ "]: Dropping legacy physical table " ~ existing_target_rel ~ " before creating virtual view", info=True) }}
          {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(existing_target_rel, 'table')) -%}
        {%- endif -%}
        {%- set view_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=to_schema, identifier=model_name, type='view') -%}
        {%- set phys_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=phys_schema, identifier=phys_table, type='table') -%}
        {%- if cross -%}
          {%- do dbt_virtual.clone_into_location(src_db, src_schema, phys_rel) -%}
        {%- endif -%}

        {{ log("dbt-virtual promote [" ~ to_env ~ "]: " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
        {%- set view_node = dbt_virtual.virtual_node_by_name(model_name) -%}
        {%- do dbt_virtual.create_env_view(view_rel, phys_rel, view_node, phys_schema, authorized_schemas) -%}
      {%- elif routine_ddl is not none and routine_ddl != '' -%}
        {#- A cross-location template names the source project; deploy into the target project instead. -#}
        {%- set rendered_ddl = dbt_virtual.resolve_env_placeholders_sql(dbt_virtual.retarget_project_in_ddl(routine_ddl, src_db if cross else ''), to_env) -%}
        {{ log("dbt-virtual promote [" ~ to_env ~ "]: Deploying " ~ resource_type ~ " " ~ to_schema ~ "." ~ model_name, info=True) }}
        {%- do dbt_virtual.exec_sql_statement(rendered_ddl) -%}
        {%- if resource_type != 'view' -%}
          {#- The check used to run without looking at its result. -#}
          {%- set routine_check = run_query(dbt_virtual.validate_routine_exists_sql(to_schema, model_name)) -%}
          {%- if routine_check is not none and routine_check.rows is defined and (routine_check.rows | length) == 0 -%}
            {{ exceptions.raise_compiler_error("dbt-virtual promote [" ~ to_env ~ "]: routine " ~ to_schema ~ "." ~ model_name ~ " was not found after deployment.") }}
          {%- endif -%}
        {%- endif -%}
      {%- endif -%}
    {%- endif -%}

    {%- if resource_type == 'model' and not unchanged -%}
      {#- Save the state the environment holds for the table it is leaving, before it is replaced. -#}
      {%- do dbt_virtual.companion_depart(dbt_virtual.virtual_node_by_name(model_name), to_env, fp) -%}
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
    {%- if resource_type == 'model' and not unchanged -%}
      {#- The promoted table and the environment's companion state (e.g. an incremental manifest)
          must describe the same data. A cross-location release is registered in the source
          registry, where its holders and saved rows are looked up. -#}
      {%- if cross -%}
        {%- do dbt_virtual.restore_companion_state(model_name, fp, to_env, prefer_env=from_env, source_database=src_db, source_schema=src_schema) -%}
      {%- else -%}
        {%- do dbt_virtual.restore_companion_state(model_name, fp, to_env, prefer_env=from_env) -%}
      {%- endif -%}
    {%- endif -%}
    {%- if cross -%}
      {%- do cross_release_values.append(dbt_virtual.promoted_release_values_sql(
        release_id, from_env, to_env, item, target_phys_ident, src_db
      )) -%}
    {%- endif -%}
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
      {%- set demote_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=demote_schema, identifier=old_name, type='view') -%}
      {{ log("dbt-virtual demote [" ~ to_env ~ "]: Removing demoted " ~ old_type ~ " " ~ demote_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(demote_rel, old_type)) -%}
      {%- do dbt_virtual.exec_ddl_dml(
        "DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
        ~ " WHERE environment = '" ~ to_env ~ "' AND model_name = '" ~ old_name ~ "'"
      ) -%}
    {%- endif -%}
  {%- endfor -%}

  {#- A MERGE keeps the old `build_quality` of a pointer it updates; everything promoted here passed the guard. -#}
  {%- if promoted_names | length > 0 -%}
    {%- set promoted_literals = [] -%}
    {%- for promoted_name in promoted_names -%}
      {%- do promoted_literals.append("'" ~ promoted_name ~ "'") -%}
    {%- endfor -%}
    {%- do dbt_virtual.exec_ddl_dml(
      "UPDATE " ~ dbt_virtual.registry_relation(phys_schema, '_environments') ~ " SET build_quality = 'full'"
      ~ " WHERE environment = '" ~ to_env ~ "' AND build_quality = 'partial' AND model_name IN (" ~ (promoted_literals | join(', ')) ~ ")"
    ) -%}
  {%- endif -%}

  {%- if cross and release_id != '' -%}
    {#- The source registry belongs to another service account: record the release in the target registry. -#}
    {%- set releases_rel = dbt_virtual.registry_relation(phys_schema, '_releases') -%}
    {%- set cross_statements = ["DELETE FROM " ~ releases_rel ~ " WHERE release_id = '" ~ release_id ~ "'"] -%}
    {%- for batch in cross_release_values | batch(100) -%}
      {%- do cross_statements.append(
        "INSERT INTO " ~ releases_rel ~ " (release_id, source_environment, target_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status, created_at, promoted_at, rolled_back_at, schema_key, build_quality, sealed_at) VALUES "
        ~ (batch | join(', '))
      ) -%}
    {%- endfor -%}
    {%- do dbt_virtual.exec_atomic(cross_statements) -%}
  {%- elif release_id != '' -%}
    {%- do dbt_virtual.exec_ddl_dml("UPDATE " ~ dbt_virtual.registry_relation(phys_schema, '_releases') ~ " SET status = 'promoted', target_environment = '" ~ to_env ~ "', promoted_at = " ~ dbt_virtual.current_timestamp_sql() ~ " WHERE release_id = '" ~ release_id ~ "'") -%}
  {%- endif -%}
  {%- if lock -%}
    {%- do dbt_virtual.virtual_release_lock(env=to_env, owner=owner, location=location) -%}
  {%- endif -%}
{%- endmacro %}


{% macro promotion_source(from_env, to_env, release_id, source_database, source_schema, phys_schema) -%}
  {#-
    DECISION(cross_project_promote_by_clone): where the release or `from_env` lives, and whether
    that differs from the target location. Without `physical_locations` and without an explicit
    `source_database` / `source_schema` nothing is cross-location (previous behaviour).
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set here_db = dbt_virtual.active_database() -%}
  {%- if not (cfg.get('physical_locations') or {}) and source_database == '' and source_schema == '' -%}
    {{ return({'cross': false, 'database': here_db, 'schema': phys_schema}) }}
  {%- endif -%}
  {%- set src = dbt_virtual.physical_location(from_env) -%}
  {%- set src = {'database': source_database or src['database'], 'schema': source_schema or src['schema']} -%}
  {%- set here = {'database': here_db, 'schema': phys_schema} -%}
  {%- if dbt_virtual.same_location(src, here) -%}
    {{ return({'cross': false, 'database': here_db, 'schema': phys_schema}) }}
  {%- endif -%}
  {%- if dbt_virtual.is_multi_location() -%}
    {{ exceptions.raise_compiler_error("dbt-virtual promote: cross-project promotion cannot be combined with physical_schemas_by_location.") }}
  {%- endif -%}
  {%- if release_id == '' -%}
    {{ exceptions.raise_compiler_error("dbt-virtual promote: env=" ~ from_env ~ " lives in " ~ src['database'] ~ "." ~ src['schema'] ~ ", not in " ~ here_db ~ "." ~ phys_schema ~ ". Cross-location promotion needs a release (clones are registered from its rows); `init-env` across locations is not supported.") }}
  {%- endif -%}
  {%- set to_loc = dbt_virtual.physical_location(to_env) -%}
  {%- if not dbt_virtual.same_location(to_loc, here) -%}
    {{ exceptions.raise_compiler_error("dbt-virtual promote: env=" ~ to_env ~ " lives in " ~ to_loc['database'] ~ "." ~ to_loc['schema'] ~ " but this run's environment (target " ~ target.name ~ ") uses " ~ here_db ~ "." ~ phys_schema ~ ". Run promote with the target of the environment you promote to (`dbt-virtual --target " ~ to_env ~ " promote ...`).") }}
  {%- endif -%}
  {{ return({'cross': true, 'database': src['database'], 'schema': src['schema']}) }}
{%- endmacro %}


{% macro clone_into_location(source_database, source_schema, target_relation) -%}
  {#- Zero-copy clone of `<source_database>.<source_schema>.<table>` to `target_relation`, if absent. -#}
  {%- if adapter.get_relation(database=target_relation.database, schema=target_relation.schema, identifier=target_relation.identifier) is none -%}
    {%- do dbt_virtual.ensure_schema_exists(target_relation.schema) -%}
    {%- set source_rel = api.Relation.create(database=source_database, schema=source_schema, identifier=target_relation.identifier, type='table') -%}
    {{ log("dbt-virtual promote: cloning " ~ source_rel ~ " -> " ~ target_relation, info=True) }}
    {%- do dbt_virtual.clone_or_copy_table(source_rel, target_relation) -%}
    {%- if adapter.cache_added is defined -%}
      {%- do adapter.cache_added(target_relation) -%}
    {%- endif -%}
  {%- else -%}
    {{ log("dbt-virtual promote: " ~ target_relation ~ " already exists; not cloning.", info=True) }}
  {%- endif -%}
{%- endmacro %}


{% macro retarget_project_in_ddl(ddl, source_database='') -%}
  {#- A routine template recorded in another project's registry names that project (`<project>`.`<dataset>`.`<object>`).
      Return it with the source project replaced by the active (target) project; unchanged when
      `source_database` is empty or already the active project. Used for the DDL deployed by a
      cross-location promote and for the template stored in the target registry, so that a later
      rollback (which has no source project at hand) redeploys a routine that stays in the target project. -#}
  {%- if ddl is none or source_database == '' or source_database == dbt_virtual.active_database() -%}
    {{ return(ddl) }}
  {%- endif -%}
  {{ return(modules.re.sub('(?<![0-9A-Za-z_-])' ~ modules.re.escape(source_database) ~ '(?![0-9A-Za-z_-])', dbt_virtual.active_database(), ddl)) }}
{%- endmacro %}


{% macro promoted_release_values_sql(release_id, from_env, to_env, item, target_phys_ident, source_database='') -%}
  {#- One `_releases` VALUES tuple for an object promoted from another location. `item` is a row
      of `ordered_registry_rows`. The stored routine template has the source project rewritten to
      the target one (`retarget_project_in_ddl`). -#}
  {%- set row_tier = {'function': 1, 'virtual_function': 1, 'model': 2, 'view': 2, 'table_function': 3}.get(item['resource_type'], 4) -%}
  {{ return(
    "('" ~ release_id ~ "', '" ~ from_env ~ "', '" ~ to_env ~ "', '" ~ item['model_name'] ~ "', '" ~ item['resource_type'] ~ "', "
    ~ row_tier ~ ", '" ~ item['fingerprint'] ~ "', '" ~ target_phys_ident ~ "', "
    ~ (dbt_virtual.sql_string_literal(dbt_virtual.retarget_project_in_ddl(item['routine_ddl'], source_database)) if item['routine_ddl'] is not none else 'NULL')
    ~ ", 'promoted', " ~ dbt_virtual.current_timestamp_sql() ~ ", " ~ dbt_virtual.current_timestamp_sql() ~ ", NULL, "
    ~ dbt_virtual.sql_nullable_literal(item['schema_key']) ~ ", "
    ~ dbt_virtual.sql_nullable_literal(item['build_quality']) ~ ", "
    ~ (dbt_virtual.current_timestamp_sql() if item['sealed_at'] is not none else 'NULL') ~ ")"
  ) }}
{%- endmacro %}


{% macro virtual_sync_reused_views(env='staging', release_id='', models_json='{}') -%}
  {#-
    Zero-compute view sync for `REUSE` models whose `<model>__<fp>` physical table already exists in
    `<physical_schema>`. Avoids invoking `dbt build` on unchanged models while keeping `<env_schema>.<model>`
    views and `_environments` / `_releases` up to date.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set models_map = fromjson(models_json) if models_json is string else models_json -%}
  {%- set ensured_schemas = [] -%}
  {%- set ensured_registries = [] -%}
  {%- set authorized_schemas = [] -%}

  {%- for model_name, fp in models_map.items() -%}
    {#- A model's table and registry are in the location of its node. -#}
    {%- set node = dbt_virtual.virtual_node_by_name(model_name) -%}
    {%- set phys_schema = dbt_virtual.physical_schema_for_node(node) -%}
    {%- set node_loc = dbt_virtual.node_location(node) -%}
    {%- if phys_schema not in ensured_registries -%}
      {%- do dbt_virtual.ensure_registry(node_loc) -%}
      {%- do ensured_registries.append(phys_schema) -%}
    {%- endif -%}
    {%- set schema_key = dbt_virtual.node_schema_key(node) -%}
    {%- set env_schema = dbt_virtual.env_schema_for(env, schema_key) -%}
    {%- if env_schema not in ensured_schemas -%}
      {%- do dbt_virtual.ensure_schema_exists(env_schema, node_loc) -%}
      {%- do ensured_schemas.append(env_schema) -%}
    {%- endif -%}
    {%- set phys_table = model_name ~ '__' ~ fp -%}
    {%- set view_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=env_schema, identifier=model_name, type='view') -%}
    {%- set phys_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=phys_schema, identifier=phys_table, type='table') -%}

    {{ log("dbt-virtual reuse [" ~ env ~ "]: Pointing " ~ view_rel ~ " -> " ~ phys_rel ~ " (zero compute)", info=True) }}
    {%- set view_node = dbt_virtual.virtual_node_by_name(model_name) -%}
    {%- do dbt_virtual.create_env_view(view_rel, phys_rel, view_node, phys_schema, authorized_schemas) -%}

    {%- set moving = dbt_virtual.companion_depart(dbt_virtual.virtual_node_by_name(model_name), env, fp) -%}
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
    {%- if moving -%}
      {#- A reused table keeps its data, so the environment's state must become the table's. -#}
      {%- do dbt_virtual.restore_companion_state(model_name, fp, env, prefer_env=cfg.get('base_environment', 'prod')) -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro virtual_init_env(env='pr_env', from_env='prod') -%}
  {%- do dbt_virtual.warn_if_experimental_adapter() -%}
  {#-
    SQLMesh-style instant virtual environment bootstrap:
    Creates schema `<env_schema>` and points all virtual views at `from_env`'s active physical tables
    in zero compute time.
  -#}
  {%- set env_schema = dbt_virtual.get_env_schema(env) -%}

  {#- A multi-location project creates each environment schema in its own location while promoting. -#}
  {%- if not dbt_virtual.is_multi_location() -%}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(env_schema)) -%}
  {%- endif -%}
  {%- do dbt_virtual.virtual_promote(from_env=from_env, to_env=env, release_id='') -%}
{%- endmacro %}


{% macro virtual_drop_env(env, location=none) -%}
  {#-
    Tears down an ephemeral virtual environment's schemas (e.g. `pr_123`) and removes its pointers
    from `_environments` without touching shared `<physical_schema>` tables.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- if env == base_env -%}
    {{ exceptions.raise_compiler_error("dbt-virtual drop-env: Refusing to drop base_environment '" ~ base_env ~ "'.") }}
  {%- endif -%}
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do dbt_virtual.virtual_drop_env(env, location=loc) -%}
    {%- endfor -%}
    {{ return('') }}
  {%- endif -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {%- do dbt_virtual.migrate_registry_if_old(phys_schema=phys_schema) -%}
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
  {%- set protected = {} -%}
  {%- for protected_schema in dbt_virtual.physical_schemas() -%}
    {%- do protected.update({(protected_schema | lower): 'physical_schema'}) -%}
  {%- endfor -%}
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


{% macro virtual_rollback(env='prod', to_release='', lock=false, lock_owner='', location=none, skip_empty=false) -%}
  {%- do dbt_virtual.warn_if_experimental_adapter() -%}
  {#-
    Instant rollback:
    Repoints all views in `env` back to `to_release` (or the most recent prior `promoted` release in `_releases`).
  -#}
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {#- One job per location: each registry holds the objects of its own location. -#}
    {%- set outcomes = [] -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do outcomes.append(dbt_virtual.virtual_rollback(env=env, to_release=to_release, lock=lock, lock_owner=lock_owner, location=loc, skip_empty=true) | trim) -%}
    {%- endfor -%}
    {%- if (outcomes | select('equalto', 'skipped') | list | length) == (outcomes | length) -%}
      {{ exceptions.raise_compiler_error("dbt-virtual rollback: nothing found in any location (" ~ (dbt_virtual.physical_locations() | join(', ')) ~ ") for env=" ~ env ~ " release=" ~ to_release) }}
    {%- endif -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {#- A registry written by an older release lacks `schema_key` / `build_quality`; migrate it first. -#}
  {%- do dbt_virtual.migrate_registry_if_old(phys_schema=phys_schema) -%}
  {%- set ensured_schemas = [] -%}
  {%- set authorized_schemas = [] -%}
  {%- set owner = lock_owner if lock_owner != '' else ('dbt-virtual:' ~ (invocation_id if invocation_id is defined else 'run')) -%}
  {%- if lock -%}
    {%- do dbt_virtual.virtual_acquire_lock(env=env, owner=owner, location=location) -%}
  {%- endif -%}

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
    {%- if skip_empty -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: nothing to roll back in " ~ phys_schema ~ " (location " ~ location ~ ").", info=True) }}
      {%- if lock -%}
        {%- do dbt_virtual.virtual_release_lock(env=env, owner=owner, location=location) -%}
      {%- endif -%}
      {{ return('skipped') }}
    {%- endif -%}
      {{ dbt_virtual.fail_releasing_lock(lock, env, owner, location, "dbt-virtual rollback: No previous promoted release found for env=" ~ env) }}
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
    {%- if skip_empty -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: nothing to roll back in " ~ phys_schema ~ " (location " ~ location ~ ").", info=True) }}
      {%- if lock -%}
        {%- do dbt_virtual.virtual_release_lock(env=env, owner=owner, location=location) -%}
      {%- endif -%}
      {{ return('skipped') }}
    {%- endif -%}
    {{ dbt_virtual.fail_releasing_lock(lock, env, owner, location, "dbt-virtual rollback: No models found for release " ~ target_release) }}
  {%- endif -%}
  {%- set ordered = dbt_virtual.ordered_registry_rows(rows.rows, 5) -%}
  {#- Pre-flight: every physical table of the target release must still exist (GC may have
      dropped it), otherwise the rollback would stop halfway with views re-pointed. -#}
  {%- for item in ordered -%}
    {%- if item['resource_type'] == 'model' -%}
      {%- set phys_table = item['physical_identifier'].split('.')[-1] -%}
      {%- if adapter.get_relation(database=dbt_virtual.active_database(), schema=phys_schema, identifier=phys_table) is none -%}
        {{ dbt_virtual.fail_releasing_lock(lock, env, owner, location, "dbt-virtual rollback pre-flight failed: physical table " ~ phys_schema ~ "." ~ phys_table ~ " of release " ~ target_release ~ " (model " ~ item['model_name'] ~ ") does not exist.") }}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
  {%- set rollback_names = [] -%}
  {%- for item in ordered -%}
    {%- set model_name = item['model_name'] -%}
    {%- set resource_type = item['resource_type'] -%}
    {%- set fp = item['fingerprint'] -%}
    {%- set phys_ident = item['physical_identifier'] -%}
    {%- set routine_ddl = item['routine_ddl'] -%}
    {%- set schema_key = item['schema_key'] -%}
    {%- set env_schema = dbt_virtual.env_schema_for(env, schema_key) -%}
    {%- do rollback_names.append(model_name) -%}
    {%- if env_schema not in ensured_schemas -%}
      {%- do dbt_virtual.ensure_schema_exists(env_schema, location) -%}
      {%- do ensured_schemas.append(env_schema) -%}
    {%- endif -%}

    {%- if resource_type == 'model' -%}
      {%- set phys_table = phys_ident.split('.')[-1] -%}
      {%- set view_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=env_schema, identifier=model_name, type='view') -%}
      {%- set phys_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=phys_schema, identifier=phys_table, type='table') -%}
      {#- The pre-flight above has verified every physical table, so replacing a legacy TABLE here
          (kept by `adopt --keep-existing-table`) cannot leave the rollback half done; BigQuery
          refuses CREATE OR REPLACE VIEW over a table. The data lives on in the physical table. -#}
      {%- set legacy_rel = adapter.get_relation(database=dbt_virtual.active_database(), schema=env_schema, identifier=model_name) -%}
      {%- if legacy_rel is not none and legacy_rel.type is defined and legacy_rel.type == 'table' -%}
        {{ log("dbt-virtual rollback [" ~ env ~ "]: Dropping legacy physical table " ~ legacy_rel ~ " before creating the view", info=True) }}
        {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(legacy_rel, 'table')) -%}
      {%- endif -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Re-pointing " ~ view_rel ~ " -> " ~ phys_rel, info=True) }}
      {%- set view_node = dbt_virtual.virtual_node_by_name(model_name) -%}
      {%- do dbt_virtual.create_env_view(view_rel, phys_rel, view_node, phys_schema, authorized_schemas) -%}
    {%- elif routine_ddl is not none and routine_ddl != '' -%}
      {%- set rendered_ddl = dbt_virtual.resolve_env_placeholders_sql(routine_ddl, env) -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Redeploying " ~ resource_type ~ " " ~ env_schema ~ "." ~ model_name, info=True) }}
      {%- do dbt_virtual.exec_sql_statement(rendered_ddl) -%}
      {#- Same identifier `virtual_promote` records for a deployed routine. -#}
      {%- set phys_ident = env_schema ~ '.' ~ model_name -%}
    {%- endif -%}

    {%- if resource_type == 'model' -%}
      {%- do dbt_virtual.companion_depart(dbt_virtual.virtual_node_by_name(model_name), env, fp) -%}
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
    {%- if resource_type == 'model' -%}
      {#- Back to the table's own state, so the next run does not skip what it never consumed. -#}
      {%- do dbt_virtual.restore_companion_state(model_name, fp, env) -%}
    {%- endif -%}
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
      {%- set removed_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=removed_schema, identifier=removed_name, type='view') -%}
      {{ log("dbt-virtual rollback [" ~ env ~ "]: Removing post-release " ~ removed_type ~ " " ~ removed_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.drop_object_if_exists_sql(removed_rel, removed_type)) -%}
      {%- do dbt_virtual.exec_ddl_dml(
        "DELETE FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
        ~ " WHERE environment = '" ~ env ~ "' AND model_name = '" ~ removed_name ~ "'"
      ) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if lock -%}
    {%- do dbt_virtual.virtual_release_lock(env=env, owner=owner, location=location) -%}
  {%- endif -%}
{%- endmacro %}


{% macro virtual_expire_envs(expire_env_days, dry_run=true) -%}
  {#-
    Drops abandoned environments (e.g. a PR env nobody dropped) whose newest `_environments.updated_at`
    is older than `expire_env_days` days, so their physical tables stop being protected by GC.
    NEVER expires `base_environment` or an environment listed in `env_schemas`. Their unpromoted
    `candidate` releases are marked `expired` (kept as history, no longer protected): GC protects
    the last N releases of every environment, and non-BigQuery table age comes from release rows.
    `dry_run` only logs. Returns the list of expired (or, in a dry run, expirable) environments.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- set mapped = (cfg.get('env_schemas', {}) or {}).keys() | list -%}
  {%- set expired = [] -%}
  {%- set rows = run_query(dbt_virtual.expired_environments_sql(phys_schema, expire_env_days)) -%}
  {%- if rows -%}
    {%- for row in rows.rows -%}
      {%- if row[0] != base_env and row[0] not in mapped -%}
        {%- do expired.append(row[0]) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- for env in expired -%}
    {%- if dry_run -%}
      {{ log("dbt-virtual gc [DRY-RUN]: Would drop expired environment " ~ env ~ " (no update in " ~ expire_env_days ~ " days)", info=True) }}
    {%- else -%}
      {{ log("dbt-virtual gc: Dropping expired environment " ~ env ~ " (no update in " ~ expire_env_days ~ " days)", info=True) }}
      {%- do dbt_virtual.virtual_drop_env(env) -%}
      {%- do dbt_virtual.exec_ddl_dml(
        "UPDATE " ~ dbt_virtual.registry_relation(phys_schema, '_releases')
        ~ " SET status = 'expired' WHERE status = 'candidate' AND target_environment IS NULL AND source_environment = '" ~ env ~ "'"
      ) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(expired) }}
{%- endmacro %}


{% macro virtual_gc(dry_run=true, retention_releases=5, retention_days=7, location=none, expire_env_days=none) -%}
  {#-
    Janitor GC (first drops environments expired by `expire_env_days`, see `virtual_expire_envs`):
    Identifies `<model>__<fp>` tables in `<physical_schema>` older than `retention_days`
    that are not referenced by any environment in `_environments` or by the last `retention_releases`
    releases in `_releases`.
  -#}
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do dbt_virtual.virtual_gc(dry_run=dry_run, retention_releases=retention_releases, retention_days=retention_days, location=loc) -%}
    {%- endfor -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}

  {%- set expire_days = expire_env_days if expire_env_days is not none else cfg.get('expire_env_days') -%}
  {%- if expire_days is not none and (expire_days | int) > 0 -%}
    {%- do dbt_virtual.virtual_expire_envs(expire_days, dry_run) -%}
  {%- endif -%}

  {%- set orphan_query = dbt_virtual.find_orphan_tables_sql(phys_schema, retention_releases, retention_days) -%}
  {%- set orphans = run_query(orphan_query) -%}
  {%- if not orphans or (orphans.rows | length) == 0 -%}
    {{ log("dbt-virtual gc: 0 orphaned physical tables eligible for deletion.", info=True) }}
    {{ return('') }}
  {%- endif -%}

  {%- set orphan_names = [] -%}
  {%- for row in orphans.rows -%}
    {%- set tbl = row[0] -%}
    {%- do orphan_names.append(tbl) -%}
    {%- set drop_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=phys_schema, identifier=tbl, type='table') -%}
    {%- if dry_run -%}
      {{ log("dbt-virtual gc [DRY-RUN]: Would drop " ~ drop_rel, info=True) }}
    {%- else -%}
      {{ log("dbt-virtual gc: Dropping " ~ drop_rel, info=True) }}
      {%- do dbt_virtual.exec_ddl_dml("DROP TABLE IF EXISTS " ~ drop_rel) -%}
    {%- endif -%}
  {%- endfor -%}
  {#- The state saved for a table (`_companion_state`) goes with the table. -#}
  {%- do dbt_virtual.companion_forget_tables(phys_schema, orphan_names, dry_run) -%}
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
      3. Registers views and routines (table_function, function, virtual_function, procedure) already
         deployed in the env schema, with a DDL template rendered from the node's compiled SQL.
      4. Records the adopted model in `<physical_schema>._environments` and `<physical_schema>._releases`.
  -#}
  {%- if not execute -%}
    {{ return('') }}
  {%- endif -%}

  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set adopted_registries = [] -%}
  {%- if not dbt_virtual.is_multi_location() -%}
    {%- set phys_schema = dbt_virtual.physical_schema_for(none) -%}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(phys_schema)) -%}
    {#- `ensure_registry_tables_sql` returns one SQL string; iterating it ran one statement per character. -#}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.ensure_registry_tables_sql(phys_schema)) -%}
    {%- do adopted_registries.append(phys_schema) -%}
  {%- endif -%}
  {%- set authorized_schemas = [] -%}

  {%- set nodes_dict = graph.get('nodes', {}) if graph is mapping else {} -%}
  {%- for unique_id, node in nodes_dict.items() -%}
    {%- if dbt_virtual.is_virtual_node(node) -%}
      {%- set model_name = dbt_virtual.node_attr(node, 'name', '') -%}
      {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
      {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', 'view') -%}
      {%- set fp = dbt_virtual.get_fingerprint(node) -%}
      {%- set phys_schema = dbt_virtual.physical_schema_for_node(node) -%}
      {%- if phys_schema not in adopted_registries -%}
        {#- One registry per location, created in that location. -#}
        {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.create_schema_if_not_exists_sql(phys_schema, dbt_virtual.create_location_for(dbt_virtual.node_location(node)))) -%}
        {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.ensure_registry_tables_sql(phys_schema)) -%}
        {%- do adopted_registries.append(phys_schema) -%}
      {%- endif -%}
      {%- set schema_key = dbt_virtual.node_schema_key(node) -%}
      {%- set env_schema = dbt_virtual.env_schema_for(env, schema_key) -%}

      {%- if mat in ['table', 'incremental'] -%}
        {%- set phys_table = model_name ~ '__' ~ fp -%}
        {%- set phys_ident = phys_schema ~ '.' ~ phys_table -%}
        {%- set phys_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=phys_schema, identifier=phys_table, type='table') -%}
        {%- set existing_rel = adapter.get_relation(database=dbt_virtual.active_database(), schema=env_schema, identifier=model_name) -%}
        {%- set phys_exists = adapter.get_relation(database=dbt_virtual.active_database(), schema=phys_schema, identifier=phys_table) is not none -%}

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
            {%- set view_rel = api.Relation.create(database=dbt_virtual.active_database(), schema=env_schema, identifier=model_name, type='view') -%}
            {%- do dbt_virtual.create_env_view(view_rel, phys_rel, node, phys_schema, authorized_schemas) -%}
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
      {%- elif mat in ['view', 'table_function', 'function', 'virtual_function', 'procedure'] -%}
        {%- do dbt_virtual.adopt_deployed_object(node, mat, env, env_schema, release_id, fp, schema_key) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro adopt_deployed_object(node, mat, env, env_schema, release_id, fp, schema_key) -%}
  {#-
    Registers a view or routine that is already deployed in `<env_schema>` (adopt never creates or
    changes it). The DDL template is rendered from the node's compiled SQL, which is what later
    promotions replay (`register_view_deployment` / `register_routine_deployment` do the same for
    objects dbt builds). Skips, with a log, when the object is not deployed, its existence cannot
    be checked on this adapter, or no compiled SQL is available.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set name = dbt_virtual.node_attr(node, 'name', '') -%}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- set label = "dbt-virtual adopt [" ~ env ~ "]: " ~ env_schema ~ "." ~ name -%}

  {%- if mat == 'view' -%}
    {%- set resource_type = 'view' -%}
    {%- set dep_tier = 2 -%}
    {%- set deployed = adapter.get_relation(database=dbt_virtual.active_database(), schema=env_schema, identifier=name) is not none -%}
  {%- else -%}
    {%- set resource_type = 'function' if mat == 'virtual_function' else mat -%}
    {%- set dep_tier = 1 if resource_type == 'function' else (3 if resource_type == 'table_function' else 4) -%}
    {%- set exists_sql = dbt_virtual.routine_exists_sql(env_schema, name) -%}
    {%- if (exists_sql | trim) == '' -%}
      {{ log(label ~ ": cannot check whether a " ~ resource_type ~ " exists on adapter '" ~ target.type ~ "'; skipping.", info=True) }}
      {{ return(none) }}
    {%- endif -%}
    {%- set deployed = (run_query(exists_sql).rows | length) > 0 -%}
  {%- endif -%}
  {%- if not deployed -%}
    {{ log(label ~ " (" ~ resource_type ~ ") is not deployed; not registering it.", info=True) }}
    {{ return(none) }}
  {%- endif -%}

  {%- set compiled = var('_dbt_virtual_compiled', {}) -%}
  {%- set body = compiled.get(name) or dbt_virtual.node_attr(node, 'compiled_code', '') or dbt_virtual.node_attr(node, 'compiled_sql', '') -%}
  {%- set body = (body | string | trim).rstrip(';') -%}
  {%- if body == '' -%}
    {{ log(label ~ " (" ~ resource_type ~ "): no compiled SQL (use `dbt-virtual adopt`, which passes it); not registering it.", info=True) }}
    {{ return(none) }}
  {%- endif -%}

  {%- set rel = api.Relation.create(database=dbt_virtual.active_database(), schema=env_schema, identifier=name, type=('external' if (target.type == 'bigquery' and resource_type != 'view') else 'view')) -%}
  {%- if resource_type == 'view' -%}
    {%- set ddl = 'CREATE OR REPLACE VIEW ' ~ rel ~ ' AS\n' ~ body -%}
  {%- else -%}
    {%- set ddl = dbt_virtual.create_routine_ddl_sql(
      resource_type,
      rel | string,
      dbt_virtual.node_attr(node_cfg, 'parameters', ''),
      dbt_virtual.node_attr(node_cfg, 'returns', ''),
      body
    ) -%}
  {%- endif -%}
  {%- set ddl_template = dbt_virtual.env_placeholders_sql(ddl, env) -%}
  {%- set phys_ident = env_schema ~ '.' ~ name -%}
  {{ log(label ~ ": registering deployed " ~ resource_type ~ " (fingerprint " ~ fp ~ ")", info=True) }}

  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.upsert_environment_pointer_sql(
    phys_schema, env, name, resource_type, fp, phys_ident, release_id, schema_key
  )) -%}
  {%- set ins_rel_sql -%}
    INSERT INTO {{ dbt_virtual.registry_relation(phys_schema, '_releases') }} (
      release_id, source_environment, target_environment, model_name, resource_type,
      dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status,
      created_at, promoted_at, rolled_back_at, schema_key
    ) VALUES (
      '{{ release_id }}', '{{ env }}', '{{ env }}', '{{ name }}', '{{ resource_type }}',
      {{ dep_tier }}, '{{ fp }}', '{{ phys_ident }}', {{ dbt_virtual.sql_string_literal(ddl_template) }}, 'promoted',
      {{ dbt_virtual.current_timestamp_sql() }}, {{ dbt_virtual.current_timestamp_sql() }}, NULL,
      {{ dbt_virtual.sql_nullable_literal(schema_key) }}
    )
  {%- endset -%}
  {%- do dbt_virtual.exec_ddl_dml(ins_rel_sql) -%}
{%- endmacro %}


{% macro virtual_seed_incremental_clones(model_names=[]) -%}
  {#- run-operation entry point for `seed_incremental_clones` (called by `dbt-virtual build`). -#}
  {%- do dbt_virtual.seed_incremental_clones(model_names=(model_names or [])) -%}
{%- endmacro %}


{% macro virtual_snapshot_release(env='staging', release_id='', models_json='{}', location=none) -%}
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
  {%- if location is none and dbt_virtual.is_multi_location() -%}
    {%- for loc in dbt_virtual.physical_locations() -%}
      {%- do dbt_virtual.virtual_snapshot_release(env=env, release_id=release_id, models_json=models_json, location=loc) -%}
    {%- endfor -%}
    {{ return('') }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = dbt_virtual.physical_schema_for(location) -%}
  {%- set releases_rel = dbt_virtual.registry_relation(phys_schema, '_releases') -%}
  {%- set models_map = fromjson(models_json) if models_json is string else models_json -%}
  {%- do dbt_virtual.ensure_registry(location) -%}

  {%- set in_release = [] -%}
  {%- set rel_rows = run_query("SELECT model_name FROM " ~ releases_rel ~ " WHERE release_id = '" ~ release_id ~ "'") -%}
  {%- if rel_rows -%}
    {%- for row in rel_rows.rows -%}
      {%- do in_release.append(row[0]) -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set env_query -%}
    SELECT model_name, resource_type, fingerprint, physical_identifier, schema_key, build_quality
    FROM {{ dbt_virtual.registry_relation(phys_schema, '_environments') }}
    WHERE environment = '{{ env }}'
  {%- endset -%}
  {%- set env_rows = run_query(env_query) -%}
  {%- set env_ptrs = {} -%}
  {%- if env_rows -%}
    {%- for row in env_rows.rows -%}
      {%- do env_ptrs.update({row[0]: {'resource_type': row[1], 'fingerprint': row[2], 'physical_identifier': row[3], 'schema_key': dbt_virtual.row_value(row, 4), 'build_quality': dbt_virtual.row_value(row, 5) or 'full'}}) -%}
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
    {%- if not dbt_virtual.node_in_location(dbt_virtual.virtual_node_by_name(model_name), location) or model_name in in_release or mat not in ['table', 'incremental', 'view', 'table_function', 'function', 'virtual_function', 'procedure'] -%}
    {%- elif ptr is none -%}
      {{ log("dbt-virtual snapshot-release [" ~ release_id ~ "]: " ~ model_name ~ " has no pointer in env=" ~ env ~ "; it is not part of the release.", info=True) }}
    {%- else -%}
      {%- set rtype = ptr['resource_type'] -%}
      {%- set tier = {'function': 1, 'model': 2, 'view': 2, 'table_function': 3}.get(rtype, 4) -%}
      {%- set template = templates.get(model_name ~ '|' ~ ptr['fingerprint']) if rtype != 'model' else none -%}
      {%- if (info or {}).get('fingerprint') and info.get('fingerprint') != ptr['fingerprint'] -%}
        {{ log("dbt-virtual snapshot-release [" ~ release_id ~ "]: " ~ model_name ~ " in env=" ~ env ~ " is at " ~ ptr['fingerprint'] ~ ", not the planned " ~ info.get('fingerprint') ~ "; recording the environment's version.", info=True) }}
      {%- endif -%}
      {%- do values.append(
        "('" ~ release_id ~ "', '" ~ env ~ "', NULL, '" ~ model_name ~ "', '" ~ rtype ~ "', " ~ tier ~ ", '"
        ~ ptr['fingerprint'] ~ "', '" ~ ptr['physical_identifier'] ~ "', "
        ~ (dbt_virtual.sql_string_literal(template) if template is not none else 'NULL')
        ~ ", 'candidate', " ~ dbt_virtual.current_timestamp_sql() ~ ", NULL, NULL, "
        ~ dbt_virtual.sql_nullable_literal(ptr['schema_key']) ~ ", '" ~ ptr['build_quality'] ~ "')"
      ) -%}
    {%- endif -%}
  {%- endfor -%}

  {%- for batch in values | batch(200) -%}
    {%- set insert_sql -%}
      INSERT INTO {{ releases_rel }} (
        release_id, source_environment, target_environment, model_name, resource_type, dependency_tier,
        fingerprint, physical_identifier, routine_ddl_template, status, created_at, promoted_at, rolled_back_at,
        schema_key, build_quality
      ) VALUES
      {{ batch | join(',\n') }}
    {%- endset -%}
    {%- do dbt_virtual.exec_sql_statement(insert_sql) -%}
  {%- endfor -%}
  {#- Every row of the release is in: seal it. Only reached after a successful `dbt build`, so
      `virtual_promote` can tell a finished release from one whose build failed half-way. -#}
  {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.seal_release_sql(phys_schema, release_id)) -%}
  {{ log("dbt-virtual snapshot-release [" ~ release_id ~ "]: added " ~ (values | length) ~ " unchanged object(s) from env=" ~ env ~ ".", info=True) }}
{%- endmacro %}
