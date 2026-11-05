{% macro current_timestamp_sql() -%}
  {{ return(adapter.dispatch('current_timestamp_sql', 'dbt_virtual')()) }}
{%- endmacro %}

{% macro bigquery__current_timestamp_sql() -%}
  CURRENT_TIMESTAMP()
{%- endmacro %}

{% macro default__current_timestamp_sql() -%}
  CURRENT_TIMESTAMP
{%- endmacro %}


{% macro clone_table_sql(source_relation, target_relation) -%}
  {{ return(adapter.dispatch('clone_table_sql', 'dbt_virtual')(source_relation, target_relation)) }}
{%- endmacro %}

{% macro bigquery__clone_table_sql(source_relation, target_relation) -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- if cfg.get('use_ctas_for_clone', false) or target.name == 'emulator' -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }} AS
    SELECT * FROM {{ source_relation }}
  {%- else -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }}
    CLONE {{ source_relation }}
  {%- endif -%}
{%- endmacro %}

{% macro snowflake__clone_table_sql(source_relation, target_relation) -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- if cfg.get('use_ctas_for_clone', false) or target.name == 'emulator' -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }} AS
    SELECT * FROM {{ source_relation }}
  {%- else -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }}
    CLONE {{ source_relation }}
  {%- endif -%}
{%- endmacro %}

{% macro databricks__clone_table_sql(source_relation, target_relation) -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- if cfg.get('use_ctas_for_clone', false) or target.name == 'emulator' -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }} AS
    SELECT * FROM {{ source_relation }}
  {%- else -%}
    CREATE TABLE IF NOT EXISTS {{ target_relation }}
    SHALLOW CLONE {{ source_relation }}
  {%- endif -%}
{%- endmacro %}

{% macro default__clone_table_sql(source_relation, target_relation) -%}
  CREATE TABLE IF NOT EXISTS {{ target_relation }} AS
  SELECT * FROM {{ source_relation }}
{%- endmacro %}


{% macro create_or_replace_view_sql(view_relation, physical_relation) -%}
  {{ return(adapter.dispatch('create_or_replace_view_sql', 'dbt_virtual')(view_relation, physical_relation)) }}
{%- endmacro %}

{% macro postgres__create_or_replace_view_sql(view_relation, physical_relation) -%}
  DROP VIEW IF EXISTS {{ view_relation }} CASCADE;
  CREATE VIEW {{ view_relation }} AS
  SELECT * FROM {{ physical_relation }}
{%- endmacro %}

{% macro default__create_or_replace_view_sql(view_relation, physical_relation) -%}
  CREATE OR REPLACE VIEW {{ view_relation }} AS
  SELECT * FROM {{ physical_relation }}
{%- endmacro %}


{% macro create_schema_if_not_exists_sql(schema_name, location=none) -%}
  {{ return(adapter.dispatch('create_schema_if_not_exists_sql', 'dbt_virtual')(schema_name, location)) }}
{%- endmacro %}

{% macro bigquery__create_schema_if_not_exists_sql(schema_name, location=none) -%}
  {#- `location` pins the dataset to a BigQuery location; dbt-bigquery's own `create schema` leaves
      it to the job's location. -#}
  CREATE SCHEMA IF NOT EXISTS `{{ target.database }}`.`{{ schema_name }}`{{ " OPTIONS (location = '" ~ location ~ "')" if location is not none else "" }}
{%- endmacro %}

{% macro snowflake__create_schema_if_not_exists_sql(schema_name, location=none) -%}
  CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
{%- endmacro %}

{% macro default__create_schema_if_not_exists_sql(schema_name, location=none) -%}
  CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
{%- endmacro %}


{% macro drop_schema_cascade_sql(schema_name) -%}
  {{ return(adapter.dispatch('drop_schema_cascade_sql', 'dbt_virtual')(schema_name)) }}
{%- endmacro %}

{% macro bigquery__drop_schema_cascade_sql(schema_name) -%}
  DROP SCHEMA IF EXISTS `{{ target.database }}`.`{{ schema_name }}` CASCADE
{%- endmacro %}

{% macro snowflake__drop_schema_cascade_sql(schema_name) -%}
  DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE
{%- endmacro %}

{% macro default__drop_schema_cascade_sql(schema_name) -%}
  DROP SCHEMA IF EXISTS {{ schema_name }} CASCADE
{%- endmacro %}


{% macro registry_relation(physical_schema, table_name) -%}
  {{ return(adapter.dispatch('registry_relation', 'dbt_virtual')(physical_schema, table_name)) }}
{%- endmacro %}

{% macro bigquery__registry_relation(physical_schema, table_name) -%}
  `{{ target.database }}`.`{{ physical_schema }}`.{{ table_name }}
{%- endmacro %}

{% macro snowflake__registry_relation(physical_schema, table_name) -%}
  {{ physical_schema }}.{{ table_name }}
{%- endmacro %}

{% macro default__registry_relation(physical_schema, table_name) -%}
  {{ physical_schema }}.{{ table_name }}
{%- endmacro %}


{% macro ensure_registry_tables_sql(physical_schema) -%}
  {{ return(adapter.dispatch('ensure_registry_tables_sql', 'dbt_virtual')(physical_schema)) }}
{%- endmacro %}

{% macro bigquery__ensure_registry_tables_sql(physical_schema) -%}
  CREATE SCHEMA IF NOT EXISTS `{{ target.database }}`.`{{ physical_schema }}`;

  CREATE TABLE IF NOT EXISTS `{{ target.database }}`.`{{ physical_schema }}`._environments (
    environment STRING NOT NULL,
    model_name STRING NOT NULL,
    resource_type STRING NOT NULL,
    fingerprint STRING NOT NULL,
    physical_identifier STRING NOT NULL,
    release_id STRING,
    updated_at TIMESTAMP NOT NULL,
    schema_key STRING,
    build_quality STRING
  )
  CLUSTER BY environment, model_name;

  CREATE TABLE IF NOT EXISTS `{{ target.database }}`.`{{ physical_schema }}`._releases (
    release_id STRING NOT NULL,
    source_environment STRING NOT NULL,
    target_environment STRING,
    model_name STRING NOT NULL,
    resource_type STRING NOT NULL,
    dependency_tier INT64 NOT NULL,
    fingerprint STRING NOT NULL,
    physical_identifier STRING NOT NULL,
    routine_ddl_template STRING,
    status STRING NOT NULL,
    created_at TIMESTAMP NOT NULL,
    promoted_at TIMESTAMP,
    rolled_back_at TIMESTAMP,
    schema_key STRING,
    build_quality STRING,
    sealed_at TIMESTAMP
  )
  PARTITION BY DATE(created_at)
  CLUSTER BY release_id, status;

  CREATE TABLE IF NOT EXISTS `{{ target.database }}`.`{{ physical_schema }}`._locks (
    environment STRING NOT NULL,
    owner STRING NOT NULL,
    acquired_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL
  );

  CREATE TABLE IF NOT EXISTS `{{ target.database }}`.`{{ physical_schema }}`._intervals (
    model_name STRING NOT NULL,
    fingerprint STRING NOT NULL,
    physical_identifier STRING NOT NULL,
    start_ts STRING NOT NULL,
    end_ts STRING NOT NULL,
    recorded_at TIMESTAMP NOT NULL
  )
  CLUSTER BY model_name, fingerprint;
{%- endmacro %}

{% macro snowflake__ensure_registry_tables_sql(physical_schema) -%}
  CREATE SCHEMA IF NOT EXISTS {{ physical_schema }};

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._environments (
    environment VARCHAR NOT NULL,
    model_name VARCHAR NOT NULL,
    resource_type VARCHAR NOT NULL,
    fingerprint VARCHAR NOT NULL,
    physical_identifier VARCHAR NOT NULL,
    release_id VARCHAR,
    updated_at TIMESTAMP NOT NULL,
    schema_key VARCHAR,
    build_quality VARCHAR
  );

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._releases (
    release_id VARCHAR NOT NULL,
    source_environment VARCHAR NOT NULL,
    target_environment VARCHAR,
    model_name VARCHAR NOT NULL,
    resource_type VARCHAR NOT NULL,
    dependency_tier INTEGER NOT NULL,
    fingerprint VARCHAR NOT NULL,
    physical_identifier VARCHAR NOT NULL,
    routine_ddl_template VARCHAR,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    promoted_at TIMESTAMP,
    rolled_back_at TIMESTAMP,
    schema_key VARCHAR,
    build_quality VARCHAR,
    sealed_at TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._locks (
    environment VARCHAR NOT NULL,
    owner VARCHAR NOT NULL,
    acquired_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL
  );

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._intervals (
    model_name VARCHAR NOT NULL,
    fingerprint VARCHAR NOT NULL,
    physical_identifier VARCHAR NOT NULL,
    start_ts VARCHAR NOT NULL,
    end_ts VARCHAR NOT NULL,
    recorded_at TIMESTAMP NOT NULL
  );
{%- endmacro %}

{% macro default__ensure_registry_tables_sql(physical_schema) -%}
  CREATE SCHEMA IF NOT EXISTS {{ physical_schema }};

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._environments (
    environment VARCHAR,
    model_name VARCHAR,
    resource_type VARCHAR,
    fingerprint VARCHAR,
    physical_identifier VARCHAR,
    release_id VARCHAR,
    updated_at TIMESTAMP,
    schema_key VARCHAR,
    build_quality VARCHAR
  );

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._releases (
    release_id VARCHAR,
    source_environment VARCHAR,
    target_environment VARCHAR,
    model_name VARCHAR,
    resource_type VARCHAR,
    dependency_tier INTEGER,
    fingerprint VARCHAR,
    physical_identifier VARCHAR,
    routine_ddl_template VARCHAR,
    status VARCHAR,
    created_at TIMESTAMP,
    promoted_at TIMESTAMP,
    rolled_back_at TIMESTAMP,
    schema_key VARCHAR,
    build_quality VARCHAR,
    sealed_at TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._locks (
    environment VARCHAR,
    owner VARCHAR,
    acquired_at TIMESTAMP,
    expires_at TIMESTAMP
  );

  CREATE TABLE IF NOT EXISTS {{ physical_schema }}._intervals (
    model_name VARCHAR,
    fingerprint VARCHAR,
    physical_identifier VARCHAR,
    start_ts VARCHAR,
    end_ts VARCHAR,
    recorded_at TIMESTAMP
  );
{%- endmacro %}


{% macro registry_migrations_sql(physical_schema) -%}
  {{ return(adapter.dispatch('registry_migrations_sql', 'dbt_virtual')(physical_schema)) }}
{%- endmacro %}

{% macro bigquery__registry_migrations_sql(physical_schema) -%}
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} ADD COLUMN IF NOT EXISTS schema_key STRING;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} ADD COLUMN IF NOT EXISTS schema_key STRING;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} ADD COLUMN IF NOT EXISTS build_quality STRING;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} ADD COLUMN IF NOT EXISTS build_quality STRING;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} ADD COLUMN IF NOT EXISTS sealed_at TIMESTAMP
{%- endmacro %}

{% macro default__registry_migrations_sql(physical_schema) -%}
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} ADD COLUMN IF NOT EXISTS schema_key VARCHAR;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} ADD COLUMN IF NOT EXISTS schema_key VARCHAR;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} ADD COLUMN IF NOT EXISTS build_quality VARCHAR;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} ADD COLUMN IF NOT EXISTS build_quality VARCHAR;
  ALTER TABLE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} ADD COLUMN IF NOT EXISTS sealed_at TIMESTAMP
{%- endmacro %}


{% macro sql_nullable_literal(value) -%}
  {%- if value is none or value == '' -%}
    {{ return('NULL') }}
  {%- endif -%}
  {{ return(dbt_virtual.sql_string_literal(value)) }}
{%- endmacro %}


{% macro upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id, schema_key=none) -%}
  {{ return(adapter.dispatch('upsert_environment_pointer_sql', 'dbt_virtual')(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id, schema_key)) }}
{%- endmacro %}

{% macro bigquery__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id, schema_key=none) -%}
  MERGE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} AS T
  USING (
    SELECT
      '{{ env_name }}' AS environment,
      '{{ model_name }}' AS model_name,
      '{{ resource_type }}' AS resource_type,
      '{{ fingerprint }}' AS fingerprint,
      '{{ physical_identifier }}' AS physical_identifier,
      '{{ release_id }}' AS release_id,
      CURRENT_TIMESTAMP() AS updated_at,
      CAST({{ dbt_virtual.sql_nullable_literal(schema_key) }} AS STRING) AS schema_key
  ) AS S
  ON T.environment = S.environment AND T.model_name = S.model_name
  WHEN MATCHED THEN UPDATE SET
    resource_type = S.resource_type,
    fingerprint = S.fingerprint,
    physical_identifier = S.physical_identifier,
    release_id = S.release_id,
    updated_at = S.updated_at,
    schema_key = S.schema_key
  WHEN NOT MATCHED THEN INSERT (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at, schema_key)
    VALUES (S.environment, S.model_name, S.resource_type, S.fingerprint, S.physical_identifier, S.release_id, S.updated_at, S.schema_key)
{%- endmacro %}

{% macro snowflake__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id, schema_key=none) -%}
  DELETE FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  WHERE environment = '{{ env_name }}' AND model_name = '{{ model_name }}';
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} (
    environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at, schema_key
  ) VALUES (
    '{{ env_name }}', '{{ model_name }}', '{{ resource_type }}', '{{ fingerprint }}', '{{ physical_identifier }}', '{{ release_id }}', CURRENT_TIMESTAMP, {{ dbt_virtual.sql_nullable_literal(schema_key) }}
  )
{%- endmacro %}

{% macro default__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id, schema_key=none) -%}
  DELETE FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  WHERE environment = '{{ env_name }}' AND model_name = '{{ model_name }}';
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} (
    environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at, schema_key
  ) VALUES (
    '{{ env_name }}', '{{ model_name }}', '{{ resource_type }}', '{{ fingerprint }}', '{{ physical_identifier }}', '{{ release_id }}', CURRENT_TIMESTAMP, {{ dbt_virtual.sql_nullable_literal(schema_key) }}
  )
{%- endmacro %}


{% macro upsert_environment_pointers_sql(physical_schema, env_name, rows) -%}
  {#-
    Multi-row form of `upsert_environment_pointer_sql` for `flush_registry`: one statement (batch)
    for N pointers of one environment. `rows` are dicts with `name`, `resource_type`, `fingerprint`,
    `physical_identifier`, `release_id` and `schema_key`; names must be unique within `rows`.
  -#}
  {{ return(adapter.dispatch('upsert_environment_pointers_sql', 'dbt_virtual')(physical_schema, env_name, rows)) }}
{%- endmacro %}

{% macro bigquery__upsert_environment_pointers_sql(physical_schema, env_name, rows) -%}
  MERGE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} AS T
  USING (
    {%- for r in rows %}
    {{ 'UNION ALL ' if not loop.first else '' }}SELECT
      {{ dbt_virtual.sql_string_literal(env_name) }} AS environment,
      {{ dbt_virtual.sql_string_literal(r['name']) }} AS model_name,
      {{ dbt_virtual.sql_string_literal(r['resource_type']) }} AS resource_type,
      {{ dbt_virtual.sql_string_literal(r['fingerprint']) }} AS fingerprint,
      {{ dbt_virtual.sql_string_literal(r['physical_identifier']) }} AS physical_identifier,
      {{ dbt_virtual.sql_string_literal(r['release_id']) }} AS release_id,
      CURRENT_TIMESTAMP() AS updated_at,
      CAST({{ dbt_virtual.sql_nullable_literal(r['schema_key']) }} AS STRING) AS schema_key
    {%- endfor %}
  ) AS S
  ON T.environment = S.environment AND T.model_name = S.model_name
  WHEN MATCHED THEN UPDATE SET
    resource_type = S.resource_type,
    fingerprint = S.fingerprint,
    physical_identifier = S.physical_identifier,
    release_id = S.release_id,
    updated_at = S.updated_at,
    schema_key = S.schema_key
  WHEN NOT MATCHED THEN INSERT (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at, schema_key)
    VALUES (S.environment, S.model_name, S.resource_type, S.fingerprint, S.physical_identifier, S.release_id, S.updated_at, S.schema_key)
{%- endmacro %}

{% macro default__upsert_environment_pointers_sql(physical_schema, env_name, rows) -%}
  DELETE FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  WHERE environment = {{ dbt_virtual.sql_string_literal(env_name) }}
    AND model_name IN ({% for r in rows %}{{ dbt_virtual.sql_string_literal(r['name']) }}{{ ', ' if not loop.last else '' }}{% endfor %});
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} (
    environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at, schema_key
  ) VALUES
  {%- for r in rows %}
  ({{ dbt_virtual.sql_string_literal(env_name) }}, {{ dbt_virtual.sql_string_literal(r['name']) }}, {{ dbt_virtual.sql_string_literal(r['resource_type']) }}, {{ dbt_virtual.sql_string_literal(r['fingerprint']) }}, {{ dbt_virtual.sql_string_literal(r['physical_identifier']) }}, {{ dbt_virtual.sql_string_literal(r['release_id']) }}, CURRENT_TIMESTAMP, {{ dbt_virtual.sql_nullable_literal(r['schema_key']) }}){{ ',' if not loop.last else '' }}
  {%- endfor %}
{%- endmacro %}


{% macro create_routine_ddl_sql(routine_type, target_relation_str, params_sig, returns_sig, body_sql) -%}
  {{ return(adapter.dispatch('create_routine_ddl_sql', 'dbt_virtual')(routine_type, target_relation_str, params_sig, returns_sig, body_sql)) }}
{%- endmacro %}

{% macro bigquery__create_routine_ddl_sql(routine_type, target_relation_str, params_sig, returns_sig, body_sql) -%}
  {%- if routine_type == 'function' -%}
CREATE OR REPLACE FUNCTION {{ target_relation_str }}({{ params_sig }})
RETURNS {{ returns_sig }}
AS (
{{ body_sql }}
)
  {%- elif routine_type == 'table_function' -%}
CREATE OR REPLACE TABLE FUNCTION {{ target_relation_str }}({{ params_sig }})
{%- if returns_sig %}
RETURNS {{ returns_sig }}
{%- endif %}
AS (
{{ body_sql }}
)
  {%- else -%}
CREATE OR REPLACE PROCEDURE {{ target_relation_str }}({{ params_sig }})
{{ body_sql }}
  {%- endif -%}
{%- endmacro %}

{% macro snowflake__create_routine_ddl_sql(routine_type, target_relation_str, params_sig, returns_sig, body_sql) -%}
  {%- if routine_type == 'function' -%}
CREATE OR REPLACE FUNCTION {{ target_relation_str }}({{ params_sig }})
RETURNS {{ returns_sig }}
AS $$
{{ body_sql }}
$$
  {%- elif routine_type == 'table_function' -%}
CREATE OR REPLACE FUNCTION {{ target_relation_str }}({{ params_sig }})
RETURNS {{ returns_sig if returns_sig else 'TABLE()' }}
AS $$
{{ body_sql }}
$$
  {%- else -%}
CREATE OR REPLACE PROCEDURE {{ target_relation_str }}({{ params_sig }})
RETURNS {{ returns_sig if returns_sig else 'VARCHAR' }}
LANGUAGE SQL
AS $$
{{ body_sql }}
$$
  {%- endif -%}
{%- endmacro %}

{% macro duckdb__create_routine_ddl_sql(routine_type, target_relation_str, params_sig, returns_sig, body_sql) -%}
  {%- if routine_type == 'function' -%}
CREATE OR REPLACE MACRO {{ target_relation_str }}({{ params_sig }}) AS ({{ body_sql }})
  {%- elif routine_type == 'table_function' -%}
CREATE OR REPLACE MACRO {{ target_relation_str }}({{ params_sig }}) AS TABLE ({{ body_sql }})
  {%- else -%}
-- DuckDB does not have SQL stored procedures; no-op
SELECT 1
  {%- endif -%}
{%- endmacro %}

{% macro default__create_routine_ddl_sql(routine_type, target_relation_str, params_sig, returns_sig, body_sql) -%}
  {%- if routine_type in ['function', 'table_function'] -%}
CREATE OR REPLACE FUNCTION {{ target_relation_str }}({{ params_sig }})
RETURNS {{ returns_sig if returns_sig else ' RECORD' }}
LANGUAGE SQL
AS $$
{{ body_sql }}
$$
  {%- else -%}
CREATE OR REPLACE PROCEDURE {{ target_relation_str }}({{ params_sig }})
LANGUAGE SQL
AS $$
{{ body_sql }}
$$
  {%- endif -%}
{%- endmacro %}


{% macro find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  {{ return(adapter.dispatch('find_orphan_tables_sql', 'dbt_virtual')(physical_schema, retention_releases, retention_days)) }}
{%- endmacro %}

{% macro bigquery__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  WITH protected_releases AS (
    -- The last N releases of EACH environment: ranking across all environments let busy PR
    -- candidates push prod's rollback targets out of retention.
    SELECT release_id
    FROM (
      SELECT
        release_id,
        COALESCE(MAX(target_environment), MAX(source_environment)) AS release_env,
        MAX(COALESCE(promoted_at, created_at)) AS release_ts
      FROM `{{ target.database }}`.`{{ physical_schema }}`._releases
      WHERE status IN ('promoted', 'candidate', 'rolled_back')
      GROUP BY release_id
    )
    -- BigQuery rejects QUALIFY without WHERE, GROUP BY or HAVING.
    WHERE TRUE
    QUALIFY DENSE_RANK() OVER (PARTITION BY release_env ORDER BY release_ts DESC) <= {{ retention_releases }}
  ),
  protected_tables AS (
    SELECT SPLIT(physical_identifier, '.')[SAFE_OFFSET(1)] AS table_name
    FROM `{{ target.database }}`.`{{ physical_schema }}`._environments
    WHERE resource_type = 'model'
    UNION DISTINCT
    SELECT SPLIT(r.physical_identifier, '.')[SAFE_OFFSET(1)] AS table_name
    FROM `{{ target.database }}`.`{{ physical_schema }}`._releases AS r
    INNER JOIN protected_releases AS pr USING (release_id)
    WHERE r.resource_type = 'model'
  )
  SELECT t.table_name
  FROM `{{ target.database }}`.`{{ physical_schema }}`.INFORMATION_SCHEMA.TABLES AS t
  LEFT JOIN protected_tables AS p ON t.table_name = p.table_name
  WHERE t.table_type IN ('BASE TABLE', 'CLONE', 'SNAPSHOT')
    AND REGEXP_CONTAINS(t.table_name, r'__[0-9a-f]{8,64}$')
    AND t.creation_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ retention_days }} DAY)
    AND p.table_name IS NULL
  ORDER BY t.table_name
{%- endmacro %}

{% macro snowflake__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  WITH protected_releases AS (
    SELECT release_id
    FROM (
      SELECT
        release_id,
        COALESCE(MAX(target_environment), MAX(source_environment)) AS release_env,
        MAX(COALESCE(promoted_at, created_at)) AS release_ts
      FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }}
      WHERE status IN ('promoted', 'candidate', 'rolled_back')
      GROUP BY release_id
    )
    WHERE TRUE
    QUALIFY DENSE_RANK() OVER (PARTITION BY release_env ORDER BY release_ts DESC) <= {{ retention_releases }}
  ),
  protected_tables AS (
    SELECT LOWER(SPLIT_PART(physical_identifier, '.', 2)) AS table_name
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
    WHERE resource_type = 'model'
    UNION
    SELECT LOWER(SPLIT_PART(r.physical_identifier, '.', 2)) AS table_name
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} AS r
    INNER JOIN protected_releases AS pr USING (release_id)
    WHERE r.resource_type = 'model'
  )
  SELECT LOWER(t.table_name) AS table_name
  FROM {{ target.database }}.information_schema.tables AS t
  LEFT JOIN protected_tables AS p ON LOWER(t.table_name) = p.table_name
  WHERE UPPER(t.table_schema) = UPPER('{{ physical_schema }}')
    AND t.table_type = 'BASE TABLE'
    AND REGEXP_LIKE(LOWER(t.table_name), '.*__[0-9a-f]{8,64}$')
    AND t.created < DATEADD(day, -{{ retention_days }}, CURRENT_TIMESTAMP())
    AND p.table_name IS NULL
  ORDER BY 1
{%- endmacro %}

{% macro duckdb__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  {{ return(dbt_virtual.find_orphan_tables_with_suffix_sql(
    physical_schema,
    retention_releases,
    retention_days,
    "regexp_matches(LOWER(t.table_name), '__[0-9a-f]{8,64}$')"
  )) }}
{%- endmacro %}

{% macro postgres__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  {{ return(dbt_virtual.find_orphan_tables_with_suffix_sql(
    physical_schema,
    retention_releases,
    retention_days,
    "LOWER(t.table_name) ~ '__[0-9a-f]{8,64}$'"
  )) }}
{%- endmacro %}

{% macro databricks__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  {{ return(dbt_virtual.find_orphan_tables_with_suffix_sql(
    physical_schema,
    retention_releases,
    retention_days,
    "LOWER(t.table_name) RLIKE '__[0-9a-f]{8,64}$'"
  )) }}
{%- endmacro %}

{% macro find_orphan_tables_with_suffix_sql(physical_schema, retention_releases, retention_days, suffix_predicate) -%}
  WITH protected_releases AS (
    SELECT release_id
    FROM (
      SELECT release_id,
        DENSE_RANK() OVER (PARTITION BY release_env ORDER BY release_ts DESC) AS release_rank
      FROM (
        SELECT
          release_id,
          COALESCE(MAX(target_environment), MAX(source_environment)) AS release_env,
          MAX(COALESCE(promoted_at, created_at)) AS release_ts
        FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }}
        WHERE status IN ('promoted', 'candidate', 'rolled_back')
        GROUP BY release_id
      ) AS release_times
    ) AS ranked_releases
    WHERE release_rank <= {{ retention_releases }}
  ),
  protected_tables AS (
    SELECT LOWER(REPLACE(physical_identifier, '{{ physical_schema }}.', '')) AS table_name
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
    WHERE resource_type = 'model'
    UNION
    SELECT LOWER(REPLACE(r.physical_identifier, '{{ physical_schema }}.', '')) AS table_name
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} AS r
    INNER JOIN protected_releases AS pr USING (release_id)
    WHERE r.resource_type = 'model'
  )
  SELECT LOWER(t.table_name) AS table_name
  FROM information_schema.tables AS t
  LEFT JOIN protected_tables AS p ON LOWER(t.table_name) = p.table_name
  WHERE LOWER(t.table_schema) = LOWER('{{ physical_schema }}')
    AND t.table_type = 'BASE TABLE'
    AND {{ suffix_predicate }}
    AND {{ dbt_virtual.orphan_table_age_predicate_sql(physical_schema, retention_days) }}
    AND p.table_name IS NULL
  ORDER BY 1
{%- endmacro %}

{% macro orphan_table_age_predicate_sql(physical_schema, retention_days) -%}
  {{ return(adapter.dispatch('orphan_table_age_predicate_sql', 'dbt_virtual')(physical_schema, retention_days)) }}
{%- endmacro %}

{% macro duckdb__orphan_table_age_predicate_sql(physical_schema, retention_days) -%}
  {#- DuckDB has no portable creation timestamp for tables; require recorded release age, failing safe for untracked tables. -#}
  EXISTS (
    SELECT 1
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} AS age_r
    WHERE age_r.resource_type = 'model'
      AND LOWER(REPLACE(age_r.physical_identifier, '{{ physical_schema }}.', '')) = LOWER(t.table_name)
    GROUP BY LOWER(REPLACE(age_r.physical_identifier, '{{ physical_schema }}.', ''))
      HAVING MAX(age_r.created_at) < CURRENT_TIMESTAMP - INTERVAL '{{ retention_days }} days'
  )
{%- endmacro %}

{% macro postgres__orphan_table_age_predicate_sql(physical_schema, retention_days) -%}
  {#- PostgreSQL's portable catalogs omit table creation time; use release timestamps and retain untracked tables. -#}
  EXISTS (
    SELECT 1
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }} AS age_r
    WHERE age_r.resource_type = 'model'
      AND LOWER(REPLACE(age_r.physical_identifier, '{{ physical_schema }}.', '')) = LOWER(t.table_name)
    GROUP BY LOWER(REPLACE(age_r.physical_identifier, '{{ physical_schema }}.', ''))
      HAVING MAX(age_r.created_at) < CURRENT_TIMESTAMP - INTERVAL '{{ retention_days }} days'
  )
{%- endmacro %}

{% macro default__orphan_table_age_predicate_sql(physical_schema, retention_days) -%}
  {#- dbt resolves `adapter.dispatch(...)` while parsing; without a default, every dbt command on
      BigQuery and Snowflake failed with "No macro named 'orphan_table_age_predicate_sql'". -#}
  {{ exceptions.raise_compiler_error("dbt-virtual gc: no table age predicate for adapter '" ~ target.type ~ "'.") }}
{%- endmacro %}

{% macro databricks__orphan_table_age_predicate_sql(physical_schema, retention_days) -%}
  t.created < DATEADD(DAY, -{{ retention_days }}, CURRENT_TIMESTAMP())
{%- endmacro %}

{% macro default__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  {{ exceptions.raise_compiler_error(
    "dbt-virtual gc is not supported for adapter '" ~ target.type
    ~ "': no safe fingerprint table matcher is configured."
  ) }}
{%- endmacro %}


{% macro drop_object_if_exists_sql(object_relation, resource_type='model') -%}
  {{ return(adapter.dispatch('drop_object_if_exists_sql', 'dbt_virtual')(object_relation, resource_type)) }}
{%- endmacro %}

{% macro bigquery__drop_object_if_exists_sql(object_relation, resource_type='model') -%}
  {%- if resource_type == 'table' -%}
    DROP TABLE IF EXISTS {{ object_relation }}
  {%- elif resource_type in ['model', 'view'] -%}
    DROP VIEW IF EXISTS {{ object_relation }}
  {%- elif resource_type == 'table_function' -%}
    DROP TABLE FUNCTION IF EXISTS {{ object_relation }}
  {%- elif resource_type == 'function' -%}
    DROP FUNCTION IF EXISTS {{ object_relation }}
  {%- elif resource_type == 'procedure' -%}
    DROP PROCEDURE IF EXISTS {{ object_relation }}
  {%- else -%}
    DROP VIEW IF EXISTS {{ object_relation }}
  {%- endif -%}
{%- endmacro %}

{% macro duckdb__drop_object_if_exists_sql(object_relation, resource_type='model') -%}
  {%- if resource_type == 'table' -%}
    DROP TABLE IF EXISTS {{ object_relation }}
  {%- elif resource_type in ['model', 'view'] -%}
    DROP VIEW IF EXISTS {{ object_relation }}
  {%- elif resource_type == 'table_function' -%}
    DROP MACRO TABLE IF EXISTS {{ object_relation }}
  {%- elif resource_type == 'function' -%}
    DROP MACRO IF EXISTS {{ object_relation }}
  {%- else -%}
    SELECT 1
  {%- endif -%}
{%- endmacro %}

{% macro default__drop_object_if_exists_sql(object_relation, resource_type='model') -%}
  {%- if resource_type == 'table' -%}
    DROP TABLE IF EXISTS {{ object_relation }} CASCADE
  {%- elif resource_type in ['model', 'view'] -%}
    DROP VIEW IF EXISTS {{ object_relation }} CASCADE
  {%- else -%}
    SELECT 1
  {%- endif -%}
{%- endmacro %}


{% macro timestamp_add_minutes_sql(minutes) -%}
  {{ return(adapter.dispatch('timestamp_add_minutes_sql', 'dbt_virtual')(minutes)) }}
{%- endmacro %}

{% macro bigquery__timestamp_add_minutes_sql(minutes) -%}
  TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {{ minutes }} MINUTE)
{%- endmacro %}

{% macro snowflake__timestamp_add_minutes_sql(minutes) -%}
  DATEADD(minute, {{ minutes }}, CURRENT_TIMESTAMP)
{%- endmacro %}

{% macro default__timestamp_add_minutes_sql(minutes) -%}
  (CURRENT_TIMESTAMP + INTERVAL '{{ minutes }} minutes')
{%- endmacro %}


{% macro list_physical_tables_sql(physical_schema) -%}
  {{ return(adapter.dispatch('list_physical_tables_sql', 'dbt_virtual')(physical_schema)) }}
{%- endmacro %}

{% macro bigquery__list_physical_tables_sql(physical_schema) -%}
  SELECT table_name
  FROM `{{ target.database }}`.`{{ physical_schema }}`.INFORMATION_SCHEMA.TABLES
  WHERE table_type IN ('BASE TABLE', 'CLONE', 'SNAPSHOT')
    AND REGEXP_CONTAINS(table_name, r'__[0-9a-f]{8,64}$')
  ORDER BY table_name
{%- endmacro %}

{% macro default__list_physical_tables_sql(physical_schema) -%}
  SELECT table_name
  FROM information_schema.tables
  WHERE LOWER(table_schema) = LOWER('{{ physical_schema }}')
    AND table_type = 'BASE TABLE'
    AND table_name LIKE '%\_\_%' ESCAPE '\'
  ORDER BY table_name
{%- endmacro %}


{% macro env_schema_placeholder_sql(sql_text, env_schema) -%}
  {#- Swap only schema-qualified references (`env.x`, `` `env`.x ``, `"env".x`) for the
      promotion placeholder. A bare `replace()` also rewrote columns, other schemas and string
      literals that merely contain the env schema name (`staging_flag`, `raw_staging.events`). -#}
  {%- set pattern = '(?<![0-9A-Za-z_$])' ~ modules.re.escape(env_schema) ~ '(?=[`"]?\\.)' -%}
  {{ return(modules.re.sub(pattern, '__DBT_VIRTUAL_ENV_SCHEMA__', sql_text)) }}
{%- endmacro %}


{% macro env_placeholder_token(schema_key=none) -%}
  {%- if schema_key is none or schema_key == '' or schema_key == 'default' -%}
    {{ return('__DBT_VIRTUAL_ENV_SCHEMA__') }}
  {%- endif -%}
  {{ return('__DBT_VIRTUAL_ENV_SCHEMA_' ~ schema_key ~ '__') }}
{%- endmacro %}


{% macro env_placeholders_sql(sql_text, env_name=none) -%}
  {#- Replace references to virtual objects in `env_name`'s schemas with a placeholder naming the
      object's custom schema, so promotion can point each reference at the right schema of the
      target environment. Several custom schemas may share one schema in `env_name` (e.g. a
      single-dataset staging), so this goes object by object, not schema by schema. -#}
  {%- set result = namespace(sql=sql_text) -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- if dbt_virtual.is_virtual_node(node) -%}
        {%- set key = dbt_virtual.node_schema_key(node) -%}
        {%- set schema = dbt_virtual.env_schema_for(env_name, key) -%}
        {%- set ident = dbt_virtual.env_object_identifier(node) -%}
        {%- set pattern = '(?<![0-9A-Za-z_$])' ~ modules.re.escape(schema) ~ '(?=[`"]?\\.[`"]?' ~ modules.re.escape(ident) ~ '(?![0-9A-Za-z_$]))' -%}
        {%- set result.sql = modules.re.sub(pattern, dbt_virtual.env_placeholder_token(key), result.sql) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {#- Other references qualified with the environment's default schema: previous behaviour. -#}
  {{ return(dbt_virtual.env_schema_placeholder_sql(result.sql, dbt_virtual.env_schema_for(env_name, none))) }}
{%- endmacro %}


{% macro resolve_env_placeholders_sql(sql_text, env_name) -%}
  {%- set result = namespace(sql=sql_text) -%}
  {%- for key in modules.re.findall('__DBT_VIRTUAL_ENV_SCHEMA_([0-9A-Za-z_]+?)__(?![0-9A-Za-z])', sql_text) | unique -%}
    {%- set result.sql = result.sql | replace(dbt_virtual.env_placeholder_token(key), dbt_virtual.env_schema_for(env_name, key)) -%}
  {%- endfor -%}
  {{ return(result.sql | replace('__DBT_VIRTUAL_ENV_SCHEMA__', dbt_virtual.env_schema_for(env_name, none))) }}
{%- endmacro %}


{% macro sql_string_literal(value) -%}
  {{ return(adapter.dispatch('sql_string_literal', 'dbt_virtual')(value)) }}
{%- endmacro %}

{% macro bigquery__sql_string_literal(value) -%}
  {#- dbt-bigquery's `string_literal` is `'''value'''`, which breaks on regex backslashes
      (`r'\d+'`) and on bodies that end with a quote. Escape instead. -#}
  {{ return("'" ~ (value | string | replace('\\', '\\\\') | replace("'", "\\'") | replace('\n', '\\n') | replace('\r', '\\r')) ~ "'") }}
{%- endmacro %}

{% macro databricks__sql_string_literal(value) -%}
  {{ return(dbt_virtual.bigquery__sql_string_literal(value)) }}
{%- endmacro %}

{% macro snowflake__sql_string_literal(value) -%}
  {{ return("'" ~ (value | string | replace('\\', '\\\\') | replace("'", "''")) ~ "'") }}
{%- endmacro %}

{% macro default__sql_string_literal(value) -%}
  {{ return("'" ~ (value | string | replace("'", "''")) ~ "'") }}
{%- endmacro %}


{% macro row_count_sql(relation) -%}
  {{ return(adapter.dispatch('row_count_sql', 'dbt_virtual')(relation)) }}
{%- endmacro %}

{% macro bigquery__row_count_sql(relation) -%}
  {#- `COUNT(*)` fails on tables with `require_partition_filter = true`; table metadata does not. -#}
  SELECT row_count
  FROM `{{ relation.database }}`.`{{ relation.schema }}`.__TABLES__
  WHERE table_id = '{{ relation.identifier }}'
{%- endmacro %}

{% macro default__row_count_sql(relation) -%}
  SELECT COUNT(*) FROM {{ relation }}
{%- endmacro %}


{% macro validate_routine_exists_sql(env_schema, routine_name) -%}
  {{ return(adapter.dispatch('validate_routine_exists_sql', 'dbt_virtual')(env_schema, routine_name)) }}
{%- endmacro %}

{% macro bigquery__validate_routine_exists_sql(env_schema, routine_name) -%}
  SELECT routine_name
  FROM `{{ target.database }}`.`{{ env_schema }}`.INFORMATION_SCHEMA.ROUTINES
  WHERE routine_name = '{{ routine_name }}'
  LIMIT 1
{%- endmacro %}

{% macro default__validate_routine_exists_sql(env_schema, routine_name) -%}
  SELECT '{{ routine_name }}' AS routine_name
{%- endmacro %}

