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


{% macro create_schema_if_not_exists_sql(schema_name) -%}
  {{ return(adapter.dispatch('create_schema_if_not_exists_sql', 'dbt_virtual')(schema_name)) }}
{%- endmacro %}

{% macro bigquery__create_schema_if_not_exists_sql(schema_name) -%}
  CREATE SCHEMA IF NOT EXISTS `{{ target.database }}`.`{{ schema_name }}`
{%- endmacro %}

{% macro snowflake__create_schema_if_not_exists_sql(schema_name) -%}
  CREATE SCHEMA IF NOT EXISTS {{ schema_name }}
{%- endmacro %}

{% macro default__create_schema_if_not_exists_sql(schema_name) -%}
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
    updated_at TIMESTAMP NOT NULL
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
    rolled_back_at TIMESTAMP
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
    updated_at TIMESTAMP NOT NULL
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
    rolled_back_at TIMESTAMP
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
    updated_at TIMESTAMP
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
    rolled_back_at TIMESTAMP
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


{% macro upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id) -%}
  {{ return(adapter.dispatch('upsert_environment_pointer_sql', 'dbt_virtual')(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id)) }}
{%- endmacro %}

{% macro bigquery__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id) -%}
  MERGE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} AS T
  USING (
    SELECT
      '{{ env_name }}' AS environment,
      '{{ model_name }}' AS model_name,
      '{{ resource_type }}' AS resource_type,
      '{{ fingerprint }}' AS fingerprint,
      '{{ physical_identifier }}' AS physical_identifier,
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
{%- endmacro %}

{% macro snowflake__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id) -%}
  DELETE FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  WHERE environment = '{{ env_name }}' AND model_name = '{{ model_name }}';
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} (
    environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at
  ) VALUES (
    '{{ env_name }}', '{{ model_name }}', '{{ resource_type }}', '{{ fingerprint }}', '{{ physical_identifier }}', '{{ release_id }}', CURRENT_TIMESTAMP
  )
{%- endmacro %}

{% macro default__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id) -%}
  DELETE FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  WHERE environment = '{{ env_name }}' AND model_name = '{{ model_name }}';
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} (
    environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at
  ) VALUES (
    '{{ env_name }}', '{{ model_name }}', '{{ resource_type }}', '{{ fingerprint }}', '{{ physical_identifier }}', '{{ release_id }}', CURRENT_TIMESTAMP
  )
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
    SELECT DISTINCT release_id
    FROM `{{ target.database }}`.`{{ physical_schema }}`._releases
    WHERE status IN ('promoted', 'candidate')
    QUALIFY DENSE_RANK() OVER (ORDER BY created_at DESC) <= {{ retention_releases }}
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
    SELECT DISTINCT release_id
    FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }}
    WHERE status IN ('promoted', 'candidate')
    QUALIFY DENSE_RANK() OVER (ORDER BY created_at DESC) <= {{ retention_releases }}
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

{% macro default__find_orphan_tables_sql(physical_schema, retention_releases, retention_days) -%}
  SELECT table_name
  FROM information_schema.tables
  WHERE table_schema = '{{ physical_schema }}'
    AND table_type = 'BASE TABLE'
    AND table_name LIKE '%\_\_%' ESCAPE '\'
    AND table_name NOT IN (
      SELECT REPLACE(physical_identifier, '{{ physical_schema }}.', '')
      FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
      WHERE resource_type = 'model'
      UNION
      SELECT REPLACE(physical_identifier, '{{ physical_schema }}.', '')
      FROM {{ dbt_virtual.registry_relation(physical_schema, '_releases') }}
      WHERE resource_type = 'model'
        AND status IN ('promoted', 'candidate')
    )
  ORDER BY table_name
{%- endmacro %}


{% macro drop_object_if_exists_sql(object_relation, resource_type='model') -%}
  {{ return(adapter.dispatch('drop_object_if_exists_sql', 'dbt_virtual')(object_relation, resource_type)) }}
{%- endmacro %}

{% macro bigquery__drop_object_if_exists_sql(object_relation, resource_type='model') -%}
  {%- if resource_type == 'table' -%}
    DROP TABLE IF EXISTS {{ object_relation }}
  {%- elif resource_type == 'model' -%}
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
  {%- elif resource_type == 'model' -%}
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
  {%- elif resource_type == 'model' -%}
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

