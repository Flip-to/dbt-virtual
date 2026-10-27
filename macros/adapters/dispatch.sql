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
  CREATE TABLE IF NOT EXISTS {{ target_relation }}
  CLONE {{ source_relation }}
{%- endmacro %}

{% macro default__clone_table_sql(source_relation, target_relation) -%}
  CREATE TABLE IF NOT EXISTS {{ target_relation }} AS
  SELECT * FROM {{ source_relation }}
{%- endmacro %}


{% macro create_or_replace_view_sql(view_relation, physical_relation) -%}
  {{ return(adapter.dispatch('create_or_replace_view_sql', 'dbt_virtual')(view_relation, physical_relation)) }}
{%- endmacro %}

{% macro default__create_or_replace_view_sql(view_relation, physical_relation) -%}
  CREATE OR REPLACE VIEW {{ view_relation }} AS
  SELECT * FROM {{ physical_relation }}
{%- endmacro %}


{% macro registry_relation(physical_schema, table_name) -%}
  {{ return(adapter.dispatch('registry_relation', 'dbt_virtual')(physical_schema, table_name)) }}
{%- endmacro %}

{% macro bigquery__registry_relation(physical_schema, table_name) -%}
  `{{ target.database }}`.`{{ physical_schema }}`.{{ table_name }}
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

{% macro default__upsert_environment_pointer_sql(physical_schema, env_name, model_name, resource_type, fingerprint, physical_identifier, release_id) -%}
  DELETE FROM {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  WHERE environment = '{{ env_name }}' AND model_name = '{{ model_name }}';
  INSERT INTO {{ dbt_virtual.registry_relation(physical_schema, '_environments') }} (
    environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at
  ) VALUES (
    '{{ env_name }}', '{{ model_name }}', '{{ resource_type }}', '{{ fingerprint }}', '{{ physical_identifier }}', '{{ release_id }}', CURRENT_TIMESTAMP
  )
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
  WHERE t.table_type = 'BASE TABLE'
    AND REGEXP_CONTAINS(t.table_name, r'__[0-9a-f]{8,64}$')
    AND t.creation_time < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {{ retention_days }} DAY)
    AND p.table_name IS NULL
  ORDER BY t.table_name
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
    )
  ORDER BY table_name
{%- endmacro %}
