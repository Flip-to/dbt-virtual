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
