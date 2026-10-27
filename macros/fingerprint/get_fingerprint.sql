{% macro get_config() -%}
  {%- set defaults = {
    'enabled': true,
    'physical_schema': 'dbt_virtual_physical',
    'environment': target.name,
    'base_environment': 'prod',
    'ref_resolution': 'env_view',
    'env_schemas': {
      'staging': 'staging',
      'prod': 'prod'
    },
    'retention_releases': 5,
    'retention_days': 7
  } -%}
  {%- set user_cfg = var('dbt_virtual', {}) -%}
  {%- do defaults.update(user_cfg) -%}
  {{ return(defaults) }}
{%- endmacro %}


{% macro is_virtual_node(node) -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- if not cfg.get('enabled', true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if node is none or node is undefined -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set node_cfg = node.get('config', {}) -%}
  {%- if node_cfg.get('virtual_env') == true -%}
    {{ return(true) }}
  {%- endif -%}
  {%- set meta = node_cfg.get('meta', {}) -%}
  {%- if meta.get('virtual_env') == true -%}
    {{ return(true) }}
  {%- endif -%}
  {{ return(false) }}
{%- endmacro %}


{% macro get_current_env() -%}
  {%- set cli_env = var('_dbt_virtual_env', '') -%}
  {%- if cli_env != '' -%}
    {{ return(cli_env) }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {{ return(cfg.get('environment', target.name)) }}
{%- endmacro %}


{% macro get_env_schema(env_name=none) -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set active_env = env_name if env_name is not none else dbt_virtual.get_current_env() -%}
  {%- set env_schemas = cfg.get('env_schemas', {}) -%}
  {{ return(env_schemas.get(active_env, target.schema)) }}
{%- endmacro %}


{% macro get_fingerprint(node) -%}
  {%- set precomputed = var('_dbt_virtual_fps', {}) -%}
  {%- set model_name = node.get('name', '') -%}
  {%- if model_name in precomputed -%}
    {{ return(precomputed[model_name]) }}
  {%- endif -%}

  {#- Pure Jinja fallback when invoked via bare `dbt run` without `dbt-virtual build` -#}
  {%- set node_cfg = node.get('config', {}) -%}
  {%- set payload = (
    (node.get('raw_code', '') or node.get('raw_sql', ''))
    ~ '|' ~ (node_cfg.get('materialized', '') | string)
    ~ '|' ~ (node_cfg.get('incremental_strategy', '') | string)
    ~ '|' ~ (node_cfg.get('partition_by', '') | tojson)
    ~ '|' ~ (node_cfg.get('cluster_by', '') | tojson)
    ~ '|' ~ (node_cfg.get('virtual_env_epoch', 1) | string)
  ) -%}
  {{ return(local_md5(payload)[:12]) }}
{%- endmacro %}
