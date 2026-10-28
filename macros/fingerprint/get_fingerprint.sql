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


{% macro node_attr(obj, key, default=none) -%}
  {%- if obj is none or obj is undefined -%}
    {{ return(default) }}
  {%- endif -%}
  {%- if obj is mapping -%}
    {{ return(obj.get(key, default)) }}
  {%- endif -%}
  {%- if obj[key] is defined -%}
    {{ return(obj[key]) }}
  {%- endif -%}
  {%- if key == 'virtual_env' or key == 'virtual_env_epoch' or key == 'parameters' or key == 'returns' -%}
    {%- set extra = dbt_virtual.node_attr(obj, '_extra', {}) -%}
    {%- if extra is mapping and key in extra -%}
      {{ return(extra.get(key, default)) }}
    {%- endif -%}
  {%- endif -%}
  {{ return(default) }}
{%- endmacro %}


{% macro is_virtual_node(node) -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- if not cfg.get('enabled', true) -%}
    {{ return(false) }}
  {%- endif -%}
  {%- if node is none or node is undefined -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- if dbt_virtual.node_attr(node_cfg, 'virtual_env', false) == true -%}
    {{ return(true) }}
  {%- endif -%}
  {%- set meta = dbt_virtual.node_attr(node_cfg, 'meta', {}) -%}
  {%- if dbt_virtual.node_attr(meta, 'virtual_env', false) == true -%}
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


{% macro compute_node_self_payload(node) -%}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- set raw_code = dbt_virtual.node_attr(node, 'raw_code', '') or dbt_virtual.node_attr(node, 'raw_sql', '') -%}
  {%- set payload = (
    raw_code
    ~ '|' ~ (dbt_virtual.node_attr(node_cfg, 'materialized', '') | string)
    ~ '|' ~ (dbt_virtual.node_attr(node_cfg, 'incremental_strategy', '') | string)
    ~ '|' ~ (dbt_virtual.node_attr(node_cfg, 'partition_by', '') | tojson)
    ~ '|' ~ (dbt_virtual.node_attr(node_cfg, 'cluster_by', '') | tojson)
    ~ '|' ~ (dbt_virtual.node_attr(node_cfg, 'virtual_env_epoch', 1) | string)
  ) -%}
  {{ return(local_md5(payload)[:12]) }}
{%- endmacro %}


{% macro get_fingerprint(node, depth=0) -%}
  {%- set precomputed = var('_dbt_virtual_fps', {}) -%}
  {%- set model_name = dbt_virtual.node_attr(node, 'name', '') -%}
  {%- if model_name in precomputed -%}
    {{ return(precomputed[model_name]) }}
  {%- endif -%}

  {#- Pure Jinja Merkle-DAG fallback when invoked via bare `dbt run` without `dbt-virtual build`:
      Walks `node.depends_on.nodes` in `graph.nodes` (up to depth 8) when available so upstream
      virtualized parent changes propagate into downstream fingerprints automatically. -#}
  {%- set self_hash = dbt_virtual.compute_node_self_payload(node) -%}
  {%- set parent_parts = [] -%}
  {%- if execute and graph is defined and graph.nodes is defined and depth < 8 -%}
    {%- set dep_obj = dbt_virtual.node_attr(node, 'depends_on', {}) -%}
    {%- set parent_uids = (dbt_virtual.node_attr(dep_obj, 'nodes', []) or []) | sort -%}
    {%- for p_uid in parent_uids -%}
      {%- set p_node = graph.nodes.get(p_uid) -%}
      {%- if p_node is not none and dbt_virtual.is_virtual_node(p_node) -%}
        {%- set p_fp = dbt_virtual.get_fingerprint(p_node, depth=depth + 1) -%}
        {%- do parent_parts.append(p_uid ~ '=' ~ p_fp) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- if (parent_parts | length) > 0 -%}
    {{ return(local_md5(self_hash ~ '|' ~ (parent_parts | join(',')))[:12]) }}
  {%- endif -%}
  {{ return(self_hash) }}
{%- endmacro %}
