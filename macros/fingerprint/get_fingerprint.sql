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
  {%- set user_cfg = var('dbt_virtual', {}) or {} -%}
  {#- dbt renders a var only when its whole value is a string, so Jinja nested inside the
      `dbt_virtual` mapping (e.g. `environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"`)
      arrives here unrendered. Render string values ourselves. -#}
  {%- for key, value in user_cfg.items() -%}
    {%- do defaults.update({key: dbt_virtual.render_config_value(value)}) -%}
  {%- endfor -%}
  {{ return(defaults) }}
{%- endmacro %}


{% macro render_config_value(value) -%}
  {%- if value is mapping -%}
    {#- `env_schemas` may nest one mapping per environment. -#}
    {%- set rendered = {} -%}
    {%- for sub_key, sub_value in value.items() -%}
      {%- do rendered.update({sub_key: dbt_virtual.render_config_value(sub_value)}) -%}
    {%- endfor -%}
    {{ return(rendered) }}
  {%- endif -%}
  {%- if value is string and ('{{' in value or '{%' in value) and render is defined -%}
    {{ return(render(value) | trim) }}
  {%- endif -%}
  {{ return(value) }}
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
  {#- on-run-start/on-run-end hooks inherit `+virtual_env` from `models:` config but are not deployable. -#}
  {%- if dbt_virtual.node_attr(node, 'resource_type', 'model') == 'operation' -%}
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
  {#- The environment's default schema (models without a custom schema). -#}
  {{ return(dbt_virtual.env_schema_for(env_name, none)) }}
{%- endmacro %}


{% macro env_schema_for(env_name=none, schema_key=none) -%}
  {#-
    DECISION(per_custom_schema_env_datasets): where an environment exposes a virtual object.
    `env_schemas.<env>` is either one schema (every custom schema collapses into it) or a mapping
    `{default: <schema>, <custom_schema>: <schema>, ...}`; an unlisted custom schema `c` becomes
    `<default>_c`, like dbt's default `generate_schema_name`.
    Unmapped environments (e.g. `pr_42`) get one schema, `<base_env_default_schema>_<env>`, as
    documented in docs/guides/configuration_reference.md.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set active_env = env_name if env_name is not none else dbt_virtual.get_current_env() -%}
  {%- set env_schemas = cfg.get('env_schemas', {}) or {} -%}
  {%- set key = (schema_key | string | trim) if schema_key is not none else '' -%}
  {%- if key == 'default' or key == 'None' -%}
    {%- set key = '' -%}
  {%- endif -%}
  {%- if active_env in env_schemas -%}
    {%- set mapping = env_schemas[active_env] -%}
    {%- if mapping is mapping -%}
      {%- set default_schema = mapping.get('default', target.schema) -%}
      {%- if key == '' -%}
        {{ return(default_schema) }}
      {%- elif key in mapping -%}
        {{ return(mapping[key]) }}
      {%- endif -%}
      {{ return(default_schema ~ '_' ~ key) }}
    {%- endif -%}
    {{ return(mapping) }}
  {%- endif -%}
  {%- set base_env = cfg.get('base_environment', 'prod') -%}
  {%- set base_schema = dbt_virtual.env_schema_for(base_env, none) if (base_env in env_schemas and base_env != active_env) else target.schema -%}
  {%- set env_suffix = modules.re.sub('[^0-9A-Za-z_]', '_', active_env | string) if modules is defined else (active_env | string) -%}
  {{ return(base_schema ~ '_' ~ env_suffix) }}
{%- endmacro %}


{% macro node_schema_key(node) -%}
  {#- The custom schema a node is configured with (`config.schema`), or none. -#}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- set key = dbt_virtual.node_attr(node_cfg, 'schema', none) -%}
  {{ return(none if (key is none or (key | string | trim) == '') else (key | string | trim)) }}
{%- endmacro %}


{% macro env_object_identifier(node) -%}
  {#- Name of a virtual node's object in an environment schema: the env view of a table model
      is named after the model; views and routines keep their alias. -#}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- if dbt_virtual.node_attr(node_cfg, 'materialized', 'view') in ['table', 'incremental'] -%}
    {{ return(dbt_virtual.node_attr(node, 'name', '')) }}
  {%- endif -%}
  {{ return(dbt_virtual.node_attr(node, 'alias', none) or dbt_virtual.node_attr(node, 'name', '')) }}
{%- endmacro %}


{% macro virtual_node_by_name(model_name) -%}
  {%- if graph is defined and graph.nodes is defined -%}
    {%- for node in graph.nodes.values() -%}
      {%- if dbt_virtual.node_attr(node, 'name', '') == model_name and dbt_virtual.is_virtual_node(node) -%}
        {{ return(node) }}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(none) }}
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
      {%- if p_node is not none and (dbt_virtual.is_virtual_node(p_node) or dbt_virtual.node_attr(p_node, 'resource_type', '') == 'model') -%}
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
