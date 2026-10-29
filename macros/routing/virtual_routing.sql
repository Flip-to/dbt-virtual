{% macro virtual_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
    {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', 'view') -%}
    {%- set cfg = dbt_virtual.get_config() -%}
    {%- if mat in ['table', 'incremental'] -%}
      {{ return(cfg.get('physical_schema')) }}
    {%- else -%}
      {#- Routines (table_function, function, procedure) and views deploy to the environment schema -#}
      {{ return(dbt_virtual.get_env_schema()) }}
    {%- endif -%}
  {%- endif -%}

  {%- if custom_schema_name is none -%}
    {{ return(target.schema) }}
  {%- else -%}
    {{ return(target.schema ~ '_' ~ (custom_schema_name | trim)) }}
  {%- endif -%}
{%- endmacro %}


{% macro virtual_alias_name(custom_alias_name, node) -%}
  {%- set base_name = (custom_alias_name | trim) if custom_alias_name is not none else dbt_virtual.node_attr(node, 'name', '') -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
    {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', 'view') -%}
    {%- if mat in ['table', 'incremental'] -%}
      {%- if base_name != dbt_virtual.node_attr(node, 'name', '') -%}
        {{ exceptions.raise_compiler_error(
          "dbt-virtual: custom aliases are not supported for virtual table models. Remove alias from model '"
          ~ dbt_virtual.node_attr(node, 'name', '') ~ "' so refs resolve to the environment view."
        ) }}
      {%- endif -%}
      {%- set fp = dbt_virtual.get_fingerprint(node) -%}
      {{ return(base_name ~ '__' ~ fp) }}
    {%- endif -%}
  {%- endif -%}
  {{ return(base_name) }}
{%- endmacro %}


{% macro virtual_ref() -%}
  {#-
    DECISION(default_ref_resolution_env_view):
    Configurable ref() resolution for virtualized models (see docs/specs/20261027_routing_ref_modes_and_clone_seeding.md):
    - Mode A ('env_view', default): resolves virtualized table models to <env_schema>.<model_name>
      so downstream views & TVFs read the virtual environment view and swap instantaneously on promote/rollback.
    - Mode B ('physical'): resolves directly to <physical_schema>.<model_name>__<fp>.
    - Mode C ('hybrid'): physical tables bind to <physical_schema>.<model_name>__<fp>, routines/views bind to <env_schema>.<model_name>.
  -#}
  {%- set raw_rel = builtins.ref(*varargs, **kwargs) -%}
  {%- if not execute -%}
    {{ return(raw_rel) }}
  {%- endif -%}

  {%- set cfg = dbt_virtual.get_config() -%}
  {%- if not cfg.get('enabled', true) -%}
    {{ return(raw_rel) }}
  {%- endif -%}

  {%- set mode = cfg.get('ref_resolution', 'env_view') -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}

  {#- Only fingerprint-shaped suffixes identify virtual tables; arbitrary names may contain `__`. -#}
  {%- set identifier_parts = raw_rel.identifier.rsplit('__', 1) -%}
  {%- set fingerprint = identifier_parts[-1] -%}
  {%- set suffix_check = namespace(valid=identifier_parts | length == 2 and fingerprint | length >= 8 and fingerprint | length <= 64) -%}
  {%- if suffix_check.valid -%}
    {%- for char in fingerprint -%}
      {%- if char not in '0123456789abcdef' -%}
        {%- set suffix_check.valid = false -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set virtual_relation = namespace(
    valid=suffix_check.valid,
    logical_name=(identifier_parts[0] if suffix_check.valid else '')
  ) -%}
  {#- physical_version may be an explicit non-hex suffix; only trust it when a manifest node declares it. -#}
  {%- if raw_rel.schema == phys_schema and not virtual_relation.valid and graph is defined and graph.nodes is defined -%}
    {%- for candidate in graph.nodes.values() -%}
      {%- if dbt_virtual.is_virtual_node(candidate) -%}
        {%- set candidate_cfg = dbt_virtual.node_attr(candidate, 'config', {}) -%}
        {%- set candidate_mat = dbt_virtual.node_attr(candidate_cfg, 'materialized', 'view') -%}
        {%- set pinned_version = dbt_virtual.node_attr(candidate_cfg, 'physical_version', none) -%}
        {%- set candidate_name = dbt_virtual.node_attr(candidate, 'name', '') -%}
        {%- if candidate_mat in ['table', 'incremental'] and pinned_version is not none and raw_rel.identifier == candidate_name ~ '__' ~ pinned_version -%}
          {%- set virtual_relation.valid = true -%}
          {%- set virtual_relation.logical_name = candidate_name -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- if raw_rel.schema == phys_schema and virtual_relation.valid -%}
    {%- set logical_name = virtual_relation.logical_name -%}
    {%- if mode == 'env_view' -%}
      {{ return(api.Relation.create(
        database=raw_rel.database,
        schema=dbt_virtual.get_env_schema(),
        identifier=logical_name,
        type='view'
      )) }}
    {%- elif mode == 'hybrid' -%}
      {%- set caller_cfg = dbt_virtual.node_attr(model, 'config', {}) if model is defined else {} -%}
      {%- set caller_mat = dbt_virtual.node_attr(caller_cfg, 'materialized', 'view') -%}
      {%- if caller_mat in ['table_function', 'function', 'procedure', 'view'] -%}
        {{ return(api.Relation.create(
          database=raw_rel.database,
          schema=dbt_virtual.get_env_schema(),
          identifier=logical_name,
          type='view'
        )) }}
      {%- endif -%}
    {%- endif -%}
  {%- endif -%}

  {{ return(raw_rel) }}
{%- endmacro %}
