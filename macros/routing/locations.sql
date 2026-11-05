{#-
  DECISION(physical_location_per_region): multi-region (BigQuery location) support.

  A BigQuery view cannot read a table in another location and `CLONE` needs the same location, so
  a project with datasets in several locations needs one physical schema and one registry
  (`_environments`, `_releases`, `_intervals`, `_locks`) per location. `physical_schemas_by_location`
  maps a location to its physical schema; `physical_schema` stays the schema for every location
  not listed. Region is one more dimension of where a physical table lives: macros that need that
  place go through `physical_schema_for(location)` / `physical_schema_for_node(node)` and loop over
  `physical_locations()`, so a per-environment placement (`physical_locations`) composes by
  wrapping these two macros.

  Config (`vars.dbt_virtual`):
    physical_schemas_by_location: {EU: rt_virtual_physical, US: rt_virtual_physical_us}
    schema_locations: {analytics: EU, analytics_us: US}   # optional, enables the mismatch check
-#}

{% macro location_key(location) -%}
  {{ return((location | string | trim | lower) if location is not none else '') }}
{%- endmacro %}


{% macro physical_schemas_by_location() -%}
  {#- `physical_schemas_by_location` with lower-cased location keys, e.g. {'eu': 'rt_virtual_physical'}. -#}
  {%- set mapping = dbt_virtual.get_config().get('physical_schemas_by_location', {}) or {} -%}
  {%- set normalized = {} -%}
  {%- for location, schema in mapping.items() -%}
    {%- do normalized.update({dbt_virtual.location_key(location): schema}) -%}
  {%- endfor -%}
  {{ return(normalized) }}
{%- endmacro %}


{% macro is_multi_location() -%}
  {{ return((dbt_virtual.physical_schemas_by_location() | length) > 0) }}
{%- endmacro %}


{% macro default_physical_schema_is_set() -%}
  {#- True when the project names `physical_schema` itself (not the package default). -#}
  {%- set user_cfg = var('dbt_virtual', {}) or {} -%}
  {{ return('physical_schema' in user_cfg) }}
{%- endmacro %}


{% macro node_location(node) -%}
  {#- Location of a node's datasets: `virtual_location` (config or meta), else the profile's
      `location`, else none. -#}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- set location = dbt_virtual.node_attr(node_cfg, 'virtual_location', none) -%}
  {%- if location is none -%}
    {%- set location = dbt_virtual.node_attr(dbt_virtual.node_attr(node_cfg, 'meta', {}), 'virtual_location', none) -%}
  {%- endif -%}
  {%- if location is none or (location | string | trim) == '' -%}
    {%- set location = target.get('location') if target is mapping else none -%}
  {%- endif -%}
  {{ return(none if (location is none or (location | string | trim) == '') else (location | string | trim)) }}
{%- endmacro %}


{% macro physical_schema_for(location=none) -%}
  {#- Physical schema (and registry schema) for a location. `none` / 'default' is `physical_schema`. -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set by_location = dbt_virtual.physical_schemas_by_location() -%}
  {%- set key = dbt_virtual.location_key(location) -%}
  {%- if key in by_location -%}
    {{ return(by_location[key]) }}
  {%- endif -%}
  {%- if (by_location | length) > 0 and key not in ['', 'default'] and not dbt_virtual.default_physical_schema_is_set() -%}
    {{ exceptions.raise_compiler_error(
      "dbt-virtual: location '" ~ location ~ "' has no entry in vars.dbt_virtual.physical_schemas_by_location ("
      ~ (by_location.keys() | list | join(', ')) ~ ") and no default `physical_schema` is set."
    ) }}
  {%- endif -%}
  {{ return(cfg.get('physical_schema')) }}
{%- endmacro %}


{% macro physical_schema_for_node(node) -%}
  {%- set location = dbt_virtual.node_location(node) -%}
  {%- if location is none and dbt_virtual.is_multi_location() and not dbt_virtual.default_physical_schema_is_set() -%}
    {{ exceptions.raise_compiler_error(
      "dbt-virtual: model '" ~ dbt_virtual.node_attr(node, 'name', '') ~ "' has no location: set `+virtual_location`, "
      ~ "the target's `location`, or a default `physical_schema` next to physical_schemas_by_location."
    ) }}
  {%- endif -%}
  {{ return(dbt_virtual.physical_schema_for(location)) }}
{%- endmacro %}


{% macro physical_locations() -%}
  {#- Locations that own a registry, in config order; 'default' stands for `physical_schema` when
      the project names it and no listed location already uses that schema. Without
      `physical_schemas_by_location`: [none], the single registry of a one-location project. -#}
  {%- if not dbt_virtual.is_multi_location() -%}
    {{ return([none]) }}
  {%- endif -%}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set mapping = cfg.get('physical_schemas_by_location', {}) or {} -%}
  {%- set locations = mapping.keys() | list -%}
  {%- if dbt_virtual.default_physical_schema_is_set() and (cfg.get('physical_schema') not in (mapping.values() | list)) -%}
    {%- do locations.append('default') -%}
  {%- endif -%}
  {{ return(locations) }}
{%- endmacro %}


{% macro physical_schemas() -%}
  {#- Every physical schema of the project. -#}
  {%- set schemas = [] -%}
  {%- for location in dbt_virtual.physical_locations() -%}
    {%- set schema = dbt_virtual.physical_schema_for(location) -%}
    {%- if schema not in schemas -%}
      {%- do schemas.append(schema) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(schemas) }}
{%- endmacro %}


{% macro node_in_location(node, location) -%}
  {#- Whether a node's physical objects belong to `location`'s registry (any node when `location` is none). -#}
  {%- if location is none -%}
    {{ return(true) }}
  {%- endif -%}
  {{ return(dbt_virtual.physical_schema_for_node(node) == dbt_virtual.physical_schema_for(location)) }}
{%- endmacro %}


{% macro create_location_for(location) -%}
  {#- The location to create a dataset in: only set for a multi-location project and a real location. -#}
  {%- if dbt_virtual.is_multi_location() and dbt_virtual.location_key(location) not in ['', 'default'] -%}
    {{ return(location) }}
  {%- endif -%}
  {{ return(none) }}
{%- endmacro %}


{% macro schema_location(schema_name) -%}
  {#- Location configured for a dataset in `schema_locations`, or none when not declared. -#}
  {%- set declared = dbt_virtual.get_config().get('schema_locations', {}) or {} -%}
  {%- for name, location in declared.items() -%}
    {%- if (name | string | lower) == (schema_name | string | lower) -%}
      {{ return(location) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return(none) }}
{%- endmacro %}


{% macro assert_node_location(node) -%}
  {#-
    Compile-time check: a virtual model's physical dataset and the datasets of every environment
    that exposes it must be in one location, because a BigQuery view cannot read a table from
    another location. Only datasets whose location is declared (`schema_locations`, or implied by
    `physical_schemas_by_location`) can be checked.
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set location = dbt_virtual.node_location(node) -%}
  {%- if location is none or not (dbt_virtual.is_multi_location() or (cfg.get('schema_locations', {}) or {}) | length > 0) -%}
    {{ return('') }}
  {%- endif -%}
  {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
  {%- set mat = dbt_virtual.node_attr(node_cfg, 'materialized', 'view') -%}
  {%- set key = dbt_virtual.node_schema_key(node) -%}
  {%- set checks = [] -%}
  {%- if mat in ['table', 'incremental'] -%}
    {%- do checks.append(['physical schema', dbt_virtual.physical_schema_for(location)]) -%}
  {%- endif -%}
  {%- set envs = (cfg.get('env_schemas', {}) or {}).keys() | list -%}
  {%- set current_env = dbt_virtual.get_current_env() -%}
  {%- if current_env not in envs -%}
    {%- do envs.append(current_env) -%}
  {%- endif -%}
  {%- for env in envs -%}
    {%- do checks.append(["environment '" ~ env ~ "' schema", dbt_virtual.env_schema_for(env, key)]) -%}
  {%- endfor -%}
  {%- for check in checks -%}
    {%- set declared = dbt_virtual.schema_location(check[1]) -%}
    {%- if declared is none and check[0] == 'physical schema' and (dbt_virtual.location_key(location) in dbt_virtual.physical_schemas_by_location()) -%}
      {%- set declared = location -%}
    {%- endif -%}
    {%- if declared is not none and dbt_virtual.location_key(declared) != dbt_virtual.location_key(location) -%}
      {{ exceptions.raise_compiler_error(
        "dbt-virtual: model '" ~ dbt_virtual.node_attr(node, 'name', '') ~ "' is in location " ~ location
        ~ " but its " ~ check[0] ~ " '" ~ check[1] ~ "' is in " ~ declared ~ ". A view cannot read a table "
        ~ "in another location and CLONE needs the same location: route this model to a dataset in "
        ~ location ~ " (a custom `schema` mapped in `env_schemas`, see docs/guides/configuration_reference.md)."
      ) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return('') }}
{%- endmacro %}
