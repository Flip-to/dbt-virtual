---
status: accepted
kind: developer
who: "Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# A physical schema and a registry per BigQuery location

## Context

dbt-virtual kept one physical schema and one registry (`_environments`, `_releases`, `_intervals`,
`_locks`) per project. A BigQuery view cannot read a table in another location, `CREATE TABLE ...
CLONE` needs the source and destination in the same location, and a query job runs in one
location. A project with EU and US datasets (models routed to US datasets by a custom
`generate_schema_name`, or a target with `location: US`) could not virtualize its US models.

## Decision

- `vars.dbt_virtual.physical_schemas_by_location` maps a location to its physical schema, for
  example `{EU: rt_virtual_physical, US: rt_virtual_physical_us}`. The registry of a location lives
  in that location's physical schema. `physical_schema` stays the schema of every location not
  listed; it is also a registry (iterated) only when the project sets it explicitly. Without the
  new key nothing changes.
- A node's location is `+virtual_location` (config or `meta`), else the profile's `location`
  (`target.location`), else none. dbt-bigquery has no model-level `location` config (read in
  `dbt/adapters/bigquery/impl.py`, `BigqueryConfig`, 2026-11-05), so only `virtual_location` is
  read. Case is ignored. A location with no entry and no explicit `physical_schema`, or a model
  with no location in that setup, is a compiler error.
- Routing: `virtual_schema_name` returns the physical schema of the node's location;
  `virtual_ref` recognizes the physical schemas of every location. Environment views and routines
  go to the environment dataset of the model's custom schema, so a US model needs `schema='us'`
  mapped in `env_schemas.<env>` to a US dataset.
- `schema_locations: {<dataset>: <location>}` (optional) declares where datasets are. When a
  virtual model's physical dataset, or the dataset of any environment in `env_schemas` or the
  current one, is declared in another location than the model, parsing fails with a message that
  names the model, both locations and the fix. Undeclared datasets are not checked.
- With `schema_locations` set, an unmapped environment (`pr_42`) gets one schema per base schema
  (`rt_pr_42`, `rt_us_pr_42`) instead of one for all, so each object stays in its base schema's
  location.
- Operations that read a registry (`virtual_promote`, `virtual_rollback`, `virtual_audit`,
  `virtual_gc`, `virtual_drop_env`, `virtual_snapshot_release`, locks, `virtual_export_state`)
  take `location=none` and, when it is none in a multi-location project, run once per location
  (`physical_locations()`). Promote and rollback skip a location that has nothing for the release
  and fail only when no location has it. Status merges the state of all registries.
  Hooks, seeding, `virtual_sync_reused_views` and `virtual_adopt` use the registry of the node's
  location.
- Datasets the package creates (registry, environment schemas) are created with
  `CREATE SCHEMA IF NOT EXISTS ... OPTIONS (location = '<location>')` in a multi-location project;
  dbt-bigquery's `create_schema` is plain `create schema if not exists` with no location (read in
  `dbt/include/global_project/macros/adapters/schema.sql`, 2026-11-05).
- Composition: region is one more dimension of where a physical table lives. Code asks
  `physical_schema_for(location)` / `physical_schema_for_node(node)` and
  `physical_locations()`, defined in `macros/routing/locations.sql`
  (`DECISION(physical_location_per_region)`); per-environment `physical_locations` (cross-project
  promotion) can wrap these two macros to add the project dimension.

## Why

Per-location registries keep every statement inside one location, the only thing BigQuery allows.
A mapping from location to schema reuses the existing "registry lives in the physical schema"
rule, so single-location projects keep their layout. The mismatch check turns a late BigQuery
runtime error ("cannot read table in a different location") into a parse error.

## Alternatives

- A location column in one registry: the registry itself would be in one location and jobs that
  touch it could not run next to data in another.
- Derive the location from the dataset through the API at parse time: needs credentials and a
  network call during `parse`; `schema_locations` is static config.
- A model-level `location` config: not a dbt-bigquery key, so it would be an undocumented extra.

## Impact

- New config keys `physical_schemas_by_location` and `schema_locations`, new model key
  `virtual_location`, new macros file, `location` argument on registry operations.
- Taking a lock per location is not atomic across locations: a failure in the second location
  leaves the first lease until its TTL.
- The release id is shared across locations; a release exists in the registries of the locations
  whose models it built.
- Not verified on real BigQuery: `OPTIONS (location = ...)` from a job in another location, and
  that a profile with `location` set still reaches a registry in another location (a dbt job has
  one location; leave the profile `location` unset, or run each location with its own target).
  Verified on DuckDB with two physical schemas (`tests/test_dbt_duckdb_multi_region.py`).
- Revisit when a project needs a model whose location differs from every listed one.

## Who

Developer decision. Made by an agent (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66),
approved by Yuri Olive.
