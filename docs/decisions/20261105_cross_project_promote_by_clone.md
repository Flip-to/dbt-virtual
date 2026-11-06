---
status: accepted
kind: developer
who: "Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: docs/specs/20261027_routing_ref_modes_and_clone_seeding.md
prs: []
---

# An environment's registry and tables may live in another project, and promotion clones across them

## Context

Registry and physical tables were always `<target.database>.<physical_schema>`. To give CI and
prod least-privilege service accounts (CI writes only to a CI project and may read prod's physical
dataset; prod writes only to prod and may read the CI project), the two have to use different
projects. With one project per registry, a CI run could not see prod's pointers (no seeding from
prod, no state for `plan`), and prod could not promote tables that CI built in its own project.

## Decision

- `vars.dbt_virtual.physical_locations` maps an environment to `{database, schema}`, where its
  registry (`_environments`, `_releases`, `_intervals`, `_locks`), physical tables and environment
  views live: `{prod: {database: <prod project>, schema: <phys>}, default: {database: <ci project>, schema: <phys_ci>}}`.
  An unlisted environment (`pr_42`) uses `default`; without `default` (or without the whole
  key) it uses `{target.database, physical_schema}`, the previous behaviour. An entry may omit
  either key. Each location has its own registry.
- `get_config()` resolves the current environment's location into `physical_schema` and
  `physical_database`, so the macros that already read `physical_schema` follow it; BigQuery SQL
  uses `active_database()` where it used `target.database`. `virtual_database_name` (for
  `generate_database_name`) builds virtual nodes in that database. This composes with
  `physical_schemas_by_location` (regions): the region map overrides the schema, `physical_locations`
  the database and the default schema.
- Seeding (`seed_incremental_clones`) and `virtual_export_state` read the base environment's pointers
  and tables from the base environment's location and clone into the current one. The planner
  takes the base location's tables (`base_physical_tables`) to plan `SEED_INCREMENTAL`; an
  unchanged model of the base location is not reused in place, it is built (or seeded) in CI.
- `virtual_promote(..., source_database, source_schema)` (CLI: `promote --source-location DATABASE.SCHEMA`)
  reads the release from the source location. When it differs from `to_env`'s location it
  clones each model table `source.<model>__<fp>` into the target location if absent, copies the
  release's rows into the target `_releases` (status `promoted`, `source_environment` = the source
  environment, which records where the clone came from), points the target environment view at the
  clone and registers it in the target registry. Routine templates are replayed in the target project
  (the source project id in the template is rewritten). The source registry is not written. No new
  registry column was needed.
- Promote must run with the target of the environment it promotes to (`--target prod`), so the lock,
  registry and views use the prod service account; any other combination fails before changing anything.
  A cross-location promote needs a release: `init-env` across locations is refused.
- GC works per location on that location's registry. CI GC must not run before prod promoted the
  releases it will clone: raise `retention_days` above the longest build-to-promote delay (a
  table younger than `retention_days` is never dropped) and `retention_releases` protects the last N
  releases of every environment.

## Why

`CLONE` is zero-copy on BigQuery and the clone carries no dependency on the CI table afterwards, so
CI can be dropped or garbage collected without breaking prod. Keeping the pointer in the target
registry means prod's rollback, GC and status never need CI. Resolving the location in
`get_config()` keeps the diff in the lifecycle macros small and leaves the single-project setup
byte-identical.

## Alternatives

- Point prod's view at the CI table (no clone): prod would depend on CI's retention and
  service account. Rejected.
- One shared registry in the prod project that CI writes to: CI could write to prod. Rejected.
- Copy with `CREATE TABLE AS SELECT`: full compute and storage. Kept only for adapters without
  clone (`use_ctas_for_clone`).

## Impact

- New config key `physical_locations`; `promote --source-location`; `virtual_database_name`; the
  state file gains `base_physical_tables`.
- Verified on real BigQuery (2026-11-05, `docs/findings/20261105_cross_project_promote_by_clone_bigquery.md`):
  `CREATE TABLE ... CLONE` from the CI project into the prod project works with both datasets in US
  (`table_type = CLONE`), prod views and registry point at the prod-project clones, the routine
  templates are replayed in the prod project (the source project id is rewritten, TVF and UDF checked),
  prod keeps working after the CI datasets are dropped, CI seeds an incremental model by cloning prod's
  table, and `audit` / `diff-data` / `promote --data-diff` compare across projects. One bug found and
  fixed: the template stored in the prod `_releases` still named the CI project, so a prod rollback
  redeployed a routine reading CI; the stored template is now rewritten too (`retarget_project_in_ddl`).
- Not verified: the minimal IAM roles. The run used one human identity (`roles/editor` on the CI project,
  prod project policy unreadable), not two least-privilege service accounts. Expected need, to verify:
  CI service account `roles/bigquery.dataEditor` + `jobUser` on the CI project and
  `roles/bigquery.dataViewer` on prod's physical dataset; prod service account `dataEditor` + `jobUser`
  on prod and `dataViewer` (the clone reads the table) on the CI physical dataset. Also not verified:
  datasets in different locations (BigQuery needs the same location for a clone) and releases promoted
  before the template fix (their prod registry rows keep the CI project in the template).
- Not done: reusing an unchanged prod table in place from CI (CI builds it instead), companion
  state across locations (not restored on a cross-location promote), `init-env` across locations,
  cross-project with `physical_schemas_by_location`.
- Revisit if BigQuery rejects the cross-project clone for these datasets, or when CI needs to
  reuse prod tables without cloning.

## Who

Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive
