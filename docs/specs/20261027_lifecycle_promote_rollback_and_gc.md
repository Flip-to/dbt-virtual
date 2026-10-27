---
status: accepted
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Lifecycle operations, state registry, rollback, and janitor GC

## Problem

Content-addressed physical tables (`<model>__<fp>`) require a persistent state registry in the warehouse so that:
1. `staging` knows which `prod` table to `CLONE` when seeding a new incremental fingerprint.
2. `promote` knows which physical tables and routines belong to a verified release.
3. `rollback` knows which previous release to restore on gate failure.
4. `gc` knows which physical tables are still referenced by active environments or recent releases so it can safely drop orphaned tables.

## Decision

1. **Warehouse State Registry (`<physical_schema>._environments` and `<physical_schema>._releases`)**:
   - `_environments` stores the current active pointer `(environment, model_name) -> (fingerprint, physical_identifier, release_id, updated_at)`.
   - `_releases` stores immutable release rows `(release_id, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status)`.
2. **Lifecycle Operations (`macros/lifecycle/operations.sql` & `src/dbt_virtual/cli.py`)**:
   - `dbt-virtual plan`: Compiles manifest and outputs `.dbt_virtual/fingerprints.json`.
   - `dbt-virtual build`: Injects `_dbt_virtual_fps`, `_dbt_virtual_env`, and `_dbt_virtual_release` into `dbt build`.
   - `dbt-virtual promote` (`virtual_promote`): Repoints target environment views and deploys routines from the source environment / release without rebuilding.
   - `dbt-virtual rollback` (`virtual_rollback`): Repoints views and redeploys routines from the prior `promoted` release in `_releases`.
   - `dbt-virtual gc` (`virtual_gc`): Drops `<model>__<fp>` tables in `<physical_schema>` older than `retention_days` that are not in `_environments` or the last `retention_releases` releases.

## Alternatives

- **Store state in GCS / local JSON files only**: Rejected; keeping `_environments` and `_releases` as tables inside `<physical_schema>` allows pure SQL macros (`clone_if_new_fp`, `virtual_promote`, `virtual_rollback`, `virtual_gc`) to query and update state directly inside the warehouse without external storage dependencies.

## Rollout

1. Ship `ensure_registry_tables_sql` in `macros/adapters/dispatch.sql`.
2. Ship `virtual_promote`, `virtual_rollback`, and `virtual_gc` in `macros/lifecycle/operations.sql`.
3. Ship CLI commands in `src/dbt_virtual/cli.py`.

## Verification

- End-to-end test in `scripts/run_emulator_e2e.py` verifying `_environments` and `_releases` creation, view promotion, and rollback on `Flip-to/bigquery-emulator`.
