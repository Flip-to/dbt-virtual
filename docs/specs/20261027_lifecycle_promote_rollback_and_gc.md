---
status: implemented
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Lifecycle operations, state registry, rollback, and janitor GC

## Problem

Content-addressed physical tables (`<model>__<fp>`) require a persistent state registry in the warehouse so that:
1. `staging` knows which `prod` table to `CLONE` when seeding a new incremental fingerprint.
2. `promote` knows which physical tables and routines belong to a verified release, which models are already promoted at the same fingerprint, and which removed models need to be demoted.
3. `audit` can verify candidate physical tables are non-empty and within row-drop thresholds before promotion.
4. `rollback` knows which previous release to restore on gate failure.
5. Concurrent CI runs cannot mutate the same environment simultaneously (`_locks`), and incremental backfills can track processed `[start_ts, end_ts]` ranges (`_intervals`).
6. `gc` knows which physical tables are still referenced by active environments or recent releases so it can safely drop orphaned tables.

## Decision

1. **Warehouse State Registry (`<physical_schema>._environments`, `_releases`, `_locks`, and `_intervals`)**:
   - `_environments` stores the current active pointer `(environment, model_name) -> (resource_type, fingerprint, physical_identifier, release_id, updated_at)`.
   - `_releases` stores immutable release rows `(release_id, source_environment, target_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status, created_at, promoted_at, rolled_back_at)`.
   - `_locks` stores TTL-backed concurrency leases `(environment, owner, acquired_at, expires_at)`.
   - `_intervals` stores processed backfill windows `(model_name, fingerprint, physical_identifier, start_ts, end_ts, recorded_at)`.
2. **Lifecycle Operations (`macros/lifecycle/operations.sql` & `src/dbt_virtual/cli.py`)**:
   - `dbt-virtual plan`: Reads `target/manifest.json` (and optional `--state-file`), computes Merkle-DAG fingerprints to `target/virtual_fingerprints.json`, and classifies each node (`REUSE`, `SEED_INCREMENTAL`, `FULL_BUILD`, `ROUTINE_DEPLOY`).
   - `dbt-virtual build`: Syncs views for `REUSE` models in 0 compute via `virtual_sync_reused_views` and invokes `dbt build` only for changed/new models with `_dbt_virtual_fps`, `_dbt_virtual_release`, `_dbt_virtual_start`, and `_dbt_virtual_end`.
   - `dbt-virtual diff`: Compares column schemas between base and candidate manifests, classifying changes as `NON_BREAKING`, `TYPE_WIDENED` (compatible widening), or `BREAKING`.
   - `dbt-virtual audit` (`virtual_audit`): Pre-promotion quality gate checking candidate tables for non-empty row counts and max row-drop percentage vs. `to_env`.
   - `dbt-virtual promote` (`virtual_promote`): Optionally runs `virtual_audit`, verifies physical tables exist, skips unchanged fingerprints (partial promotion), deploys in strict tier order (`1 -> 2 -> 3 -> 4`), and demotes (`drop_object_if_exists_sql`) obsolete views/routines deleted from the release.
   - `dbt-virtual rollback` (`virtual_rollback`): Repoints views and redeploys routines from the prior `promoted` release in `_releases`.
   - `dbt-virtual status` (`virtual_export_state`): Exports live warehouse state (`env_state`, `base_env_state`, `existing_physical_tables`).
   - `dbt-virtual init-env` / `drop-env` (`virtual_init_env`, `virtual_drop_env`): Bootstraps and tears down ephemeral PR environments.
   - `dbt-virtual gc` (`virtual_gc`): Drops `<model>__<fp>` tables in `<physical_schema>` older than `retention_days` that are not in `_environments` or the last `retention_releases` releases.

## Alternatives

- **Store state in GCS / local JSON files only**: Rejected; keeping `_environments`, `_releases`, `_locks`, and `_intervals` as tables inside `<physical_schema>` allows pure SQL macros (`clone_if_new_fp`, `virtual_promote`, `virtual_audit`, `virtual_rollback`, `virtual_gc`) to query and update state directly inside the warehouse without external storage dependencies.

## Rollout

1. Ship `ensure_registry_tables_sql` in `macros/adapters/dispatch.sql`.
2. Ship `virtual_promote`, `virtual_audit`, `virtual_rollback`, `virtual_acquire_lock`, `virtual_release_lock`, `virtual_export_state`, `virtual_init_env`, `virtual_drop_env`, and `virtual_gc` in `macros/lifecycle/operations.sql`.
3. Ship CLI commands in `src/dbt_virtual/cli.py` and planner/schema-differ in `src/dbt_virtual/planner.py`.

## Verification

- End-to-end tests in `scripts/run_emulator_e2e.py`, `scripts/run_snowflake_emulator_e2e.py`, `tests/test_duckdb_e2e.py`, and `tests/test_dbt_duckdb_project_e2e.py`.
