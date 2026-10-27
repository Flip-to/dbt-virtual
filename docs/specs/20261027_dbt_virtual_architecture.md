---
status: accepted
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Virtual data environments for dbt (`dbt-virtual` master architecture)

## Problem

Teams running dbt on BigQuery (or Snowflake/Databricks) face three structural limitations:

1. **Promoting `staging` to `prod` requires a second physical build.** Even after a staging build has run and passed gates on real data, standard dbt requires running `dbt build` again in production—paying double warehouse compute and risking data drift between the two builds.
2. **Rollback is slow and incomplete for routines.** Rolling back data requires restoring tables from clones or time travel, while BigQuery routines (`CREATE TABLE FUNCTION`, `CREATE FUNCTION`, `CREATE PROCEDURE`) have no time travel at all.
3. **Adopting SQLMesh breaks dbt-native ecosystems.** SQLMesh's virtual data environments solve 1 and 2 via content-addressed physical tables and view swaps, but migrating off dbt breaks custom Jinja materializations (`procedure`, `table_function`, `function`), package manifest hooks, `dbt-checkpoint`, the `sqlfluff` dbt templater, Elementary, and Slim CI.

## Decision

Build `dbt-virtual` (`Flip-to/dbt-virtual`) as a 100% open-source (Apache-2.0) hybrid dbt macro package + lightweight Python CLI (`dbt-virtual`):

- **Content-addressed physical tables** (`<physical_schema>.<model>__<fp>`) driven by deterministic Merkle-DAG fingerprinting (`20261027_merkle_dag_fingerprinting.md`).
- **Default `env_view` `ref()` resolution (Option A)** with support for `physical`, `hybrid`, and `explicit` modes (`20261027_routing_ref_modes_and_clone_seeding.md` and decision [20261027_default_ref_resolution_env_view.md](../decisions/20261027_default_ref_resolution_env_view.md)).
- **Zero-copy `CLONE` incremental seeding** before incremental models execute (`20261027_routing_ref_modes_and_clone_seeding.md`).
- **First-class routine virtualization** (`table_function`, `function`, `procedure` materializations + `tvf_test_ref`) with environment-tokenized DDL capture (`20261027_routine_virtualization_and_tvf_testing.md`).
- **Lifecycle state registry (`_environments` & `_releases`) and CLI/macro operations** (`plan`, `build`, `promote`, `rollback`, `diff`, `gc`) (`20261027_lifecycle_promote_rollback_and_gc.md`).
- **Adapter-dispatched SQL** with BigQuery first and BigQuery emulator E2E verification (`20261027_adapter_dispatch_and_emulator_ci.md` and decision [20261027_hybrid_package_and_bigquery_first.md](../decisions/20261027_hybrid_package_and_bigquery_first.md)).

## Alternatives

- **Adopt SQLMesh directly**: Rejected for existing dbt projects with custom Jinja materializations and dbt-manifest-dependent CI.
- **No virtual layer (clone-restore only)**: Rejected; every promotion requires a second build and rollback is a data restore rather than an instant view pointer swap.

## Rollout

1. **Milestone 1**: Repository scaffolding, specs, decisions, tech debt tracking, `check_docs_layout.py`, Merkle-DAG fingerprinting engine (`src/dbt_virtual/fingerprint.py`), and unit tests.
2. **Milestone 2**: dbt routing macros, `clone_if_new_fp` pre-hook, `sync_env_view` post-hook, and BigQuery `table_function` / `function` / `procedure` materializations.
3. **Milestone 3**: `virtual_promote`, `virtual_rollback`, `virtual_gc` operations and `dbt-virtual` CLI (`src/dbt_virtual/cli.py`).
4. **Milestone 4**: CI/CD workflow with cached `Flip-to/bigquery-emulator` E2E integration tests.

## Verification

- Offline guard: `uv run python scripts/check_docs_layout.py` exits 0.
- Unit tests: `uv run pytest -v` passes 100% of fingerprinting, CLI, and Jinja AST checks.
- Emulator E2E test: `uv run python scripts/run_emulator_e2e.py` builds a virtualized incremental model + TVF against the BigQuery emulator, promotes `staging` to `prod` via view swap, and rolls back to the prior fingerprint.
