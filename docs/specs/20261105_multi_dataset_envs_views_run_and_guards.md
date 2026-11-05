---
status: implemented
ticket: None
bean: dbt-virtual-0024
prs: [19]
---

# Multi-dataset environments, virtual views, `run`, adopt and guards (update to the 20261027 architecture)

Updates `20261027_dbt_virtual_architecture.md` and `20261027_routing_ref_modes_and_clone_seeding.md`
for what was built on the production-readiness branch after them. Where this file and those
disagree, this file wins; they stay `implemented` for the parts it does not change.

## Problem

The 20261027 design assumed one schema per environment, only table, incremental and routine
objects, promotion as the only way code reaches an environment, and a `ref_resolution` list with an
`explicit` mode. A real multi-dataset BigQuery project needs more:

- consumer-facing objects live in several datasets per environment, and non-production targets
  collapse them into one;
- views must be promoted and rolled back like routines;
- unchanged tables must be refreshable on a schedule without a release;
- an existing deployment must be adoptable without rebuilding anything;
- known foot-guns (a bare `dbt run`, an unregistered clone, `explicit` behaving like `physical`)
  need guards.

Evidence for each item is in the linked decisions and the tests named under Verification.

## Decision

1. **One schema per custom schema per environment.** `env_schemas.<env>` is a schema name or a
   mapping `{default, <custom_schema>: <schema>}`; `_environments` / `_releases` carry `schema_key`
   (`docs/decisions/20261104_per_custom_schema_env_datasets.md`). Fingerprints do not depend on
   the environment: dependency database/schema qualifiers are stripped before hashing.
2. **Virtual views are registered and promoted** with a `CREATE OR REPLACE VIEW` template
   (`resource_type = 'view'`), deployed by DAG depth together with routines
   (`docs/decisions/20261105_virtual_views_and_run_command.md`).
3. **`dbt-virtual run --env <env>`** refreshes unchanged table/incremental models in place (via
   `dbt build`) and refuses a changed fingerprint unless `--allow-changes`.
4. **CLI surface**: `plan` / `build` take `--target`, `--vars`; `build` takes `--defer`,
   `--state`; `promote`, `audit`, `rollback` take `--no-lock`, `--lock-owner`; `status`
   returns empty state when the registry does not exist yet; `adopt` registers deployed tables,
   views and routines (templates rendered from compiled SQL passed as `_dbt_virtual_compiled`).
5. **`ref_resolution`** accepts `env_view` (default), `physical`, `hybrid`. `explicit` and unknown
   values are compile errors (`docs/decisions/20261105_ref_resolution_explicit_is_rejected.md`).
6. **Guards**: `require_cli_fingerprints` fails a bare `dbt run` / `dbt build` of virtual tables;
   an unregistered physical table is never reused; rollback refuses a release whose tables were
   garbage-collected; `drop-env` refuses `base_environment`, `physical_schema` and schemas of other
   environments; `virtual_function` replaces `function` on dbt-core 1.11+.
7. **CI**: a `dbt parse` job runs the package on BigQuery and Snowflake profiles with fake
   credentials (parse needs no connection); `scripts/run_bigquery_sandbox_e2e.py` is an opt-in
   run against real BigQuery.

## Alternatives

- Keep a single schema per environment: rejected, it breaks consumers of the other datasets.
- Per-call `explicit` ref control through a second macro: not built; see the decision for why.
- Register adopted views from the warehouse definition (`INFORMATION_SCHEMA.VIEWS`) instead of
  compiled SQL: rejected, later promotions replay the node's compiled SQL, so adopting the
  warehouse text would register a template that differs from what the next build produces.

## Rollout

Everything above is on branch `claude/dbt-virtual-prod-ready` and its follow-up
`claude/dbt-virtual-small-gaps`. Not yet verified: a pilot on the team's real BigQuery project
(`pending`, see bean dbt-virtual-0023).

## Verification

- Real dbt-core + dbt-duckdb: `tests/test_dbt_duckdb_multi_schema.py`,
  `tests/test_dbt_duckdb_views_and_run.py`, `tests/test_dbt_duckdb_regressions.py`,
  `tests/test_dbt_duckdb_adopt_objects.py`, `tests/test_dbt_duckdb_small_gaps.py`,
  `tests/test_require_cli_fingerprints.py`, `tests/test_sqlmesh_parity_fixes.py`.
- `tests/test_dbt_parse_warehouses.py`: parse on BigQuery and Snowflake; the macros of
  `0f42ee9^` fail it with "No macro named 'orphan_table_age_predicate_sql'" (2026-11-05).
- `tests/test_dispatch_coverage.py`: every dispatched macro has a `default__`.
