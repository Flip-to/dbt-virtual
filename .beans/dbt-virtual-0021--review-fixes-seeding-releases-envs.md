---
# dbt-virtual-0021
title: 'Fix review findings: incremental seeding, partial releases, env schemas, adopt, BigQuery SQL'
status: in-progress
type: bug
created_at: 2026-11-04T00:00:00Z
updated_at: 2026-11-04T00:00:00Z
---

In-depth review of `main` at 71a3fc4 (PRs #1-#17 were merged without review), focused on BigQuery
and on running dbt-virtual in the flipto-dbt project. Each finding below was reproduced with real
dbt-core 1.12.6 + dbt-duckdb 1.11.1 before fixing, unless marked otherwise, and each has a
regression test.

1. Incremental clone seeding never worked. dbt's incremental materialization loads
   `existing_relation` before model pre-hooks run (dbt-core 1.12.6
   `materializations/models/incremental/incremental.sql:5` vs `:28`; dbt-bigquery 1.12.1
   `materializations/incremental.sql:79` vs `:96`). DuckDB failed "Table ... already exists";
   on BigQuery `create or replace table` replaces the clone, and with the documented
   `is_incremental()` override the table keeps only the delta rows. Fix: `seed_incremental_clones()`
   (on-run-start, skipped for `dbt compile`) and `virtual_seed_incremental_clones` (CLI, before
   `dbt build`); `clone_if_new_fp` no longer clones; `dbt_virtual.is_incremental()` returns dbt's.
2. A release from a partial build (REUSE models, unchanged routines, `--select`) listed only what
   dbt built; promote then dropped every other prod view (reproduced: `b` dropped). Fix: CLI
   `build --release` snapshots the environment into the release (`virtual_snapshot_release`), and
   promote never demotes a model that is still a virtual node in the project.
3. The second default rollback returned to the same release (`rolled_back_at` was never set).
   Fix: rollback marks newer releases `rolled_back` and re-promotes the target.
4. `virtual_adopt` iterated the registry DDL string one character at a time ("syntax error at or
   near "C""), and registered pointers to tables it never created. Fixed both.
5. Unmapped envs (`pr_42`) used `target.schema`: `drop-env --env pr_99` dropped the staging
   target's schema, and `init-env` overwrote staging's views. Fix: documented
   `<base_env_schema>_<env>` fallback, drop-env refuses shared/physical schemas, sync creates the
   env schema when missing.
6. Jinja nested in `vars.dbt_virtual` (the documented `environment: "{{ env_var(...) }}"`) was
   never rendered by dbt; bare `dbt run` failed with a parser error. Fix: `get_config` renders it.
7. BigQuery: `COUNT(*)` in `virtual_audit` fails on `require_partition_filter` tables (measured
   2026-11-04 on BigQuery: "Cannot query over table ... without a filter"); now reads
   `__TABLES__.row_count`. Routine DDL templates used dbt-bigquery's `'''value'''` literal, which
   BigQuery rejects for a body with `r'\d+'` ("Illegal escape sequence: \d", measured
   2026-11-04); the escaped literal returned the 62-character body unchanged. Templates also used a bare `replace()` of the env schema name; now only
   schema-qualified references are replaced. The rollback `UPDATE ... WHERE promoted_at > (SELECT
   ...)` was run on DuckDB only, not on BigQuery.
8. `init-env` did not deploy routines (no DDL template on the `_environments` path) and used
   `CAST(NULL AS STRING)` (invalid on PostgreSQL). Fix: read templates from `_releases`.
9. Python fingerprints treated on-run-start hook nodes as virtual models and ignored BigQuery
   table options, `sql_header`, user hooks and enforced contract types; added when set.
10. Docs referenced `dbt_virtual.virtual_is_incremental()` (did not exist; now an alias) and
    `plan/build --base-env` (rejected by argparse).

Not fixed: bare `dbt run` cannot put upstream fingerprints in physical names
([bare_dbt_run_parse_time_fingerprint](../docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md),
bean dbt-virtual-0022).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | In-depth review of dbt-virtual (BigQuery focus), check it works in flipto-dbt, fix bugs, turn findings into tests | Chat request, 2026-11-04 | 2026-11-04 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Fixes listed above; seeding moved before materialization, releases snapshot the env | branch claude/dbt-virtual-code-review-06eb5c | 2026-11-04 |
| Proven | CI | L3: `tests/test_dbt_duckdb_regressions.py` runs real dbt-duckdb; all 5 tests failed on 71a3fc4 and pass with the fix (local run 2026-11-04) | pending: CI on the PR | |
| Reviewed | Reviewer: pending | | pending | |
| Shipped | Developer: Yuri Olive | | pending | |
| Verified | Developer: Yuri Olive | | pending: BigQuery emulator E2E + a flipto-dbt trial | |
