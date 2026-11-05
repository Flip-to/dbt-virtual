---
# dbt-virtual-0031
title: 'Real dbt-core + dbt-bigquery on the BigQuery emulator, and a real-dbt BigQuery E2E in CI'
status: in-progress
type: task
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Until now dbt never ran on the BigQuery dialect in CI: `scripts/run_emulator_e2e.py` renders the
macros in a Python harness. This bean makes real dbt-core + dbt-bigquery run against
`Flip-to/bigquery-emulator` and drives the whole dbt-virtual lifecycle through the CLI.

1. **Wiring, tests only.** dbt-bigquery 1.12.2 has `api_endpoint` in the profile and the method
   `oauth-secrets` with a dummy token never contacts Google, so no shim or adapter patch is needed
   (`tests/bq_emulator_project.py`; `DBT_VIRTUAL_BQ_EMULATOR` holds the emulator URL).
2. **Real-dbt BigQuery E2E** (`tests/test_dbt_bigquery_emulator_e2e.py`): partitioned `table`,
   partitioned `incremental` (merge), `view`, `virtual_function` (UDF) and `table_function` (TVF); plan,
   build (with a state file, so unchanged models are REUSE, and with `--select`), CLONE seeding of an
   incremental model, audit, promote, rollback, init-env, drop-env, gc, status, diff-data, `run`,
   custom schemas mapped to several datasets, demotion; `DBT_EMPTY` fingerprint equality.
3. **Emulator gaps fixed** in Flip-to/googlesqlite#37 and Flip-to/bigquery-emulator#33.
4. **CI**: the emulator job runs the new tests; the step is gated on a probe of the emulator
   (`scripts/probe_bq_emulator_dbt_support.py`) until the emulator PRs below are merged.

## Emulator PRs (not merged)

| Repo | PR | What |
|---|---|---|
| Flip-to/googlesqlite | #37 | `DROP PROCEDURE [IF EXISTS]` no-op; `TableSpec.ClusterBy` and `TableSpec.Partitioning()` |
| Flip-to/bigquery-emulator | #33 | `tables.get` reports `timePartitioning` / `rangePartitioning` / `clustering` of SQL-created tables; go.mod bump |
| Flip-to/googlesqlite#36, Flip-to/bigquery-emulator#32 | merged | `CREATE TABLE ... CLONE` (needed by incremental seeding). #37 and #33 are merged with flipto/main and add `CLONE` keeps `CLUSTER BY` |

## Gated until those merge

The CI step "Real dbt-core + dbt-bigquery E2E" runs only when `scripts/probe_bq_emulator_dbt_support.py`
finds the behaviours above (CLONE is already on flipto/main; the table layout and DROP PROCEDURE are not) in the emulator built from `flipto/main`; otherwise a warning
annotation names what is missing. To remove the gate: delete the probe step, the `GATED` step and the
script, and drop the `if:` of the E2E step.

## Known emulator gaps not fixed here

- `dbt run` of an `insert_overwrite` incremental model: `Statement not supported: VariableDeclaration`.
- `dbt docs generate`: `INFORMATION_SCHEMA.MATERIALIZED_VIEWS` not found.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Run real dbt on BigQuery dialect in CI instead of a macro harness | Chat request 2026-11-05 | 2026-11-05 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Tests, probe and CI gate; emulator fixes in two other repos | branch claude/dbt-virtual-dbt-bigquery-emulator; Flip-to/googlesqlite#37; Flip-to/bigquery-emulator#33 | 2026-11-05 |
| Proven | pending | L3 once CI shows the E2E green against an emulator build that has the fixes | see PR | |
| Reviewed | pending | | | |
| Shipped | pending | | | |
| Verified | pending | | | |
