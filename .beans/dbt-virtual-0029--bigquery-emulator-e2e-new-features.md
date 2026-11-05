---
# dbt-virtual-0029
title: 'BigQuery emulator E2E for features tested only on DuckDB or the mock harness'
status: in-progress
type: task
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Six features had only DuckDB or mock-harness tests. `scripts/emulator_e2e_gap_steps.py` (called from
`run_feature_steps` in `scripts/run_emulator_e2e.py`) now runs them as steps 20-25 on the BigQuery
emulator, through the real rendered macros (`MacroTestHarness`, `query_executor` / `relation_lookup`):

| Step | Feature | What is asserted |
|---|---|---|
| 20 | `virtual_data_diff` | Partitioned tables with injected differences: only-in-candidate 2, only-in-target 1, changed 3 (per-column `v` 2, `amt` 1), 2 identical partitions never joined, bisection (`threshold=4`) gives the same totals, `max_mismatch_pct` 5 refuses and 15 passes, the `promote(data_diff=true)` gate refuses before any change, identical tables run 2 checksum queries and no join |
| 21 | `virtual_promote` by clone | Two datasets as two locations (`physical_locations`) and a second project (`source_database` / `source_schema`): real `CREATE TABLE ... CLONE`, view reads the clone, source table and dataset dropped and the view still works, re-clone skipped, release rows copied to the target registry |
| 22 | `authorize_env_views` | One authorized-dataset entry (`targetTypes: [VIEWS]`) added through `datasets.get` + `datasets.patch` by promote, not duplicated on a rerun, other access entries kept, one entry per env dataset |
| 23 | Env view metadata | `OPTIONS(description, labels)` (with `;`, quotes and an apostrophe) and column descriptions (`update_columns` -> `tables.patch`) read back through `tables.get` |
| 24 | `gc --expire-env-days` | Dry run reports the abandoned env only; the real run drops its dataset (`DROP SCHEMA ... CASCADE`) and pointers, keeps the old base and mapped envs and an env with one fresh pointer, marks candidate releases `expired`; `expire_env_days` from the config works |
| 25 | Shared-table write guard, `DBT_VIRTUAL_FPS` | Refused when another env serves the table under another fingerprint (message text, `on_run_start`); allowed for the current env, an equal fingerprint, `compile`, guard off; `_dbt_virtual_fps` and `DBT_VIRTUAL_FPS` skip it; invalid JSON and `require_cli_fingerprints` messages |

The harness change in `tests/test_macros_execution.py`: `adapter_extras` may replace the recording
`grant_access_to` / `update_columns` stubs, so the E2E performs the same REST calls as dbt-bigquery.

## Emulator fixes

None of steps 20-25 are skipped; the emulator lacked these, fixed in two PRs (driver first):

- Flip-to/googlesqlite#36: `CREATE TABLE ... CLONE`, `CREATE TABLE IF NOT EXISTS ... AS SELECT` on an existing table no longer fails, `OPTIONS` of `CREATE VIEW` / CTAS reported with `Option` / `Labels`, `DROP SCHEMA [IF EXISTS] [CASCADE]` reported.
- Flip-to/bigquery-emulator#32: `datasets.patch` / `datasets.update` persisted (and `access` stored), view/CTAS `OPTIONS` as table metadata, `DROP SCHEMA` drops the dataset; `go.mod` bumped to the driver branch.

dbt-virtual CI builds `flipto/main` of the emulator, so the `emulator-e2e` job fails on this PR until both
emulator PRs are merged.

## dbt-virtual bugs found

None: no macro or Python change was needed for steps 20-25 (all passed against the fixed emulator
without touching `macros/` or `src/`), so there is no regression test to add on the DuckDB side.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Features tested only on DuckDB or the mock harness had no BigQuery emulator coverage | Chat request 2026-11-05 | 2026-11-05 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Steps 20-25 plus the emulator fixes they needed | branch claude/dbt-virtual-bq-e2e-new-features; Flip-to/googlesqlite#36; Flip-to/bigquery-emulator#32 | 2026-11-05 |
| Proven | pending | L3: `uv run --extra dev --extra emulator python scripts/run_emulator_e2e.py` passed locally against a build of Flip-to/bigquery-emulator#32 (no skipped steps); the missing emulator behaviour failed before the fixes (CLONE, DROP SCHEMA, `access`, view OPTIONS); CI not yet run, it needs both emulator PRs merged | local run 2026-11-05, CI pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending: the same flows are not yet run on real BigQuery | |
