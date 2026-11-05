---
# dbt-virtual-0022
title: 'Guard bare dbt run against rebuilding shared physical tables'
status: completed
type: bug
created_at: 2026-11-04T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Without CLI `_dbt_virtual_fps`, physical names are generated at parse time without upstream
fingerprints, so a bare `dbt run` after an upstream change rebuilds a downstream physical table
in place, including one another environment points at. See
[bare_dbt_run_parse_time_fingerprint](../docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Found in the 2026-11-04 review; environments must not overwrite each other's tables | dbt-virtual-0021 review, repro in docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md | 2026-11-04 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Opt-in guard `vars.dbt_virtual.require_cli_fingerprints` (default false): `assert_cli_fingerprints()` fails `run`/`build`/`retry` of selected virtual table models in on-run-start when `_dbt_virtual_fps` is missing; new `on_run_start()` hook, and `seed_incremental_clones()` as a hook runs the same check. Mitigation only: the tech debt stays open while the default is off | `macros/lifecycle/hooks.sql`, `tests/test_require_cli_fingerprints.py` (7 passed locally 2026-11-04; `test_bare_dbt_run_fails_before_building` failed on `b390217` before the change: bare run exited 0), uncommitted | 2026-11-04 |
| Proven | CI | Guard fails bare runs before building | CI on PR #19 head 80b7a59, all 6 jobs green (unit 3.10/3.11/3.12, BigQuery emulator E2E strict with no skipped steps, Snowflake+Postgres E2E, dbt parse BigQuery/Snowflake); L3: `tests/test_require_cli_fingerprints.py` | 2026-11-05 |
| Reviewed | Developer: Yuri Olive | Asked to merge after the coordinator review of every merged agent branch | Chat 2026-11-05 ("Resolva tudo por e de merge"); no line-by-line human review | 2026-11-05 |
| Shipped | Developer: Yuri Olive | | PR #19 merged to main | 2026-11-05 |
| Verified | Developer: Yuri Olive | | pending: real BigQuery run in the sandbox project (authorized views, view OPTIONS, cross-project CLONE, data diff SQL not yet run on BigQuery) | |
| Built (2) | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | `dbt-virtual dbt <args>` passthrough (cached `.dbt_virtual/fingerprints.json`), `plan --print-fps` + `DBT_VIRTUAL_FPS` env source in `get_precomputed_fps()`, `guard_shared_tables` (default true) in `assert_shared_tables_safe()`; bare runs register the run-time Merkle fingerprint | branch claude/dbt-virtual-dbt-passthrough | 2026-11-05 |
| Proven (2) | CI | Same tests in the full suite on the merged branch | CI on PR #19 head 80b7a59, all 6 jobs green (unit 3.10/3.11/3.12, BigQuery emulator E2E strict with no skipped steps, Snowflake+Postgres E2E, dbt parse BigQuery/Snowflake); L3: `tests/test_dbt_passthrough.py` | 2026-11-05 |
