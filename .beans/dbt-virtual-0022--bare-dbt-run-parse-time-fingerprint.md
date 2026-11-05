---
# dbt-virtual-0022
title: 'Guard bare dbt run against rebuilding shared physical tables'
status: in-progress
type: bug
created_at: 2026-11-04T00:00:00Z
updated_at: 2026-11-04T00:00:00Z
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
| Proven | pending | | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
| Built (2) | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | `dbt-virtual dbt <args>` passthrough (cached `.dbt_virtual/fingerprints.json`), `plan --print-fps` + `DBT_VIRTUAL_FPS` env source in `get_precomputed_fps()`, `guard_shared_tables` (default true) in `assert_shared_tables_safe()`; bare runs register the run-time Merkle fingerprint | branch claude/dbt-virtual-dbt-passthrough | 2026-11-05 |
| Proven (2) | pending CI | `tests/test_dbt_passthrough.py` 18 tests; full suite run 2026-11-05: 120 passed, 9 failed (mock harness lacked `env_var`, one registered-fp assertion); after fixing, the affected files re-ran green (69 + 8 passed); no single full rerun after the fix | L3: `test_guard_refuses_bare_run_that_would_rewrite_a_table_prod_points_at` (guard off = `test_guard_can_be_turned_off` runs through), `test_env_var_fingerprints_make_plain_dbt_run_use_merkle_names` | 2026-11-05 |
