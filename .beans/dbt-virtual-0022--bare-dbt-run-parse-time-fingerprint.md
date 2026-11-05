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
