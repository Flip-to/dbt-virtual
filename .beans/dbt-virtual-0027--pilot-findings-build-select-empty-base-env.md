---
# dbt-virtual-0027
title: 'Pilot findings: build --select vs REUSE, Jinja base_environment, --empty fingerprints'
status: in-progress
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Three bugs found by a real BigQuery pilot (2026-11-05).

1. `dbt-virtual build --select <sel>` passed the raw selector to dbt, which rebuilt (and, with
   `--empty`, emptied) REUSE tables the plan only meant to point at. The selector is now resolved
   with `dbt ls` (same target and vars) and intersected with the planned non-REUSE models.
2. `promote` read `vars.dbt_virtual.base_environment` without rendering Jinja, so the default
   audit never matched `--to`. A Jinja value is now rendered by `virtual_print_base_environment`
   (`dbt run-operation`); if that fails the promote audits and warns.
3. `DBT_EMPTY=true` made `dbt compile` wrap `source()` / unresolved refs in
   `(select * from x where false limit 0)`, so fingerprints changed. The CLI's `compile` calls now
   run without `DBT_EMPTY`.

Not fixed: REUSE pointers recorded `partial` in a `--empty` run, see
docs/tech_debt/reuse_pointer_build_quality.md.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Bugs found by the BigQuery pilot | pilot findings, chat request | 2026-11-05 |
| Built | Agent: Claude (session dbt-virtual-pilot-fixes), approved by Yuri Olive | `dbt ls` intersection, rendered base_environment, compile without DBT_EMPTY | PR (draft) on claude/dbt-virtual-pilot-fixes | 2026-11-05 |
| Proven | CI | Real dbt-duckdb regressions fail without the fixes | L3: tests/test_dbt_duckdb_pilot_fixes.py (real dbt-duckdb; 4 fail on main) and tests/test_pilot_fixes_bigquery.py (BigQuery macro harness + mocked-dbt CLI; 8 of 9 fail on main); emulator step in scripts/run_emulator_e2e.py runs in CI only | pending CI |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
