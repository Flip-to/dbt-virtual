---
# dbt-virtual-0037
title: 'BigQuery `adopt` cut the clone backstop script at its first `;`'
status: in-progress
type: bug
created_at: 2026-11-10T00:00:00Z
updated_at: 2026-11-10T00:00:00Z
---

`dbt-virtual adopt` on real BigQuery failed with `Syntax error: Expected ";" but got end of script at [4:110]`
(flipto-dbt sandbox rehearsal on v0.2.1, 2026-11-10, job text read with `bq show -j`: `BEGIN CREATE TABLE ... CLONE ...`
and nothing after). Since bean 0033 `bigquery__clone_table_sql` is a `BEGIN ... EXCEPTION ... END` script (the
3-chained-clones backstop); `virtual_adopt` sent it through `exec_ddl_dml`, which splits on `;`. Seeding already used
`clone_or_copy_table` (one statement). The emulator E2E did not see it: the `emulator` target takes the CTAS branch of
`clone_table_sql`.

Fix: `virtual_adopt` calls `clone_or_copy_table`, like seeding, which also copies a legacy table that is already 3
clones deep instead of failing.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Run the pilot rehearsal on v0.2.1 | chat request | 2026-11-10 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | adopt uses `clone_or_copy_table` | branch claude/dbt-virtual-adopt-clone-unsplit | 2026-11-10 |
| Proven | CI | L2: BigQuery macro harness test `test_bigquery_adopt_runs_the_clone_backstop_as_one_script` fails on main, passes here (local 2026-11-10) | tests/test_macros_execution.py | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | Real BigQuery adopt in the flipto-dbt sandbox rehearsal | pending: re-run the rehearsal on this branch | |
