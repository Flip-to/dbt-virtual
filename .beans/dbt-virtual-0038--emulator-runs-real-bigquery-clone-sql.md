---
# dbt-virtual-0038
title: 'BigQuery emulator E2E runs the real clone SQL (adopt included)'
status: in-progress
type: task
created_at: 2026-11-10T00:00:00Z
updated_at: 2026-11-10T00:00:00Z
---

Bean 0037 (PR #36): `dbt-virtual adopt` on real BigQuery failed because the `BEGIN ... EXCEPTION ... END` clone
script was cut at its `;`. CI did not catch it: no emulator step ran `adopt`, and nothing ran the script's
EXCEPTION branch. `bigquery__clone_table_sql` and `clone_or_copy_table` also still took the CTAS branch for a
target named `emulator`, left from before the emulator supported `CLONE`.

What changes here:

- `bigquery__clone_table_sql` and `clone_or_copy_table` no longer special-case `target.name == 'emulator'`
  (`use_ctas_for_clone` stays; Snowflake and Databricks are unchanged).
- Emulator gap found and fixed upstream (see the PR body for links): the driver ran the body of a
  `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block but never the handler, `@@error.message` was unknown, and
  `IF` / `RAISE` were no-ops, so the clone script's backstop could not run on the emulator.
- `scripts/run_emulator_e2e.py` step 30.6 runs the backstop script on a source that is 3 clones deep (the
  handler copies) and on a missing source (the handler re-raises).
- `tests/test_dbt_bigquery_emulator_e2e.py::test_adopt_clones_legacy_tables_through_the_real_clone_script` runs
  real `dbt-virtual adopt` over legacy tables, one of them already 3 clones deep.

Depends on: PR #36 (adopt fix, this branch contains it) and the emulator PRs (the CI emulator job builds
`Flip-to/bigquery-emulator` `flipto/main`, so it passes only after those merge).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Catch bugs like PR #36 in CI by running the real BigQuery clone SQL on the emulator | chat request | 2026-11-10 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | remove the emulator CTAS branch for BigQuery; add backstop and adopt E2E steps | branch claude/dbt-virtual-emulator-real-clone | 2026-11-10 |
| Proven | pending | | pending: emulator job green once the emulator PRs are merged | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
