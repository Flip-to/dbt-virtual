---
# dbt-virtual-0032
title: 'Mark Databricks support experimental'
status: in-progress
type: task
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Databricks has only SQL-shape tests in the macro harness and has never been executed. Label it
experimental in the docs and log a warning at runtime; see
[databricks_experimental](../docs/decisions/20261105_databricks_experimental.md).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Do not present an unexecuted warehouse as supported | Chat request 2026-11-05 | 2026-11-05 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Docs wording, decision, `warn_if_experimental_adapter` called from promote, rollback, init-env and seeding | branch claude/dbt-virtual-databricks-experimental | 2026-11-05 |
| Proven | CI | L2: `test_databricks_logs_an_experimental_warning_and_other_adapters_do_not` (macro harness; the macro does not exist on main, so it fails there) | local run 2026-11-05, CI pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | n/a: docs and a log line | | | |
