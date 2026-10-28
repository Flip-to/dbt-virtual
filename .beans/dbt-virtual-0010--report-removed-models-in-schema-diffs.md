---
# dbt-virtual-0010
title: 'Report removed models in schema diffs'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

The schema diff must report removed models as breaking changes so `--fail-on-breaking` can stop an unsafe promotion. Include removed model IDs in the output and keep the documented classifications at one list level.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review dbt-virtual against SQLMesh and flag destructive model changes | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Report removed models as breaking and correct the CLI guide list indentation | `src/dbt_virtual/planner.py`; `tests/test_cli_and_macros.py`; `docs/guides/cli_and_operations.md` | 2026-10-28 |
| Proven | CI | L3: removed models classify as breaking; CI checks pass | PR #9 Python 3.10–3.12, BigQuery, Snowflake/Postgres jobs; focused schema-diff tests; docs layout check | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
