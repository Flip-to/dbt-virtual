---
# dbt-virtual-0010
title: 'Execute stored procedure DDL as one statement'
status: completed
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T12:00:00Z
---

The generic DDL executor splits SQL text on semicolons. Procedure bodies contain semicolon-separated statements, so promotion and rollback could send invalid fragments instead of one procedure definition.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue checking routine deployment and lifecycle execution against SQLMesh expectations | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Execute routine DDL as a whole while retaining splitting for ordinary DDL batches | `macros/lifecycle/operations.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Promotion and rollback tests assert procedure bodies with internal semicolons are preserved as single statements; full suite passes | `test_virtual_promote_rollback_init_drop_and_gc_operations`; `uv run --extra dev pytest -v` — 27 passed; lint and docs layout passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Agent: Claude (session 6f3c6805-18ef-442a-8407-0380c1bfc2e1), approved by Yuri Olive | Diff reviewed against main; no defects found | PR #7 review, 2026-10-28; merge approved by Yuri Olive in chat | 2026-10-28 |
| Shipped | Developer: Yuri Olive | Squash-merged to main | PR #7 merged | 2026-10-28 |
| Verified | CI | Post-merge main CI run | pending: main CI run after merge | pending |
