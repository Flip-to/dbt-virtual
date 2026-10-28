---
# dbt-virtual-0010
title: 'Execute stored procedure DDL as one statement'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

The generic DDL executor splits SQL text on semicolons. Procedure bodies contain semicolon-separated statements, so promotion and rollback could send invalid fragments instead of one procedure definition.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue checking routine deployment and lifecycle execution against SQLMesh expectations | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Execute routine DDL as a whole while retaining splitting for ordinary DDL batches | `macros/lifecycle/operations.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Promotion and rollback tests assert procedure bodies with internal semicolons are preserved as single statements; full suite passes | `test_virtual_promote_rollback_init_drop_and_gc_operations`; `uv run --extra dev pytest -v` — 27 passed; lint and docs layout passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
