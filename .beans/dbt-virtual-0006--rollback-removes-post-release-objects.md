---
# dbt-virtual-0006
title: 'Remove post-release objects during rollback'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

Rollback must remove models and routines that exist in the current environment but are absent from the selected release, and clear their active registry pointers. Otherwise newer objects remain visible after rollback.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue the implementation review in lifecycle areas not checked in the earlier passes | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Demote post-release objects in reverse dependency order during rollback | `macros/lifecycle/operations.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Regression test verifies stale models/routines are dropped in reverse dependency order and pointers deleted; full suite passes | `test_virtual_promote_rollback_init_drop_and_gc_operations`; `uv run --extra dev pytest -v` — 27 passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
