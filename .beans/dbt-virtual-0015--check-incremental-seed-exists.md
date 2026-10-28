---
# dbt-virtual-0015
title: 'Check incremental base table exists before is_incremental'
status: in_progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

`is_incremental()` must only select an incremental query branch when the registered base physical table exists. A stale registry pointer currently reports true even though `clone_if_new_fp()` cannot seed the target, which can run delta-only SQL against an empty target. Check the catalog relation and test both missing and present base tables.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue implementation review and add regression tests for bugs | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Require the registered physical base relation before returning true from the incremental predicate | `macros/lifecycle/hooks.sql`; `tests/test_macros_execution.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: missing base yields false and existing base yields true; full suite passes | `uv run pytest -q` (28 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed |  | pending | pending | pending |
| Shipped |  | pending | pending | pending |
| Verified |  | pending | pending | pending |
