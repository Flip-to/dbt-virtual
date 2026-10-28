---
# dbt-virtual-0013
title: 'Route only fingerprinted tables through virtual refs'
status: in_progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

`virtual_ref()` must not redirect arbitrary physical table names containing `__` as if they were dbt-virtual tables. Restrict routing to identifiers ending in a lowercase hexadecimal fingerprint suffix and cover an unrelated table name with a regression test.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review remaining routing behavior and make concrete bugs into tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Validate the physical relation suffix before env-view routing and test that `daily__archive` remains unchanged | `macros/routing/virtual_routing.sql`; `tests/test_macros_execution.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: regression passes and generic macro checks pass | Targeted macro test; `uv run ruff check src tests scripts`; docs layout check | 2026-10-28 |
| Reviewed |  | pending | pending | pending |
| Shipped |  | pending | pending | pending |
| Verified |  | pending | pending | pending |
