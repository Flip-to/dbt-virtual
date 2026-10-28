---
# dbt-virtual-0013
title: 'Route only fingerprinted tables through virtual refs'
status: in_progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

`virtual_ref()` must not redirect arbitrary physical table names containing `__` as if they were dbt-virtual tables. Require the standard lowercase hexadecimal fingerprint suffix, or an explicit `physical_version` matching a virtual model in the manifest. Cover both a false match and pinned-version routing.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review remaining routing behavior and make concrete bugs into tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Validate fingerprint suffixes, allow declared pinned versions, and test unrelated and pinned identifiers | `macros/routing/virtual_routing.sql`; `tests/test_macros_execution.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: unrelated `__` table remains unchanged and a declared non-hex pinned identifier routes correctly; full suite passes | `uv run pytest -q` (27 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed |  | pending | pending | pending |
| Shipped |  | pending | pending | pending |
| Verified |  | pending | pending | pending |
