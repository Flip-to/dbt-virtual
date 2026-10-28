---
# dbt-virtual-0018
title: 'Skip unscoped dbt builds when a plan has no build actions'
status: in_progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

When all virtual routines are already reused, or the manifest contains no virtual nodes, `dbt-virtual build` currently falls through to `dbt build` without a selector and builds the whole project. Skip dbt build whenever there are no planned build actions and no explicit `--select`, and test both cases.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue review and turn every build-scope bug into a regression test | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Skip unscoped build fallback when no node needs work and test empty and routine-only reuse plans | `src/dbt_virtual/cli.py`; `tests/test_cli_and_macros.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: both no-action plans invoke no dbt command; full suite passes | `uv run pytest -q` (28 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed |  | pending | pending | pending |
| Shipped |  | pending | pending | pending |
| Verified |  | pending | pending | pending |
