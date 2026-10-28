---
# dbt-virtual-0019
title: 'Merge declared and inferred columns in schema diffs'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

dbt manifest `columns` metadata may document only some projected columns, or may still list a column removed from the query. `diff` currently treats any declared columns as complete and skips compiled SQL inference, which can miss removals or changes to undocumented columns. Use compiled projections to determine column presence, use declarations to refine types, and fall back to declarations only when projections cannot be inferred.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue SQLMesh comparison and turn schema-diff blind spots into tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Use compiled SQL projections for column presence and declared metadata for types; cover undocumented and stale declaration cases | `src/dbt_virtual/planner.py`; `tests/test_cli_and_macros.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: compiled projection removals are breaking with partial or stale declarations, and wildcard projections retain documented columns; full suite passes | `uv run pytest -q` (29 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
