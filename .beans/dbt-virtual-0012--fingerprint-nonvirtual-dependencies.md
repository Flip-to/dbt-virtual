---
# dbt-virtual-0012
title: 'Include non-virtual model dependencies in virtual fingerprints'
status: in_progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

Virtual model fingerprints must change when a referenced non-virtual dbt model changes. Otherwise the planner can reuse a physical table built from stale upstream SQL. Resolve model dependencies recursively for fingerprint inputs while returning plans only for opted-in virtual nodes.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review dbt-virtual against SQLMesh and turn bugs into tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Hash non-virtual model dependencies recursively in both planner and Jinja fallback, with regression tests proving a parent SQL change invalidates its virtual child | `src/dbt_virtual/fingerprint.py`; `macros/fingerprint/get_fingerprint.sql`; `tests/test_fingerprint.py`; `tests/test_macros_execution.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: changed non-virtual parent SQL changes the virtual child's fingerprint in planner and fallback; full suite passes | `uv run pytest -q` (29 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed |  | pending | pending | pending |
| Shipped |  | pending | pending | pending |
| Verified |  | pending | pending | pending |
