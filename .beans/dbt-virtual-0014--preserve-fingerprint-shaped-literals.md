---
# dbt-virtual-0014
title: 'Preserve fingerprint-shaped SQL string literals'
status: in_progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

Fingerprint normalization must not strip `__<hex>` text from SQL string literals. Doing so makes different query results share a fingerprint and can reuse stale physical data. Keep suffix normalization out of single-quoted literals and cover the collision with regression tests.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review dbt-virtual against SQLMesh and turn bugs into tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Preserve fingerprint-like suffixes inside string literals while normalizing relation identifiers | `src/dbt_virtual/fingerprint.py`; `tests/test_fingerprint.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: distinct literals produce distinct normalized SQL and fingerprints; full suite passes | `uv run pytest -q` (28 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed |  | pending | pending | pending |
| Shipped |  | pending | pending | pending |
| Verified |  | pending | pending | pending |
