---
# dbt-virtual-0004
title: 'Preserve quoted comment markers during SQL fingerprint normalization'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

SQL normalization must preserve quoted comment markers and fingerprint-shaped literal values. It must also follow the manifest adapter dialect for backslash escapes, so PostgreSQL's standard string quoting does not treat a backslash as an escape. Run comment and fingerprint-suffix normalization in one quote-aware scan.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review dbt-virtual against SQLMesh and fix concrete bugs | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Normalize comments and physical suffixes in one quote-aware scan, selecting SQL dialect from the manifest | `src/dbt_virtual/fingerprint.py`; `tests/test_fingerprint.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: comments and suffixes normalize outside literals; Postgres string escapes follow adapter dialect; full suite pending | Focused normalization and dialect tests; lint and docs layout | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
