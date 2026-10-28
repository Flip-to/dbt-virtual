---
# dbt-virtual-0005
title: 'Apply release and age retention in Snowflake garbage collection'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

Snowflake orphan discovery must apply the `retention_releases` and `retention_days` arguments used by `dbt-virtual gc`; otherwise it can delete recently orphaned physical tables and continue protecting every promoted release indefinitely.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue the implementation review in areas not checked in the first pass | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Apply configured age and release retention to Snowflake orphan discovery | `macros/adapters/dispatch.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Snowflake orphan discovery SQL includes both the release window and table age cutoff; full local suite passes | `test_multi_adapter_dispatch_postgres_duckdb_fallback`; `uv run --extra dev pytest -v` — 27 passed; lint and docs layout passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
