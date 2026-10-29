---
# dbt-virtual-0005
title: 'Apply release and age retention in Snowflake garbage collection'
status: completed
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T12:00:00Z
---

Snowflake orphan discovery must apply the `retention_releases` and `retention_days` arguments used by `dbt-virtual gc`; otherwise it can delete recently orphaned physical tables and continue protecting every promoted release indefinitely.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue the implementation review in areas not checked in the first pass | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Apply configured age and release retention to Snowflake orphan discovery | `macros/adapters/dispatch.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Snowflake orphan discovery SQL includes both the release window and table age cutoff; full local suite passes | `test_multi_adapter_dispatch_postgres_duckdb_fallback`; `uv run --extra dev pytest -v` — 27 passed; lint and docs layout passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Agent: Claude (session 6f3c6805-18ef-442a-8407-0380c1bfc2e1), approved by Yuri Olive | Diff reviewed against main; no defects found | PR #2 review, 2026-10-28; merge approved by Yuri Olive in chat | 2026-10-28 |
| Shipped | Developer: Yuri Olive | Squash-merged to main | PR #2 merged | 2026-10-28 |
| Verified | CI | Post-merge main CI run | pending: main CI run after merge | pending |
