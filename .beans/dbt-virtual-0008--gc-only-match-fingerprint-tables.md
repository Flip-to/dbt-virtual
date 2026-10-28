---
# dbt-virtual-0008
title: 'Restrict garbage collection to fingerprint-named tables'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

The generic orphan query matched any table containing two underscores. Since `virtual_gc` drops every returned name, a table such as `daily__archive` could be removed even though it is not a dbt-virtual fingerprint table.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue reviewing warehouse cleanup behavior across adapters | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Require a valid hex fingerprint suffix and fail closed for unmatched adapters | `macros/adapters/dispatch.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Macro tests assert strict matching for DuckDB, Postgres, Databricks and fail-closed fallback; DuckDB catalog E2E excludes an unrelated double-underscore table | `test_multi_adapter_dispatch_postgres_duckdb_fallback`; `test_duckdb_gc_only_selects_fingerprint_suffix_tables` | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
