---
# dbt-virtual-0008
title: 'Restrict garbage collection to fingerprint-named tables'
status: completed
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T12:00:00Z
---

The generic orphan query matched any table containing two underscores. Since `virtual_gc` drops every returned name, a table such as `daily__archive` could be removed even though it is not a dbt-virtual fingerprint table.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue reviewing warehouse cleanup behavior across adapters | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Require a valid hex fingerprint suffix, fail closed for unmatched adapters, and apply release-count and age retention before collection | `macros/adapters/dispatch.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: Release rows are ranked per release; DuckDB and PostgreSQL use the latest registered reference for the age cutoff, retaining a table with a recent reference even when older rows exist; DuckDB E2E confirms old orphans are candidates while recent and unrelated tables are retained | `test_virtual_promote_rollback_init_drop_and_gc_operations`; `test_duckdb_gc_only_selects_fingerprint_suffix_tables`; ruff and docs layout passed | 2026-10-28 |
| Reviewed | Agent: Claude (session 6f3c6805-18ef-442a-8407-0380c1bfc2e1), approved by Yuri Olive | Diff reviewed against main; no defects found | PR #5 review, 2026-10-28; merge approved by Yuri Olive in chat | 2026-10-28 |
| Shipped | Developer: Yuri Olive | Squash-merged to main | PR #5 merged | 2026-10-28 |
| Verified | CI | Post-merge main CI run | pending: main CI run after merge | pending |
