---
# dbt-virtual-0003
title: 'Parameterize registry table SQL in operations.sql via adapter.dispatch'
status: todo
type: task
created_at: 2026-10-27T20:30:00Z
updated_at: 2026-10-27T20:30:00Z
---

Track tech debt `bigquery_sql_in_lifecycle_ops` (`docs/tech_debt/bigquery_sql_in_lifecycle_ops.md`). `macros/lifecycle/operations.sql` and `macros/lifecycle/hooks.sql` currently use BigQuery backtick quoting and `QUALIFY` / `SAFE_OFFSET` directly; factor those queries into `macros/adapters/dispatch.sql` so DuckDB and Snowflake can execute `virtual_promote`, `virtual_rollback`, and `virtual_gc` natively.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Document and track BigQuery-specific SQL in lifecycle operations | `docs/tech_debt/bigquery_sql_in_lifecycle_ops.md` | 2026-10-27 |
| Built | pending | | | |
| Proven | pending | | | |
| Reviewed | pending | | | |
| Shipped | pending | | | |
| Verified | pending | | | |

## Todo

- [ ] Move registry `MERGE` and orphan `INFORMATION_SCHEMA` queries into `macros/adapters/dispatch.sql`
- [ ] Add `default__` ANSI/DuckDB implementations
- [ ] Remove `TECH_DEBT(bigquery_sql_in_lifecycle_ops)` markers and mark debt `paid`
