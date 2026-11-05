---
# dbt-virtual-0028
title: 'Atomic registry upserts on every adapter'
status: in-progress
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

`upsert_environment_pointer_sql` was a MERGE on BigQuery only; the other adapters ran DELETE then INSERT as
separate committed statements. Now MERGE on Snowflake, Databricks, DuckDB; one CTE statement on PostgreSQL; the
other DELETE + INSERT registry writes run in one transaction (`exec_atomic`). See
docs/decisions/20261105_atomic_registry_upserts.md and docs/tech_debt/registry_delete_insert_non_atomic.md.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | A failure between DELETE and INSERT leaves a model without a pointer | chat request | 2026-11-05 |
| Built | Agent: Claude (session dbt-virtual-registry-merge), approved by Yuri Olive | MERGE / one statement per adapter, `exec_atomic` for the other pairs | PR (draft), branch claude/dbt-virtual-registry-merge | 2026-11-05 |
| Proven | pending | Unit + real dbt-duckdb + Postgres 16 local run done; CI emulators pending | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
