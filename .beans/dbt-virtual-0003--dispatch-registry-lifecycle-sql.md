---
# dbt-virtual-0003
title: 'Parameterize registry table SQL in operations.sql via adapter.dispatch'
status: completed
type: task
created_at: 2026-10-27T20:30:00Z
updated_at: 2026-10-27T22:48:00Z
---

Track tech debt `bigquery_sql_in_lifecycle_ops` (`docs/tech_debt/bigquery_sql_in_lifecycle_ops.md`). Factored registry table relations, upserts, routine DDL, and orphan `INFORMATION_SCHEMA` queries into `macros/adapters/dispatch.sql` so BigQuery, Snowflake, DuckDB, Postgres, and Databricks can execute `virtual_promote`, `virtual_rollback`, `virtual_audit`, and `virtual_gc` natively.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Document and resolve BigQuery-specific SQL in lifecycle operations | `docs/tech_debt/bigquery_sql_in_lifecycle_ops.md` | 2026-10-27 |
| Built | Agent: Claude (session 8546528b-092b-4b9b-b7e2-853b31780db0), approved by Yuri Olive | Extracted multi-warehouse dispatch macros (`registry_relation`, `upsert_environment_pointer_sql`, `create_routine_ddl_sql`, `find_orphan_tables_sql`, `current_timestamp_sql`) into `macros/adapters/dispatch.sql` | commit 5390909 | 2026-10-27 |
| Proven | CI | L2: Verified across BigQuery Emulator (`scripts/run_emulator_e2e.py`), Snowflake Emulator + Postgres 16 (`scripts/run_snowflake_emulator_e2e.py`), and DuckDB (`tests/test_duckdb_e2e.py`, `tests/test_dbt_duckdb_project_e2e.py`) | `pytest` + emulator E2E | 2026-10-27 |
| Reviewed | Developer: Yuri Olive | Reviewed and approved | direct push to main | 2026-10-27 |
| Shipped | CI | Pushed to `Flip-to/dbt-virtual` `main` | commit 5390909 / 4165f15 | 2026-10-27 |
| Verified | CI | All 5 CI jobs green on GitHub Actions | GitHub Actions run 40616950085 | 2026-10-27 |

## Todo

- [x] Move registry `MERGE` and orphan `INFORMATION_SCHEMA` queries into `macros/adapters/dispatch.sql`
- [x] Add `default__` and `snowflake__` ANSI/DuckDB/Postgres implementations
- [x] Remove `TECH_DEBT(bigquery_sql_in_lifecycle_ops)` markers and mark debt `paid`
