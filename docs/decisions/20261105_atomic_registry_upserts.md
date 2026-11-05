---
status: accepted
kind: developer
who: "Agent: Claude (session dbt-virtual-registry-merge), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# Registry pointer upserts are one statement (MERGE) or one transaction on every adapter

## Context

`upsert_environment_pointer_sql` was a `MERGE` on BigQuery only. Snowflake, Databricks, DuckDB,
PostgreSQL and the default adapter ran `DELETE` then `INSERT`. Those two statements were not one
transaction: `exec_ddl_dml` split the script on `;` and `exec_sql_statement` called `adapter.commit()` after each
statement (`macros/lifecycle/operations.sql` at 15011d0, `exec_sql_statement` / `exec_ddl_dml`). A failure or a
concurrent reader between the two statements saw a model with no pointer. The same split-commit applied to the other
registry replace sites (`_releases` candidates, `_companion_state`, companion tables, cross-project release copy).

## Decision

- `upsert_environment_pointers_sql` (the batched form, also used for one row by every non-BigQuery adapter)
  renders: `MERGE INTO ... ON (environment, model_name)` on Snowflake, Databricks and DuckDB (1.4.0 or newer);
  `MERGE` on BigQuery (unchanged shape); one data-modifying CTE statement (`WITH replaced AS (DELETE ... RETURNING 1) INSERT ...`)
  on PostgreSQL; `DELETE` + `INSERT` on the default adapter.
- `exec_atomic(statements)` runs several statements in one transaction (one commit at the end), and `exec_ddl_dml`
  uses it. The DELETE + INSERT pairs on `_releases`, `_companion_state`, companion tables and the cross-project release copy
  go through it.

## Why

A single statement cannot half-apply. PostgreSQL MERGE needs server 15; the CTE gives the same atomicity on every
version without a version probe (a probe costs a query per call and would reorder the queries the macro tests and
emulator scripts replay by index).

## Alternatives

- `INSERT ... ON CONFLICT` / `INSERT OR REPLACE`: needs a primary key on `_environments`; existing registries have none.
- PostgreSQL MERGE with a `server_version_num` probe, DELETE + INSERT fallback: more code, same effect.
- DuckDB DELETE + INSERT in a transaction (works on DuckDB below 1.4): rejected for a new MERGE-only path; the package
  now documents DuckDB 1.4.0 or newer (MERGE INTO release), `pyproject.toml` pins `duckdb>=1.4.0` for tests.

## Impact

DuckDB older than 1.4.0 fails on the pointer upsert (MERGE not supported). Databricks and BigQuery still run each
statement of the secondary DELETE + INSERT pairs as its own job: `docs/tech_debt/registry_delete_insert_non_atomic.md`.
Revisit when Databricks gets multi-statement transactions or when BigQuery scripting (`BEGIN TRANSACTION`) is wired through dbt.

## Who

Agent: Claude (session dbt-virtual-registry-merge), approved by Yuri Olive (chat 2026-11-05).
