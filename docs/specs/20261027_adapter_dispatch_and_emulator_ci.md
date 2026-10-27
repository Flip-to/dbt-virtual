---
status: accepted
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Multi-warehouse adapter dispatch and emulator CI (BigQuery, Snowflake, DuckDB, Postgres, Databricks)

## Problem

`dbt-virtual` originated on BigQuery (where `CREATE TABLE ... CLONE` and `CREATE TABLE FUNCTION` are core primitives), but production data teams also run dbt on **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks**, and open-source contributors need fast, hermetic CI pipelines that verify every warehouse dialect without paid cloud credentials.

## Decision

1. **Multi-Warehouse Adapter Dispatch (`macros/adapters/dispatch.sql`)**:
   - Isolate all warehouse-specific DDL and DQL behind `adapter.dispatch(..., 'dbt_virtual')`:
     - `clone_table_sql`: `CLONE` on BigQuery and Snowflake (`bigquery__`, `snowflake__`), `SHALLOW CLONE` on Databricks (`databricks__`), and `CREATE TABLE IF NOT EXISTS ... AS SELECT *` on DuckDB/Postgres (`default__`) or when `use_ctas_for_clone` / `target.name == 'emulator'` is active.
     - `create_or_replace_view_sql`: `CREATE OR REPLACE VIEW` (`default__`) and `DROP VIEW IF EXISTS ... CASCADE; CREATE VIEW ...` (`postgres__`).
     - `ensure_registry_tables_sql`, `registry_relation`, `upsert_environment_pointer_sql`, `current_timestamp_sql`, `create_schema_if_not_exists_sql`, `drop_schema_cascade_sql`, `create_routine_ddl_sql`, and `find_orphan_tables_sql` with dedicated `bigquery__`, `snowflake__`, `duckdb__`, `postgres__`, and `default__` implementations.
2. **Hermetic Multi-Emulator CI (`.github/workflows/ci.yml`)**:
   - **Unit + In-Process DuckDB E2E (`unit-and-guards`)**: Runs `ruff check`, `scripts/check_docs_layout.py`, and `pytest` (including [`tests/test_duckdb_e2e.py`](../../tests/test_duckdb_e2e.py), which executes the full virtual environment lifecycle + DuckDB scalar/table macros against an in-memory DuckDB instance across Python 3.10, 3.11, and 3.12).
   - **BigQuery Emulator E2E (`emulator-e2e`)**: Starts `Flip-to/bigquery-emulator` (`flipto/main`, cached via `actions/cache@v5` + `zstd`) and runs [`scripts/run_emulator_e2e.py`](../../scripts/run_emulator_e2e.py).
   - **Snowflake Emulator & PostgreSQL E2E (`snowflake-and-postgres-e2e`)**: Starts [`nnnkkk7/snowflake-emulator`](https://github.com/nnnkkk7/snowflake-emulator) (`ghcr.io/nnnkkk7/snowflake-emulator:latest`, cached via `actions/cache@v5`) alongside a `postgres:16-alpine` service container and runs [`scripts/run_snowflake_emulator_e2e.py`](../../scripts/run_snowflake_emulator_e2e.py) across both Snowflake REST API v2 (`/api/v2/statements`) and PostgreSQL.

## Alternatives

- **Mock all database calls in Python unit tests only**: Rejected; unit tests cannot catch SQL syntax or Jinja macro rendering bugs.
- **Require live GCP BigQuery credentials on every PR**: Rejected for an open-source repo where external contributors and fork PRs do not have access to organization secrets.

## Rollout

1. Add `use_ctas_for_clone` emulator fallback in `macros/adapters/dispatch.sql`.
2. Create `scripts/run_emulator_e2e.py` and wire it into `.github/workflows/ci.yml` with Docker build caching.

## Verification

- `.github/workflows/ci.yml` runs both unit/layout checks and the `Flip-to/bigquery-emulator` E2E job.
