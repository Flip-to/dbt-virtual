# Multi-Warehouse Support Matrix

`dbt-virtual` is designed with **Google BigQuery** as its primary first-class warehouse while supporting **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks** (experimental, see below) via `adapter.dispatch('...', 'dbt_virtual')` in [`macros/adapters/dispatch.sql`](https://github.com/Flip-to/dbt-virtual/blob/main/macros/adapters/dispatch.sql).

!!! warning "Databricks is experimental"
    The `databricks__` macros are checked only as SQL text by the macro harness (`tests/test_macros_execution.py`). No CI job and no maintainer run has executed them on Databricks. Expect to find bugs; try it in a throwaway catalog first. With `target.type == 'databricks'`, `virtual_promote`, `virtual_rollback`, `virtual_init_env` and incremental seeding log a warning. Reason and exit criteria: [decision](https://github.com/Flip-to/dbt-virtual/blob/main/docs/decisions/20261105_databricks_experimental.md).

---

## Feature & SQL Dispatch Matrix

| Capability | BigQuery (`bigquery__`) | Snowflake (`snowflake__`) | Databricks (`databricks__`) | DuckDB / PostgreSQL (`default__`, `duckdb__`, `postgres__`) |
|---|---|---|---|---|
| **Physical Table Storage** | `` `project`.`analytics_physical`.`model__fp` `` | `ANALYTICS_DB.ANALYTICS_PHYSICAL.MODEL__FP` | `catalog.analytics_physical.model__fp` | `analytics_physical.model__fp` |
| **Incremental Seeding (`clone_table_sql`)** | Zero-copy `CREATE TABLE IF NOT EXISTS dst CLONE src` | Zero-copy `CREATE TABLE IF NOT EXISTS dst CLONE src` | Zero-copy `CREATE TABLE IF NOT EXISTS dst SHALLOW CLONE src` | `CREATE TABLE IF NOT EXISTS dst AS SELECT * FROM src` |
| **Registry Upsert (`upsert_environment_pointer_sql`)** | Atomic `MERGE ... WHEN MATCHED / NOT MATCHED` | Atomic `MERGE INTO` | Atomic `MERGE INTO` (Delta) | DuckDB (1.4+): `MERGE INTO`; PostgreSQL: one `WITH ... DELETE ... INSERT` statement (any version); other: `DELETE` + `INSERT` in one transaction |
| **Audit Row Counts (`row_count_sql`)** | `row_count` from `<dataset>.__TABLES__` (no table scan) | `COUNT(*)` | `COUNT(*)` | `COUNT(*)` |
| **Scalar UDF (`virtual_function`)** | `CREATE OR REPLACE FUNCTION ... RETURNS <type> AS (...)` | `CREATE OR REPLACE FUNCTION ... RETURNS <type> AS $$ ... $$` | `CREATE OR REPLACE FUNCTION ... RETURNS <type> LANGUAGE SQL AS $$ ... $$` (`default__`) | DuckDB `CREATE OR REPLACE MACRO ...`; PostgreSQL `CREATE OR REPLACE FUNCTION ... LANGUAGE SQL` |
| **Table Function (`table_function`)** | `CREATE OR REPLACE TABLE FUNCTION ... AS (SELECT ...)` | `CREATE OR REPLACE FUNCTION ... RETURNS TABLE(...) AS $$ ... $$` | `CREATE OR REPLACE FUNCTION ... RETURNS <returns> LANGUAGE SQL AS $$ ... $$` (`default__`) | DuckDB: **not virtualized** (dbt-duckdb's own materialization runs); PostgreSQL `CREATE OR REPLACE FUNCTION ...` |
| **Stored Procedure (`procedure`)** | `CREATE OR REPLACE PROCEDURE ... BEGIN ... END` (body supplied by the model) | `CREATE OR REPLACE PROCEDURE ... LANGUAGE SQL AS $$ ... $$` | `CREATE OR REPLACE PROCEDURE ... LANGUAGE SQL AS $$ ... $$` (`default__`) | DuckDB no-op (`SELECT 1`); PostgreSQL `CREATE OR REPLACE PROCEDURE ...` |
| **Orphan Table Discovery (`find_orphan_tables_sql`)** | `INFORMATION_SCHEMA.TABLES` (`table_type IN ('BASE TABLE', 'CLONE', 'SNAPSHOT')` + `REGEXP_CONTAINS`) | `INFORMATION_SCHEMA.TABLES` (`REGEXP_LIKE` + `DATEADD`) | `information_schema.tables` (`RLIKE` + `CREATED` age cutoff) | DuckDB `regexp_matches` + recorded release age; PostgreSQL `~` + recorded release age; unsupported adapters fail closed |
| **CI Verification** | [`Flip-to/bigquery-emulator`](https://github.com/Flip-to/bigquery-emulator) E2E in CI, plus real `dbt-core` + `dbt-bigquery` against it (`tests/test_dbt_bigquery_emulator_e2e.py`) | [`nnnkkk7/snowflake-emulator`](https://github.com/nnnkkk7/snowflake-emulator) E2E in CI | Macro Execution Harness only (SQL not executed): **experimental** | Real `dbt-core` + `dbt-duckdb` E2E in CI; PostgreSQL in the Snowflake-emulator E2E script |

On BigQuery, Snowflake and Databricks, `use_ctas_for_clone: true` replaces `CLONE` with `CREATE TABLE ... AS SELECT *`, for engines without `CLONE`. On Snowflake and Databricks a target named `emulator` does the same. On BigQuery it no longer does: the BigQuery emulator supports `CLONE` and `COPY`, so its E2E runs the same clone script as real BigQuery.

---

## BigQuery-Specific Behaviour

Because BigQuery is our primary production warehouse, `dbt-virtual` handles several BigQuery behaviors out of the box:

1. **Zero-copy `CLONE` seeding before dbt runs**:
   `seed_incremental_clones` (the `on-run-start` hook, or the step `dbt-virtual build` runs before `dbt build`) creates `<model>__<new_fp>` with `CREATE TABLE ... CLONE <model>__<prod_fp>`. It cannot be a model pre-hook: dbt-bigquery's incremental materialization loads the existing relation before pre-hooks run and would replace the clone with `create or replace table`. The deprecated `clone_if_new_fp()` pre-hook no longer clones; it only logs when a table was not seeded.
2. **`require_partition_filter` tables**:
   `COUNT(*)` fails on a table created with `require_partition_filter = true`, so `virtual_audit` reads `row_count` from `` `project`.`dataset`.__TABLES__ `` instead (no bytes scanned). `require_partition_filter` and `partition_expiration_days` are part of the fingerprint when set, so changing them creates a new physical table.
3. **`table_type = 'CLONE'` and `'SNAPSHOT'` Discovery**:
   In BigQuery `INFORMATION_SCHEMA.TABLES`, tables created via `CREATE TABLE ... CLONE ...` report `table_type = 'CLONE'` rather than `'BASE TABLE'`. Both `bigquery__find_orphan_tables_sql` and `bigquery__list_physical_tables_sql` explicitly include `('BASE TABLE', 'CLONE', 'SNAPSHOT')`.
4. **Post-Clone `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`**:
   When an incremental model adds new columns (`virtual_add_columns`), `seed_incremental_clones` clones the `prod` table and immediately adds the new columns before dbt's `insert_overwrite` or `merge` runs.
5. **Routine templates as string literals**:
   Routine DDL is stored in `_releases` with backslashes, quotes and newlines escaped (dbt-bigquery's triple-quoted `string_literal` broke on regex backslashes such as `r'\d+'`).
6. **Several datasets per environment**:
   `env_schemas.prod` can map each custom schema to its own dataset (`analytics`, `analytics_gold`, `spacetime`, ...) while staging keeps one dataset; see [Configuration Reference](configuration_reference.md).
7. **Compatible Type Widening**:
   `dbt-virtual diff` recognizes BigQuery's widening rules (`INT64 -> NUMERIC / BIGNUMERIC / FLOAT64`, `DATE -> DATETIME / TIMESTAMP`, `STRING(n) -> STRING`, and aliases `INT == INT64`, `DECIMAL == NUMERIC`) as `TYPE_WIDENED` (non-breaking).
8. **Isolated TVF Unit Testing (`tvf_test_ref`)**:
   Enables zero-I/O unit testing of BigQuery Table Functions using `ANY TABLE` parameters and `TABLE <cte_name>` arguments.
9. **Several locations (EU and US datasets)**:
   A view cannot read a table in another location and `CLONE` needs the same location, so `physical_schemas_by_location` gives each location its own physical schema and registry, models choose one with `+virtual_location` (or the profile's `location`), and `promote` / `rollback` / `gc` / `status` run once per location. Datasets the package creates get `OPTIONS (location = ...)`. See [Configuration Reference](configuration_reference.md#physical_schemas_by_location-multi-region-bigquery). Verified on DuckDB with two physical schemas; not yet on real BigQuery.

---

## dbt-core 1.11+: `virtual_function`

dbt-core 1.11 added a built-in `function` materialization, and dbt does not let a package override a built-in one, so `materialized='function'` fails with `'NoneType' object has no attribute 'type'` (dbt-core 1.12.6). Use `materialized='virtual_function'` for scalar UDFs on every adapter; see [Routines](routines_and_tvf_testing.md).

---

## DuckDB Limitations

- **Table functions are not virtualized.** dbt-duckdb (1.11.1) ships `materialization table_function, adapter='duckdb'`, and dbt prefers an adapter-specific materialization over the package's `default` one. The table macro is created by dbt-duckdb in the environment schema, but no pointer or template is recorded, so it is not promoted or rolled back. `tests/test_dbt_duckdb_multi_schema.py` uses a `virtual_function` (a DuckDB `MACRO`) for this reason.
- **No stored procedures**: `procedure` models compile to a no-op.
- **No zero-copy clone**: seeding copies the base table with `CREATE TABLE ... AS SELECT *`.
