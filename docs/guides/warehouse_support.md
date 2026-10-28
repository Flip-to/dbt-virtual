# Multi-Warehouse Support Matrix

`dbt-virtual` is designed with **Google BigQuery** as its primary first-class warehouse while supporting **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks** via `adapter.dispatch('...', 'dbt_virtual')` in [`macros/adapters/dispatch.sql`](https://github.com/Flip-to/dbt-virtual/blob/main/macros/adapters/dispatch.sql).

---

## Feature & SQL Dispatch Matrix

| Capability | BigQuery (`bigquery__`) | Snowflake (`snowflake__`) | Databricks (`databricks__`) | DuckDB / PostgreSQL (`default__`) |
|---|---|---|---|---|
| **Physical Table Storage** | `` `project`.`analytics_physical`.`model__fp` `` | `ANALYTICS_DB.ANALYTICS_PHYSICAL.MODEL__FP` | `catalog.analytics_physical.model__fp` | `analytics_physical.model__fp` |
| **Incremental Seeding (`clone_table_sql`)** | Zero-copy `CREATE TABLE IF NOT EXISTS dst CLONE src` | Zero-copy `CREATE TABLE IF NOT EXISTS dst CLONE src` | Zero-copy `CREATE TABLE IF NOT EXISTS dst SHALLOW CLONE src` | `CREATE TABLE IF NOT EXISTS dst AS SELECT * FROM src` |
| **Registry Upsert (`upsert_environment_pointer_sql`)** | Atomic `MERGE ... ON ... WHEN MATCHED / NOT MATCHED` | Atomic `MERGE INTO ...` | Atomic `MERGE INTO ...` | Transactional `DELETE` + `INSERT` |
| **Scalar UDF (`function`)** | `CREATE OR REPLACE FUNCTION ... RETURNS <type> AS (...)` | `CREATE OR REPLACE FUNCTION ... RETURNS <type> AS $$ ... $$` | `CREATE OR REPLACE FUNCTION ... RETURNS <type> RETURN ...` | `CREATE OR REPLACE FUNCTION ...` |
| **Table Function (`table_function`)** | `CREATE OR REPLACE TABLE FUNCTION ... AS (SELECT ...)` | `CREATE OR REPLACE FUNCTION ... RETURNS TABLE(...) AS $$ ... $$` | `CREATE OR REPLACE FUNCTION ... RETURNS TABLE(...) RETURN ...` | View / Function fallback |
| **Stored Procedure (`procedure`)** | `CREATE OR REPLACE PROCEDURE ... BEGIN ... END` | `CREATE OR REPLACE PROCEDURE ... LANGUAGE SQL AS $$ BEGIN ... END; $$` | SQL Scripting | `CREATE OR REPLACE PROCEDURE ...` |
| **Orphan Table Discovery (`find_orphan_tables_sql`)** | `INFORMATION_SCHEMA.TABLES` (`table_type IN ('BASE TABLE', 'CLONE', 'SNAPSHOT')` + `REGEXP_CONTAINS`) | `INFORMATION_SCHEMA.TABLES` (`REGEXP_LIKE` + `DATEADD`) | `information_schema.tables` (`RLIKE` + `CREATED` age cutoff) | DuckDB `regexp_matches` + recorded release age; PostgreSQL `~` + recorded release age; unsupported adapters fail closed |
| **CI Verification** | [`Flip-to/bigquery-emulator`](https://github.com/Flip-to/bigquery-emulator) E2E in CI | [`nnnkkk7/snowflake-emulator`](https://github.com/nnnkkk7/snowflake-emulator) E2E in CI | Macro Execution Harness | Real `dbt-core` + `dbt-duckdb` E2E in CI |

---

## BigQuery-Specific Optimizations

Because BigQuery is our primary production warehouse, `dbt-virtual` handles several subtle BigQuery behaviors out of the box:

1. **`table_type = 'CLONE'` and `'SNAPSHOT'` Discovery**:
   In BigQuery `INFORMATION_SCHEMA.TABLES`, tables created via `CREATE TABLE ... CLONE ...` report `table_type = 'CLONE'` rather than `'BASE TABLE'`. Both `bigquery__find_orphan_tables_sql` and `bigquery__list_physical_tables_sql` explicitly include `('BASE TABLE', 'CLONE', 'SNAPSHOT')`.
2. **Post-Clone `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`**:
   When an incremental model adds new columns (`virtual_add_columns`), `clone_if_new_fp` clones the `prod` table and immediately adds the new columns before dbt's `insert_overwrite` or `merge` runs.
3. **Compatible Type Widening**:
   `dbt-virtual diff` recognizes BigQuery's widening rules (`INT64 -> NUMERIC / BIGNUMERIC / FLOAT64`, `DATE -> DATETIME / TIMESTAMP`, `STRING(n) -> STRING`, and aliases `INT == INT64`, `DECIMAL == NUMERIC`) as `TYPE_WIDENED` (non-breaking).
4. **Isolated TVF Unit Testing (`tvf_test_ref`)**:
   Enables zero-I/O unit testing of BigQuery Table Functions using `ANY TABLE` parameters and `TABLE <cte_name>` arguments.
