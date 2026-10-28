---
status: implemented
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Routine virtualization (`table_function`, `function`, `procedure`) and TVF testing

## Problem

Warehouse routines (`CREATE TABLE FUNCTION`, `CREATE FUNCTION`, `CREATE PROCEDURE` in BigQuery, Snowflake, Postgres, DuckDB, and Databricks) cannot be aliased by a `SELECT *` view and bind their target schema/dataset references at creation time. Furthermore, dbt's native `unit_tests` cannot pass table call arguments to a BigQuery `TABLE FUNCTION`.

## Decision

1. **Ship three virtual-environment-aware routine materializations (`macros/materializations/routines.sql`)**:
   - `table_function`: Dispatches `create_routine_ddl_sql` (`CREATE OR REPLACE TABLE FUNCTION` on BigQuery with idempotent `DROP PROCEDURE IF EXISTS` transition guard, `RETURNS TABLE(...)` on Snowflake/Postgres/Databricks, and table macro on DuckDB).
   - `function`: Dispatches `create_routine_ddl_sql` (`CREATE OR REPLACE FUNCTION ... RETURNS <type> AS (...)` for scalar SQL UDFs across BigQuery, Snowflake, DuckDB, Postgres, and Databricks).
   - `procedure`: Dispatches `create_routine_ddl_sql` (`CREATE OR REPLACE PROCEDURE`).
2. **Environment-Tokenized DDL Capture (`routine_ddl_template`)**:
   - When a virtualized routine deploys in `staging`, `register_routine_deployment` registers its fingerprint in `_environments` and stores its compiled DDL (with the environment schema tokenized as `__DBT_VIRTUAL_ENV_SCHEMA__`) in `_releases`.
   - On `promote` or `rollback`, render `__DBT_VIRTUAL_ENV_SCHEMA__` for the target environment (`prod`), execute routines in strict dependency tier order (`1=function`, `2=model view`, `3=table_function`, `4=procedure`), and verify existence via `validate_routine_exists_sql`.
3. **TVF Fixture Injection Macro (`macros/testing/tvf_test_ref.sql`)**:
   - `{{ dbt_virtual.tvf_test_ref('model') }}` returns `{{ dbt_virtual.virtual_ref('model') }}` in normal builds and `<model>__fixture` when `--vars '{tvf_test_fixtures: true}'` is passed, enabling `ANY TABLE` fixture testing on BigQuery and `Flip-to/bigquery-emulator`.

## Alternatives

- **Require `dbt run` in `prod` to deploy routines on promotion/rollback**: Rejected; storing `routine_ddl_template` in `_releases` allows atomic single-step promotion and rollback of views + routines without re-running dbt.

## Rollout

1. Ship `table_function`, `function`, and `procedure` materializations in `macros/materializations/routines.sql` and `tvf_test_ref` in `macros/testing/tvf_test_ref.sql`.
2. Wire routine registration into `macros/lifecycle/hooks.sql` and `macros/lifecycle/operations.sql`.

## Verification

- Exercise `table_function` and `function` creation, tiered promotion (`1 -> 2 -> 3`), and `tvf_test_ref` against `Flip-to/bigquery-emulator` in `scripts/run_emulator_e2e.py` and `nnnkkk7/snowflake-emulator` in `scripts/run_snowflake_emulator_e2e.py`.
