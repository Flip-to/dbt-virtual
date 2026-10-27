---
status: accepted
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Routine virtualization (`table_function`, `function`, `procedure`) and TVF testing

## Problem

BigQuery routines (`CREATE TABLE FUNCTION`, `CREATE FUNCTION`, `CREATE PROCEDURE`) cannot be aliased by a `SELECT *` view and bind their target dataset references at creation time. Furthermore, dbt's native `unit_tests` cannot pass call arguments (`DS_*`) to a BigQuery `TABLE FUNCTION`.

## Decision

1. **Ship three virtual-environment-aware routine materializations (`macros/materializations/`)**:
   - `table_function.sql`: Emits `CREATE OR REPLACE TABLE FUNCTION`, includes an idempotent `DROP PROCEDURE IF EXISTS` transition block, and supports optional `RETURNS TABLE<...>`.
   - `function.sql`: Emits `CREATE OR REPLACE FUNCTION ... RETURNS <type> AS (<expr>)` for scalar SQL UDFs.
   - `procedure.sql`: Emits `CREATE OR REPLACE PROCEDURE`.
2. **Environment-Tokenized DDL Capture (`routine_ddl_template`)**:
   - When a virtualized routine deploys in `staging`, register its fingerprint in `_environments` and store its compiled DDL (with the environment schema tokenized as `__DBT_VIRTUAL_ENV_SCHEMA__`) in `_releases`.
   - On `promote` or `rollback`, render `__DBT_VIRTUAL_ENV_SCHEMA__` for the target environment (`prod`) and execute routines in dependency tier order (`1=function`, `2=model view`, `3=table_function`, `4=procedure`).
3. **TVF Fixture Injection Macro (`macros/testing/tvf_test_ref.sql`)**:
   - `{{ tvf_test_ref('model') }}` returns `{{ ref('model') }}` in normal builds and `<model>__fixture` when `--vars '{tvf_test_fixtures: true}'` is passed, enabling `ANY TABLE` fixture testing on BigQuery and `Flip-to/bigquery-emulator`.

## Alternatives

- **Require `dbt run` in `prod` to deploy routines on promotion/rollback**: Rejected; storing `routine_ddl_template` in `_releases` allows atomic single-step promotion and rollback of views + routines without re-running dbt.

## Rollout

1. Ship `table_function.sql`, `function.sql`, `procedure.sql`, and `tvf_test_ref.sql`.
2. Wire routine registration into `macros/lifecycle/hooks.sql` and `macros/lifecycle/operations.sql`.

## Verification

- Exercise `table_function` and `function` creation, invocation, and `tvf_test_ref` against `Flip-to/bigquery-emulator` in `scripts/run_emulator_e2e.py`.
