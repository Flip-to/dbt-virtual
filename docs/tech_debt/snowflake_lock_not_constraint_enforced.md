---
status: open
area: lifecycle
impact: medium
effort: m
bean: dbt-virtual-0030
---

# On Snowflake and Databricks the lease lock is not backed by an enforced key

## What

`virtual_acquire_lock` excludes concurrent runners on DuckDB and PostgreSQL with a unique index on
`_locks(environment)`. Snowflake does not enforce unique keys, so its acquire is a `MERGE`
(`snowflake__lock_upsert_sql` in `macros/adapters/dispatch.sql`,
`TECH_DEBT(snowflake_lock_not_constraint_enforced)`) that relies on the table DML lock serializing
two `MERGE` statements, plus the read-back that makes two simultaneous winners impossible
(`docs/decisions/20261105_lease_lock_single_statement_acquire.md`). Databricks relies on Delta
conflict detection the same way.

## Why it exists

Neither warehouse offers an enforced uniqueness constraint to hang the lock on. Snowflake hybrid
tables do, but they are a different table type and not available everywhere.

## Consequence

If a pair of `MERGE` statements were not serialized, two runners could both insert; the read-back then
makes both see each other and both yield (safe, but nobody holds the lock). Not measured: no
concurrent-acquire test runs on Snowflake or Databricks.

## Fix

Run `scenario_lock_race_has_exactly_one_winner` against a real Snowflake and a real Databricks
workspace (an opt-in script like `scripts/run_bigquery_sandbox_e2e.py`); if the `MERGE` does not
serialize, move the lease to a hybrid table with a primary key.

## Workaround meanwhile

Run one promote at a time per environment through the CI concurrency group.
