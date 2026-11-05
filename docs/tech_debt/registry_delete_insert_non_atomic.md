---
status: open
area: adapters
impact: low
effort: m
bean: dbt-virtual-0028
---

# Secondary registry DELETE + INSERT pairs are not atomic on Databricks and BigQuery

## What

`exec_atomic` (`macros/lifecycle/operations.sql`, `TECH_DEBT(registry_delete_insert_non_atomic)`) commits several statements
together. dbt opens a transaction for DML on PostgreSQL, DuckDB and Snowflake. On Databricks and BigQuery every statement is its own
job, so the DELETE + INSERT pairs on `_releases` candidates, `_companion_state`, companion tables and the cross-project release
copy can still half-apply. The `_environments` pointer upsert is a single MERGE there and is not affected.

## Why it exists

Delta has no multi-statement transactions; BigQuery needs a scripting `BEGIN TRANSACTION` block sent as one job, which the
emulator CI does not support (not verified against the emulator).

## Consequence

A failure between the DELETE and the INSERT leaves the candidate release rows or companion state missing until the next run
(both are rewritten on re-run).

## Fix

MERGE the release candidate rows (key release_id, source_environment, model_name, status = 'candidate'); send BigQuery pairs as one script.

## Workaround meanwhile

Re-run `dbt-virtual build` / the promotion; both writes are idempotent.
