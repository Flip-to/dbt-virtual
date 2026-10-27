---
status: paid
area: adapters
impact: low
effort: s
bean: dbt-virtual-0003
paid_by: 5390909
---

# Lifecycle hooks and operations contain BigQuery-specific SQL syntax

## What

While `clone_table_sql`, `create_or_replace_view_sql`, and `ensure_registry_tables_sql` are dispatched via `adapter.dispatch` in `macros/adapters/dispatch.sql`, the registry lookup/upsert and GC queries in `macros/lifecycle/hooks.sql` and `macros/lifecycle/operations.sql` currently use BigQuery backtick identifiers (`` `{{ target.database }}`.`{{ phys_schema }}` ``), `MERGE ... WHEN NOT MATCHED THEN INSERT ROW`, `QUALIFY`, and `SAFE_OFFSET` directly (`TECH_DEBT(bigquery_sql_in_lifecycle_ops)`).

## Why it exists

v1 targets BigQuery first (decision [20261027_hybrid_package_and_bigquery_first.md](../decisions/20261027_hybrid_package_and_bigquery_first.md)), and keeping the SQL inline in `hooks.sql` and `operations.sql` kept the initial implementation readable.

## Consequence

Running `virtual_promote`, `virtual_rollback`, or `virtual_gc` on DuckDB, Postgres, or Snowflake will fail on BigQuery-specific syntax (`INSERT ROW`, `SAFE_OFFSET`, backtick quoting) until those queries are moved into `macros/adapters/dispatch.sql`.

## Fix

Extract `upsert_environment_pointer_sql`, `insert_release_row_sql`, and `find_orphan_tables_sql` into `macros/adapters/dispatch.sql` with `bigquery__` and `default__` (ANSI SQL) implementations.

## Workaround meanwhile

Use BigQuery or `Flip-to/bigquery-emulator` for end-to-end lifecycle tests (`scripts/run_emulator_e2e.py`).
