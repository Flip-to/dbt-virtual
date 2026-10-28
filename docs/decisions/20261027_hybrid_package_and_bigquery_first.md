---
status: accepted
kind: developer
who: "Developer: Yuri Olive"
ticket: None
spec: docs/specs/20261027_dbt_virtual_architecture.md
prs: []
---

# Package `dbt-virtual` as a hybrid dbt macro package + Python CLI, targeting BigQuery first with multi-warehouse adapter dispatch

## Context

When designing `dbt-virtual` (`Flip-to/dbt-virtual`) to bring SQLMesh-style virtual data environments into dbt, we had to choose the packaging model (pure dbt macros vs. Python CLI wrapper vs. hybrid) and the warehouse target architecture.

## Decision

1. **Hybrid Packaging**: Ship `dbt-virtual` as both a valid dbt package (`dbt_project.yml` + `macros/`) and a Python CLI package (`pyproject.toml` + `src/dbt_virtual/`). Every lifecycle operation (`promote`, `audit`, `rollback`, `acquire_lock`, `release_lock`, `export_state`, `init_env`, `drop_env`, `gc`) is executable via `dbt run-operation` macros or via the `dbt-virtual` CLI (`plan`, `build`, `diff`, `audit`, `promote`, `rollback`, `status`, `init-env`, `drop-env`, `gc`).
2. **BigQuery-First with Multi-Warehouse Adapter Dispatch**: Treat BigQuery as the primary first-class warehouse (`CREATE TABLE ... CLONE`, `CREATE TABLE FUNCTION`, `CREATE FUNCTION`, `CREATE PROCEDURE`, `INFORMATION_SCHEMA.TABLES` with `CLONE`/`SNAPSHOT` discovery), while routing all warehouse-specific DDL/DML through `adapter.dispatch(..., 'dbt_virtual')` (`macros/adapters/dispatch.sql`) with full support for **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks**.

## Why

- **Deterministic Merkle-DAG fingerprinting needs `manifest.json`**: Computing topological parent-to-child fingerprints over normalized and `sqlglot`-canonicalized `compiled_code` is clean and fast in a Python pass over `target/manifest.json` (`src/dbt_virtual/fingerprint.py`).
- **Pure-dbt ergonomics still work**: Providing a 3-level Merkle-DAG pure-Jinja fallback in `macros/fingerprint/get_fingerprint.sql` and `dbt run-operation` macros means users can still run bare `dbt run` or orchestrate everything from dbt Cloud / Cloud Workflows without a custom container if desired.
- **BigQuery is where our primary production workload lives, while multi-warehouse dispatch unlocks universal adoption**: BigQuery workloads rely heavily on TVFs (`table_function`), scalar UDFs (`function`), and zero-copy table clones (`CLONE`), while `adapter.dispatch` enables Snowflake (`CLONE`), Databricks (`SHALLOW CLONE`), DuckDB, and Postgres using the exact same macro interface.

## Alternatives

- **Pure dbt macro package only**: Cannot normalize `node.compiled_code` with `sqlglot` AST across the full topological DAG before dbt begins executing nodes.
- **Python CLI wrapper only (no standalone `run-operation` macros)**: Forces every user to install and invoke a wrapper binary even for simple view swaps.

## Impact

- Repo root holds both `dbt_project.yml` (for `packages.yml` git installations) and `pyproject.toml` (for `uv pip install` / PyPI).
- All warehouse-specific SQL in `macros/lifecycle/` is dispatched via `macros/adapters/dispatch.sql` (`docs/tech_debt/bigquery_sql_in_lifecycle_ops.md` — paid in bean `dbt-virtual-0003`).

## Who

Developer decision: Yuri Olive, 2026-10-27.
