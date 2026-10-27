---
status: accepted
kind: developer
who: "Developer: Yuri Olive"
ticket: None
spec: docs/specs/20261027_dbt_virtual_architecture.md
prs: []
---

# Package `dbt-virtual` as a hybrid dbt macro package + Python CLI, targeting BigQuery first with adapter dispatch

## Context

When designing `dbt-virtual` (`Flip-to/dbt-virtual`) to bring SQLMesh-style virtual data environments into dbt, we had to choose the packaging model (pure dbt macros vs. Python CLI wrapper vs. hybrid) and the initial warehouse target scope.

## Decision

1. **Hybrid Packaging**: Ship `dbt-virtual` as both a valid dbt Hub package (`dbt_project.yml` + `macros/`) and a zero-dependency Python CLI package (`pyproject.toml` + `src/dbt_virtual/`). Every lifecycle operation (`promote`, `rollback`, `gc`) is executable via `dbt run-operation` macros or via the `dbt-virtual` CLI (`plan`, `build`, `promote`, `rollback`, `gc`).
2. **BigQuery-First with Adapter Dispatch**: Target BigQuery first in v1 (`CREATE TABLE ... CLONE`, `CREATE TABLE FUNCTION`, `CREATE FUNCTION`, `CREATE PROCEDURE`, `INFORMATION_SCHEMA.TABLES`), while routing warehouse-specific DDL through `adapter.dispatch(..., 'dbt_virtual')` (`macros/adapters/dispatch.sql`).

## Why

- **Deterministic Merkle-DAG fingerprinting needs `manifest.json`**: Computing topological parent-to-child fingerprints over normalized `compiled_code` is clean and fast in a 200-line Python pass over `target/manifest.json` (`src/dbt_virtual/fingerprint.py`), whereas doing full compiled-SQL normalization purely inside Jinja at parse time has limited access to compiled upstream bodies.
- **Pure-dbt ergonomics still work**: Providing a pure-Jinja fallback in `macros/fingerprint/get_fingerprint.sql` and `dbt run-operation` macros means users can still run bare `dbt run` or orchestrate everything from dbt Cloud / Cloud Workflows without a custom container if desired.
- **BigQuery is where the immediate production workload lives**: Production workloads on BigQuery rely heavily on BigQuery TVFs (`table_function`), scalar UDFs (`function`), and zero-copy table clones (`CLONE`).

## Alternatives

- **Pure dbt macro package only**: Cannot normalize `node.compiled_code` across the full topological DAG before dbt begins executing nodes.
- **Python CLI wrapper only (no standalone `run-operation` macros)**: Forces every user to install and invoke a wrapper binary even for simple view swaps.
- **Multi-warehouse (BigQuery + Snowflake + Databricks) simultaneously in v1**: Delays v1 delivery without production workloads to validate Snowflake/Databricks routine semantics.

## Impact

- Repo root holds both `dbt_project.yml` (for `packages.yml` git installations) and `pyproject.toml` (for `uv pip install` / PyPI).
- Warehouse-specific SQL in `macros/lifecycle/` that is not yet dispatched is tracked as tech debt (`docs/tech_debt/bigquery_sql_in_lifecycle_ops.md`, bean `dbt-virtual-0003`).

## Who

Developer decision: Yuri Olive, 2026-10-27.
