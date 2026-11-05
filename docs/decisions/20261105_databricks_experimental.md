---
status: accepted
kind: developer
who: "Developer: Yuri Olive"
ticket: None
spec: None
prs: []
---

# Databricks support is labelled experimental until its SQL has been executed

## Context

The `databricks__` macros in `macros/adapters/dispatch.sql` (SHALLOW CLONE seeding, orphan discovery, expired environments, string literals) are checked only as rendered SQL text by the macro harness (`tests/test_macros_execution.py`, for example `test_multi_adapter_dispatch_postgres_duckdb_fallback`). No CI job runs them on Databricks or on an emulator, and no run on a real workspace is recorded. BigQuery, Snowflake, DuckDB and PostgreSQL are executed in CI (`.github/workflows/ci.yml`). The README, docs and CLAUDE.md listed Databricks next to those as a supported warehouse.

## Decision

Databricks is documented as **experimental** in the README, the warehouse support guide, the configuration reference, getting started and CLAUDE.md. When `target.type == 'databricks'`, `virtual_promote`, `virtual_rollback`, `virtual_init_env` and `seed_incremental_clones` log one warning each (`warn_if_experimental_adapter`, `DECISION(databricks_experimental)`). The label is removed when the SQL has been executed on Databricks (real workspace or CI).

## Why

A user reading "supported" would trust promotion and rollback on a warehouse where nothing has run. The label costs nothing and moves the claim to what the evidence shows: SQL text only.

## Alternatives

- Keep the wording and rely on the support matrix row "Macro Execution Harness only": easy to miss, and the README intro still said first-class list.
- Remove Databricks: loses the dispatch work already done, and there may be users.
- Fail on Databricks unless a var opts in: stricter, but blocks people who already tested it themselves. Revisit if a bug is reported.

## Impact

Docs wording and a log line only; no behaviour change. The warning is logged per lifecycle operation, not once per process (a Jinja macro has no process-wide state). Exit criterion: an executed run of the lifecycle (seed, promote, rollback, gc) on Databricks, then drop the label and the macro.

## Who

Developer decision. Developer: Yuri Olive.
