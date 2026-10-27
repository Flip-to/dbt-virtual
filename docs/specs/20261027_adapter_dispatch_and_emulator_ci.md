---
status: accepted
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Multi-warehouse adapter dispatch and BigQuery emulator CI

## Problem

`dbt-virtual` targets BigQuery first (where `CREATE TABLE ... CLONE` and `CREATE TABLE FUNCTION` are core primitives), but open-source contributors and CI pipelines need to test every PR without requiring paid BigQuery credentials or production data.

## Decision

1. **Adapter Dispatch (`macros/adapters/dispatch.sql`)**:
   - Isolate warehouse-specific DDL behind `adapter.dispatch(..., 'dbt_virtual')`: `clone_table_sql`, `create_or_replace_view_sql`, and `ensure_registry_tables_sql`.
2. **Hermetic CI on `Flip-to/bigquery-emulator` (`.github/workflows/ci.yml` & `scripts/run_emulator_e2e.py`)**:
   - The `Flip-to/bigquery-emulator` fork (`flipto/main`, backed by `Flip-to/googlesqlite`) supports `CREATE OR REPLACE TABLE`, `CREATE OR REPLACE VIEW`, `CREATE OR REPLACE TABLE FUNCTION` (including `ANY TABLE`), `CREATE OR REPLACE FUNCTION`, `MERGE` with subquery sources, and `ASSERT`.
   - Note on `CLONE` in the emulator: where `CREATE TABLE ... CLONE` is not supported by the SQLite-backed emulator analyzer, `bigquery__clone_table_sql` supports a configurable `var('dbt_virtual').use_ctas_for_clone` fallback (`CREATE TABLE IF NOT EXISTS ... AS SELECT * FROM ...`) when `target.name == 'emulator'`.
   - CI runs two jobs on every push and PR with full caching (`setup-uv` cache + `docker/build-push-action` GitHub Actions cache):
     1. **Fast Unit & Docs Layout Guard**: `ruff check`, `pytest`, and `scripts/check_docs_layout.py`.
     2. **BigQuery Emulator E2E Test**: Starts `Flip-to/bigquery-emulator`, runs `scripts/run_emulator_e2e.py` (which tests fingerprinting, staging build, `CREATE TABLE FUNCTION`, zero-rebuild `virtual_promote` to `prod`, and `virtual_rollback`), and asserts exact row parity.

## Alternatives

- **Mock all database calls in Python unit tests only**: Rejected; unit tests cannot catch SQL syntax or Jinja macro rendering bugs.
- **Require live GCP BigQuery credentials on every PR**: Rejected for an open-source repo where external contributors and fork PRs do not have access to organization secrets.

## Rollout

1. Add `use_ctas_for_clone` emulator fallback in `macros/adapters/dispatch.sql`.
2. Create `scripts/run_emulator_e2e.py` and wire it into `.github/workflows/ci.yml` with Docker build caching.

## Verification

- `.github/workflows/ci.yml` runs both unit/layout checks and the `Flip-to/bigquery-emulator` E2E job.
