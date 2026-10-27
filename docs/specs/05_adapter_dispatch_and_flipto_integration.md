# SPEC 05: Multi-Warehouse Adapter Dispatch & Integration with `flipto-dbt`

- **Status**: Draft v1.0
- **Component**: `macros/adapters/` & Migration Guide

---

## 1. Adapter Dispatch Architecture

All warehouse-specific DDL in `dbt-virtual` is routed through `adapter.dispatch(macro_name, 'dbt_virtual')` so that **BigQuery** is supported with full production fidelity in v1, **DuckDB** can run fast local/CI end-to-end tests in seconds, and **Snowflake / Databricks** can be contributed cleanly.

| Dispatched Macro | BigQuery (`bigquery__...`) | DuckDB / Default (`default__...`) | Snowflake (`snowflake__...`) |
|---|---|---|---|
| `clone_table_sql(source, target)` | `CREATE TABLE IF NOT EXISTS {{ target }} CLONE {{ source }}` | `CREATE TABLE IF NOT EXISTS {{ target }} AS SELECT * FROM {{ source }}` | `CREATE TABLE IF NOT EXISTS {{ target }} CLONE {{ source }}` |
| `create_or_replace_view_sql(view, target)` | `CREATE OR REPLACE VIEW {{ view }} AS SELECT * FROM {{ target }}` | `CREATE OR REPLACE VIEW {{ view }} AS SELECT * FROM {{ target }}` | `CREATE OR REPLACE VIEW {{ view }} AS SELECT * FROM {{ target }}` |
| `ensure_registry_tables_sql(phys_schema)` | BigQuery `STRING`, `INT64`, `TIMESTAMP` with `PARTITION BY` / `CLUSTER BY` | ANSI `VARCHAR`, `BIGINT`, `TIMESTAMP` | Snowflake `VARCHAR`, `NUMBER`, `TIMESTAMP_TZ` |
| `list_candidate_gc_tables_sql(phys_schema, min_age_days)` | Queries `` `<project>.<phys_schema>.INFORMATION_SCHEMA.TABLES` `` | Queries `information_schema.tables` | Queries `information_schema.tables` |

---

## 2. How `flipto-dbt` Integrates `dbt-virtual` (Concrete Example)

### 2.1 Install in `packages.yml` & `pyproject.toml`
```yaml
# packages.yml
packages:
  - git: "https://github.com/Flip-to/dbt-virtual.git"
    revision: v0.1.0
```

### 2.2 Configure in `dbt_project.yml`
Scope `virtual_env: true` to the 9 `*_metrics_root` models and the serving TVFs/UDFs (`models/gold/procedures/`), leaving the upstream Snowplow accumulators untouched (or guarded by Part 3 B1 snapshots):

```yaml
vars:
  dbt_virtual:
    enabled: true
    physical_schema: "spacetime_physical"
    environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"
    base_environment: "prod"
    ref_resolution: "env_view"   # Option A: TVFs & downstream read spacetime_staging.<root> / spacetime.<root>
    env_schemas:
      staging: "spacetime_staging"
      prod: "spacetime"
    retention_releases: 5
    retention_days: 7

models:
  flipto_snowplow_web:
    metrics:
      +virtual_env: true
      +pre-hook:
        - "{{ dbt_virtual.clone_if_new_fp() }}"
      +post-hook:
        - "{{ dbt_virtual.sync_env_view() }}"
    gold:
      procedures:
        +virtual_env: true
```

### 2.3 Cloud Workflows Integration (`flipto-terraform` `dbt-daily.yaml`)
1. **Nightly Staging Run (`master`)**:
   - Runs `dbt-virtual build --env staging --release $IMAGE_DIGEST --select state:modified+ tag:metrics_root tag:serving_tvf`.
   - Unchanged roots keep their fingerprint and run incremental updates in-place on `spacetime_physical.<root>__<fp>`.
   - Modified roots clone `spacetime_physical.<root>__<fp_prod>` → `spacetime_physical.<root>__<fp_new>`, run the incremental window, and point `spacetime_staging.<root>` at `<fp_new>`.
   - `stgmanage` (Platform staging) immediately reads `spacetime_staging.ft_report_*`.
2. **Release Promotion (`vYYYY.MM.DD`)**:
   - Runs `dbt-virtual promote --from staging --to prod --release $RELEASE_DIGEST`.
   - Zero rebuild of the 9 roots; `spacetime.<root>` views repoint to the gated physical tables and `spacetime.ft_report_*` / `spacetime.udf_spacetime_*` update in seconds.
3. **Automatic Rollback on Gate Failure**:
   - Runs `dbt-virtual rollback --env prod`.
   - Instantaneously repoints `spacetime.<root>` views and redeploys `last_good_release` TVFs/UDFs.
4. **Nightly Janitor Step**:
   - Runs `dbt-virtual gc --retention-releases 5 --retention-days 7`.
