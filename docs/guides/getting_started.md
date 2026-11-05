# Step-by-Step Getting Started Guide

This guide walks you through adding `dbt-virtual` to a new or existing dbt project from scratch, explaining what happens at every step.

---

## Prerequisites

- **dbt-core** `>= 1.8.0` with one of the supported adapters (`dbt-bigquery`, `dbt-snowflake`, `dbt-duckdb`, `dbt-postgres`, `dbt-databricks`)
- **Python** `>= 3.10` (for the `dbt-virtual` CLI planner & orchestrator)
- On **dbt-core 1.11+**, scalar UDFs use `materialized='virtual_function'` (dbt-core now owns `function`; see [Routines](routines_and_tvf_testing.md))

---

## Step 1: Install the dbt Package & Python CLI

### 1a. Add `dbt-virtual` to `packages.yml`
In the root of your dbt project, add `dbt-virtual` to `packages.yml`:

```yaml
packages:
  - git: "https://github.com/Flip-to/dbt-virtual.git"
    revision: main
```

Install the dbt macros into `dbt_packages/dbt_virtual`:
```bash
dbt deps
```

### 1b. Install the `dbt-virtual` CLI
Install the Python package in the same environment where you run `dbt`:

```bash
uv pip install git+https://github.com/Flip-to/dbt-virtual.git
# or with pip:
pip install git+https://github.com/Flip-to/dbt-virtual.git
```

Verify the CLI is installed:
```bash
dbt-virtual --help
```

---

## Step 2: Configure `dbt_project.yml`

Open your `dbt_project.yml` and add the `dbt_virtual` dictionary under `vars:`, the `on-run-start` seeding hook, and `+virtual_env: true` plus the `sync_env_view` post-hook on the model directories you want to virtualize:

```yaml
name: "my_analytics"
version: "1.0.0"
config-version: 2

vars:
  dbt_virtual:
    enabled: true
    physical_schema: "analytics_physical"
    environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"
    base_environment: "prod"
    ref_resolution: "env_view"   # Default Option A ('env_view'). Also supports 'physical', 'hybrid'
    env_schemas:                 # a string per env, or a mapping per custom schema (see Configuration Reference)
      staging: "analytics_staging"
      prod: "analytics"
    retention_releases: 5
    retention_days: 7

on-run-start:
  # Clone the base environment's table for changed incremental models BEFORE dbt materializes them.
  - "{{ dbt_virtual.seed_incremental_clones() }}"

models:
  my_analytics:
    # Opt in your models folder (can be enabled per-folder or per-model)
    marts:
      +virtual_env: true
      +post-hook:
        - "{{ dbt_virtual.sync_env_view() }}"
    routines:
      +virtual_env: true
```

!!! note "`clone_if_new_fp()` pre-hook"
    Older setups also added `+pre-hook: "{{ dbt_virtual.clone_if_new_fp() }}"`. It still parses but no longer clones: dbt's incremental materialization reads the existing relation before model pre-hooks run, so seeding moved to `seed_incremental_clones()` (the `on-run-start` hook above, and a step of `dbt-virtual build`). You can remove the pre-hook.

!!! tip "Incremental Adoption"
    You do **not** have to migrate your entire dbt project at once! Only models with `virtual_env: true` (set in `dbt_project.yml` or inside a model's `{{ config(virtual_env=true) }}` block) use virtual routing. All other models continue to behave like standard dbt models.

---

## Step 3: Wire Schema & Alias Routing in `macros/`

dbt determines where to materialize each model by calling two standard macros: `generate_schema_name` and `generate_alias_name`.

Create (or update) `macros/virtual_routing_overrides.sql` in your dbt project:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_schema_name(custom_schema_name, node)) }}
{%- endmacro %}


{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_alias_name(custom_alias_name, node)) }}
{%- endmacro %}
```

### What Do These Two Overrides Do?
1. **`virtual_schema_name`**:
   - Routes **table / incremental** models to `analytics_physical` (your `physical_schema`).
   - Routes **routines** (`table_function`, `virtual_function`, `procedure`) and standard `view` models to your active environment schema for their custom schema (`analytics_staging` when building in staging, `analytics` in prod, or e.g. `analytics_gold` for `config(schema='gold')` when prod maps custom schemas to their own datasets).
2. **`virtual_alias_name`**:
   - Appends the 12-character Merkle-DAG fingerprint suffix to physical tables: `orders_metrics` → `orders_metrics__58cce796d2a1`.
   - Keeps clean, un-suffixed names for environment views and routines (`analytics_staging.orders_metrics`, `analytics_staging.tvf_orders_report`).

Non-virtual models fall through to your existing logic, so if your project already has a custom `generate_schema_name`, keep its body and only add the `is_virtual_node` guard at the top.

### Optional: Override `ref()`
With dbt's own `ref()`, a model that reads a virtual table compiles against the physical table name (`analytics_physical.orders_metrics__58cce796d2a1`). To get the `ref_resolution` behaviour (`env_view` by default: read `<env_schema>.orders_metrics`), add this thin wrapper in `macros/virtual_routing_overrides.sql`:

```sql
{% macro ref() -%}
  {{ return(dbt_virtual.virtual_ref(*varargs, **kwargs)) }}
{%- endmacro %}
```

You do not need to override `is_incremental()`. `seed_incremental_clones()` creates the cloned table before dbt starts, so dbt's own `is_incremental()` is already true for a seeded model. `dbt_virtual.virtual_is_incremental()` (an alias of `dbt_virtual.is_incremental()`) still exists for projects that wired it in, and simply returns dbt's `is_incremental()`.

---

## Step 4: Write Your Models (Zero Changes Required!)

Your SQL models look like ordinary dbt models. Here is an incremental model example (`models/marts/orders_metrics.sql`):

```sql
{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    virtual_env=true
  )
}}

SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(amount) AS total_revenue,
    MAX(order_date) AS last_order_date
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
  WHERE order_date >= (SELECT MAX(last_order_date) FROM {{ this }})
{% endif %}
GROUP BY customer_id
```

---

## Step 5: Plan & Build in `staging`

`dbt-virtual plan` and `dbt-virtual build` pass `--env` to dbt as `--target`, so `profiles.yml` needs an output named `staging` (and `prod` for production builds).

### 5a. Export the warehouse state (from the second build on)
The planner only knows which physical tables and pointers exist through a state file written by `dbt-virtual status`:

```bash
dbt-virtual status --env staging --base-env prod --output state.json
```

`status` reads the registry tables in `physical_schema`, which the first build creates. On a brand-new project, skip this step and run `plan` / `build` without `--state-file`: every model is then `FULL_BUILD` or `ROUTINE_DEPLOY`, which is what a first build does anyway.

### 5b. Run `dbt-virtual plan`
`plan` runs `dbt compile --target staging` itself (skip it with `--skip-compile`), computes the Merkle-DAG fingerprints, writes them to `.dbt_virtual/fingerprints.json`, and shows what `dbt-virtual` will do for each model:

```bash
dbt-virtual plan --env staging                          # first build
dbt-virtual plan --env staging --state-file state.json  # later builds
```

Example output (your fingerprints will differ):
```text
dbt-virtual plan (env=staging) — 3 virtualized node(s):
  - [FULL_BUILD      ] orders_metrics                   [incremental   ] fp=58cce796d2a1 -> orders_metrics__58cce796d2a1 (upstreams=1)
  - [ROUTINE_DEPLOY  ] tvf_orders_report                [table_function] fp=d08c83eda55d -> tvf_orders_report (upstreams=2)
  - [ROUTINE_DEPLOY  ] udf_double_orders                [virtual_function] fp=3eb02715f7bb -> udf_double_orders (upstreams=0)
```

Use `--format json` for the full plan with a `reason` per node.

### 5c. Run `dbt-virtual build`
Execute the build for release `v1`:

```bash
dbt-virtual build --env staging --release v1
```

**What happened behind the scenes?**
1. `orders_metrics` was materialized as `analytics_physical.orders_metrics__58cce796d2a1` by `dbt build --target staging --select ...`, with the CLI's fingerprints passed as `--vars`.
2. Its post-hook (`sync_env_view`) created the virtual view `analytics_staging.orders_metrics` → `SELECT * FROM analytics_physical.orders_metrics__58cce796d2a1`.
3. `sync_env_view` recorded the active pointer in `analytics_physical._environments` and registered release `v1` in `analytics_physical._releases`; the routine materializations did the same for `udf_double_orders` and `tvf_orders_report`, storing their DDL as templates.
4. All dbt tests (`not_null`, `unique`, etc.) of the selected models ran against the newly built data in `staging`.
5. `virtual_snapshot_release` added every other virtual object of `staging` to release `v1`, so the release describes the whole environment even when only some models were rebuilt.

!!! warning "Build virtual models with `dbt-virtual build`, not bare `dbt run`"
    A bare `dbt run` / `dbt build` has no CLI fingerprints, so physical names are computed while dbt parses the project, without upstream fingerprints and with a different hash. A changed upstream model then does not give its downstream models new physical tables, and a bare run in staging can rebuild a table that prod points at ([tech debt](../tech_debt/bare_dbt_run_parse_time_fingerprint.md)). Set `require_cli_fingerprints: true` under `vars.dbt_virtual` to make such runs fail before anything is built ([Configuration Reference](configuration_reference.md)).

---

## Step 6: Promote `staging` → `prod` with Zero Rebuild!

Once `staging` passes your tests, promote release `v1` to `prod`:

```bash
dbt-virtual promote --from staging --to prod --release v1 --audit
```

**What happened during `promote`?**
1. **Quality Gate (`--audit`)**: Verified that `analytics_physical.orders_metrics__58cce796d2a1` is non-empty and did not suffer an unexpected row-count drop compared to `prod` (on BigQuery, row counts come from `__TABLES__` metadata, so no table is scanned).
2. **Pre-Flight Check**: Verified every physical table in release `v1` exists in `analytics_physical`.
3. **Ordered Zero-Rebuild Swap**:
   - **Environment views first**: `CREATE OR REPLACE VIEW analytics.orders_metrics AS SELECT * FROM analytics_physical.orders_metrics__58cce796d2a1`. Routines may read these views, so they must exist first.
   - **Routines in DAG order**: `udf_double_orders` before `tvf_orders_report`, which calls it, each deployed into `analytics` with its references re-pointed from `analytics_staging` to `analytics`. Routines at the same depth go UDFs, then table functions, then procedures.
   - Objects already at the same fingerprint in `prod` are skipped. The CLI holds the `prod` lease lock in `_locks` while it promotes.
   - **Demotion**: Dropped views/routines that were active in `prod`, are not in release `v1`, and are no longer virtual models in the project.
4. **Zero table compute**: Production now serves release `v1` immediately without rebuilding a single table!

---

## Step 7: Modify an Incremental Model (`SEED_INCREMENTAL`)

Suppose tomorrow you edit `models/marts/orders_metrics.sql` to add a filter or column.

1. Run `dbt-virtual status --env staging --base-env prod --output state.json` and `dbt-virtual plan --env staging --state-file state.json`:
   - `orders_metrics` now gets a new fingerprint (`2b8f65b7f303`).
   - Because `prod` already has an active table (`orders_metrics__58cce796d2a1`), the planner classifies `orders_metrics` as **`SEED_INCREMENTAL`**.
2. Run `dbt-virtual build --env staging --state-file state.json --release v2`:
   - Before dbt builds anything, `dbt-virtual build` (or the `on-run-start` hook `seed_incremental_clones()`) runs:
     ```sql
     CREATE TABLE IF NOT EXISTS `analytics_physical`.`orders_metrics__2b8f65b7f303`
     CLONE `analytics_physical`.`orders_metrics__58cce796d2a1`
     ```
   - `is_incremental()` evaluates to `True`, so dbt only processes the incremental delta on top of the zero-copy clone!
   - The clone has to exist before dbt starts: dbt's incremental materialization (dbt-core and dbt-bigquery 1.12) decides "new table" before model pre-hooks run, so a clone made in a pre-hook is replaced by a full build.
   - Meanwhile, `prod` (`analytics.orders_metrics`) is completely untouched and still points to `orders_metrics__58cce796d2a1` until you promote `v2`.

---

## Step 8: Instant Rollback & Garbage Collection

If you ever discover a data issue after promoting `v2` to `prod`, roll back to `v1` in seconds:

```bash
dbt-virtual rollback --env prod --to-release v1
```

Without `--to-release`, rollback goes to the previous promoted release; the releases it undoes are marked `rolled_back`, so running it again goes one more release back.

And periodically clean up old, unreferenced physical tables outside your retention window (`retention_releases: 5`, `retention_days: 7`):

```bash
# Preview orphan tables first
dbt-virtual gc --dry-run

# Drop orphan tables
dbt-virtual gc
```
