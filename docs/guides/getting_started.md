# Step-by-Step Getting Started Guide

This guide walks you through adding `dbt-virtual` to a new or existing dbt project from scratch, explaining what happens at every step.

---

## Prerequisites

- **dbt-core** `>= 1.8.0` with one of the supported adapters (`dbt-bigquery`, `dbt-snowflake`, `dbt-duckdb`, `dbt-postgres`, `dbt-databricks`)
- **Python** `>= 3.10` (for the `dbt-virtual` CLI planner & orchestrator)

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

Open your `dbt_project.yml` and add the `dbt_virtual` dictionary under `vars:`. Then enable `+virtual_env: true` and the lifecycle hooks on the model directories you want to virtualize:

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
    env_schemas:
      staging: "analytics_staging"
      prod: "analytics"
    retention_releases: 5
    retention_days: 7

models:
  my_analytics:
    # Opt in your models folder (can be enabled per-folder or per-model)
    marts:
      +virtual_env: true
      +pre-hook:
        - "{{ dbt_virtual.clone_if_new_fp() }}"
      +post-hook:
        - "{{ dbt_virtual.sync_env_view() }}"
    routines:
      +virtual_env: true
```

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
   - Routes **routines** (`table_function`, `function`, `procedure`) and standard `view` models to your active environment schema (`analytics_staging` when building in staging, `analytics` in prod).
2. **`virtual_alias_name`**:
   - Appends the 12-character Merkle-DAG fingerprint suffix to physical tables: `orders_metrics` → `orders_metrics__58cce796d2a1`.
   - Keeps clean, un-suffixed names for environment views and routines (`analytics_staging.orders_metrics`, `analytics_staging.tvf_orders_report`).

### Optional: Override `ref()` and `is_incremental()`
If you ever want to switch `ref_resolution` from `"env_view"` (Option A) to `"physical"` (Option B) or `"hybrid"` (Option C), also add these two thin wrappers in `macros/virtual_routing_overrides.sql`:

```sql
{% macro ref(model_name) -%}
  {{ return(dbt_virtual.virtual_ref(model_name, packagename=none)) }}
{%- endmacro %}

{% macro is_incremental() -%}
  {{ return(dbt_virtual.virtual_is_incremental()) }}
{%- endmacro %}
```

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

### 5a. Compile the project
First, run `dbt compile` so `target/manifest.json` reflects your current SQL:

```bash
dbt compile --target staging
```

### 5b. Run `dbt-virtual plan`
Inspect the Merkle-DAG fingerprints and see what `dbt-virtual` will do for each model:

```bash
dbt-virtual plan --env staging
```

Example output:
```text
dbt-virtual plan (env=staging, fingerprints_file=target/virtual_fingerprints.json)
====================================================================================================
MODEL                            TYPE             FINGERPRINT    ACTION             REASON
----------------------------------------------------------------------------------------------------
orders_metrics                   incremental      58cce796d2a1   FULL_BUILD         No physical table for fingerprint 58cce796d2a1 and no base table to seed from
udf_double_orders                function         3eb02715f7bb   ROUTINE_DEPLOY     Routine deployed into target environment schema
tvf_orders_report                table_function   d08c83eda55d   ROUTINE_DEPLOY     Routine deployed into target environment schema
----------------------------------------------------------------------------------------------------
Summary: 3 virtualized node(s) | Actions: {'FULL_BUILD': 1, 'ROUTINE_DEPLOY': 2}
```

### 5c. Run `dbt-virtual build`
Execute the build for release `v1`:

```bash
dbt-virtual build --env staging --release v1
```

**What happened behind the scenes?**
1. `orders_metrics` was materialized as `analytics_physical.orders_metrics__58cce796d2a1`.
2. Its post-hook (`sync_env_view`) created the virtual view `analytics_staging.orders_metrics` → `SELECT * FROM analytics_physical.orders_metrics__58cce796d2a1`.
3. `sync_env_view` recorded the active pointer in `analytics_physical._environments` and registered release `v1` in `analytics_physical._releases`.
4. All dbt tests (`not_null`, `unique`, etc.) ran against the newly built data in `staging`.

---

## Step 6: Promote `staging` → `prod` with Zero Rebuild!

Once `staging` passes your tests, promote release `v1` to `prod`:

```bash
dbt-virtual promote --from staging --to prod --release v1 --audit
```

**What happened during `promote`?**
1. **Quality Gate (`--audit`)**: Verified that `analytics_physical.orders_metrics__58cce796d2a1` is non-empty and did not suffer an unexpected row-count drop compared to `prod`.
2. **Pre-Flight Check**: Verified every physical table in release `v1` exists in `analytics_physical`.
3. **Tiered Zero-Rebuild Swap**:
   - **Tier 1 (`function`)**: Deployed scalar UDFs into `analytics`.
   - **Tier 2 (`model`)**: Executed `CREATE OR REPLACE VIEW analytics.orders_metrics AS SELECT * FROM analytics_physical.orders_metrics__58cce796d2a1`.
   - **Tier 3 (`table_function`)**: Deployed TVFs into `analytics`.
   - **Tier 4 (`procedure`)**: Deployed stored procedures into `analytics`.
   - **Demotion**: Dropped any obsolete views/routines in `analytics` that were deleted in release `v1`.
4. **Zero table compute**: Production now serves release `v1` immediately without rebuilding a single table!

---

## Step 7: Modify an Incremental Model (`SEED_INCREMENTAL`)

Suppose tomorrow you edit `models/marts/orders_metrics.sql` to add a filter or column.

1. Run `dbt compile --target staging` and `dbt-virtual plan --env staging`:
   - `orders_metrics` now gets a new fingerprint (`2b8f65b7f303`).
   - Because `prod` already has an active table (`orders_metrics__58cce796d2a1`), the planner classifies `orders_metrics` as **`SEED_INCREMENTAL`**.
2. Run `dbt-virtual build --env staging --release v2`:
   - Before dbt runs the model, the pre-hook `clone_if_new_fp()` runs:
     ```sql
     CREATE TABLE IF NOT EXISTS `analytics_physical`.`orders_metrics__2b8f65b7f303`
     CLONE `analytics_physical`.`orders_metrics__58cce796d2a1`
     ```
   - `is_incremental()` evaluates to `True`, so dbt only processes the incremental delta on top of the zero-copy clone!
   - Meanwhile, `prod` (`analytics.orders_metrics`) is completely untouched and still points to `orders_metrics__58cce796d2a1` until you promote `v2`.

---

## Step 8: Instant Rollback & Garbage Collection

If you ever discover a data issue after promoting `v2` to `prod`, roll back to `v1` in seconds:

```bash
dbt-virtual rollback --env prod --to-release v1
```

And periodically clean up old, unreferenced physical tables outside your retention window (`retention_releases: 5`, `retention_days: 7`):

```bash
# Preview orphan tables first
dbt-virtual gc --dry-run

# Drop orphan tables
dbt-virtual gc
```
