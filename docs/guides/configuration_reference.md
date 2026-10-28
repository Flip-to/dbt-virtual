# Configuration & Parameters Reference

This page documents every configuration parameter supported by `dbt-virtual`, both at the **project level** (`vars.dbt_virtual` in `dbt_project.yml`) and at the **model level** (`config(...)` in SQL files or `dbt_project.yml`).

---

## 1. Complete `dbt_project.yml` Example

Here is the standard `vars.dbt_virtual` block in `dbt_project.yml`:

```yaml
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
```

---

## 2. Project-Level Parameters (`vars.dbt_virtual`) Explained

### `enabled`
- **Type**: `boolean`
- **Default**: `true`
- **What it does**:
  Master kill-switch for `dbt-virtual`. When set to `true`, models with `virtual_env: true` are routed to `physical_schema` with content-addressed `<model>__<fp>` aliases and managed via virtual views.
- **When to change it**:
  Set `enabled: false` (or pass `--vars '{"dbt_virtual": {"enabled": false}}'`) if you ever want to temporarily bypass virtual environments and run standard in-place dbt builds.

---

### `physical_schema`
- **Type**: `string`
- **Default**: `"analytics_physical"`
- **What it does**:
  Defines the shared warehouse dataset/schema where:
  1. **Immutable physical tables** are materialized (`analytics_physical.<model>__<fingerprint>`).
  2. **State registry tables** are stored:
     - `analytics_physical._environments` (active model/routine pointers per environment)
     - `analytics_physical._releases` (immutable release history and routine DDL templates for promotion & rollback)
     - `analytics_physical._locks` (TTL-based concurrency leases per environment)
     - `analytics_physical._intervals` (processed `[start_ts, end_ts]` backfill intervals per fingerprint)
- **Best Practice**:
  Grant write permissions on `analytics_physical` to your CI/CD service account, while BI tools (Looker, Tableau, Metabase, Hex) and end users only query the virtual environment schemas (`analytics` and `analytics_staging`).

---

### `environment`
- **Type**: `string`
- **Default**: `"{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"` (falls back to `target.name` if omitted)
- **What it does**:
  Identifies the logical virtual environment currently being built or queried (for example: `staging`, `prod`, `pr_42`, `dev_alice`).
- **How it works with `DBT_VIRTUAL_ENV`**:
  By setting `environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"`, you can:
  - Use your normal dbt profile target (`--target staging`) and automatically build into the `staging` virtual environment.
  - Dynamically spin up ephemeral PR environments (`DBT_VIRTUAL_ENV=pr_123 dbt build` or `dbt-virtual build --env pr_123`) without adding a new target to `profiles.yml`!

---

### `base_environment`
- **Type**: `string`
- **Default**: `"prod"`
- **What it does**:
  Specifies the canonical production environment name. It serves three critical purposes:
  1. **Incremental Clone Source (`clone_if_new_fp`)**: When a model's SQL changes in `staging` or `pr_123`, `clone_if_new_fp()` looks up the active physical table for `base_environment` (`prod`) in `_environments` and clones it (`CREATE TABLE new_fp CLONE prod_fp`) so your incremental model starts with full production history.
  2. **Planner Baseline (`dbt-virtual plan`)**: Used to determine whether a modified incremental model can be classified as `SEED_INCREMENTAL` (base table exists in `prod`) vs `FULL_BUILD` (brand-new model).
  3. **Teardown Guardrail (`virtual_drop_env`)**: `dbt-virtual drop-env` refuses to drop `base_environment`, preventing accidental deletion of production views.

---

### `ref_resolution`
- **Type**: `string` (`"env_view"` | `"physical"` | `"hybrid"` | `"explicit"`)
- **Default**: `"env_view"`
- **What it does**:
  Controls how `{{ ref('upstream_model') }}` resolves when compiling a downstream model.

| Mode | How `{{ ref('orders') }}` Compiles | Best For |
|---|---|---|
| **`"env_view"`** *(Default, Option A)* | `analytics_staging.orders` (the virtual environment view) | **Recommended for almost all projects**, especially BigQuery projects with Table Functions (`TVF`), views, or external BI queries. When `orders` is promoted or updated, downstream views and TVFs automatically read the new table through the environment view without needing to be recompiled. |
| **`"physical"`** *(Option B)* | `analytics_physical.orders__58cce796d2a1` (the exact physical table) | Pure table/incremental pipelines with no TVFs where you want every compiled table query pinned directly to its upstream physical table identifier at compile time. |
| **`"hybrid"`** *(Option C)* | Routes routine/view callers (`table_function`, `view`) to `analytics_staging.orders`, and table/incremental callers (`table`, `incremental`) to `analytics_physical.orders__58cce796d2a1` | Projects that mix incremental tables with BigQuery TVFs and want direct physical table reads during incremental builds while keeping TVFs bound to stable environment view names. |
| **`"explicit"`** *(Option D)* | Standard `ref()` uses `"env_view"`, while callers can explicitly invoke `dbt_virtual.virtual_ref('orders')` | Gradual migration or advanced per-query overrides. |

---

### `env_schemas`
- **Type**: `dict[string, string]`
- **Default**:
  ```yaml
  env_schemas:
    staging: "analytics_staging"
    prod: "analytics"
  ```
- **What it does**:
  Maps logical environment names (`staging`, `prod`, etc.) to the exact warehouse dataset/schema where virtual views and routines (`table_function`, `function`, `procedure`) are created.
- **Automatic Fallback for Ephemeral Environments**:
  If you run with an environment name that is **not** explicitly listed in `env_schemas` (for example, `pr_42` or `dev_yuri`), `dbt-virtual` automatically derives the schema name using `<prod_schema>_<env>`:
  - `pr_42` → `"analytics_pr_42"`
  - `dev_yuri` → `"analytics_dev_yuri"`
  This means ephemeral CI pull request environments work out of the box with zero `dbt_project.yml` edits!

---

### `retention_releases`
- **Type**: `integer`
- **Default**: `5`
- **What it does**:
  Used by `dbt-virtual gc` (`virtual_gc` macro). Protects all physical tables (`<model>__<fp>`) referenced by the **most recent `N` releases** in `analytics_physical._releases` (per environment), guaranteeing you can always roll back at least `N` releases.

---

### `retention_days`
- **Type**: `integer`
- **Default**: `7`
- **What it does**:
  Used by `dbt-virtual gc` (`virtual_gc` macro). In addition to `retention_releases`, any physical table created or referenced within the last `retention_days` days is protected from garbage collection—even if it was created by an aborted or unpromoted staging/PR build.

---

## 3. Model-Level Parameters (`config(...)`)

You can configure these parameters either in `dbt_project.yml` under `models:` (using `+param_name:`) or inside an individual model's `{{ config(...) }}` block:

### `virtual_env`
- **Type**: `boolean`
- **Default**: `false`
- **Example**:
  ```sql
  {{ config(materialized='incremental', virtual_env=true) }}
  ```
- **What it does**:
  Enables `dbt-virtual` for the model or routine. Models without `virtual_env: true` are completely ignored by `dbt-virtual` and behave like standard dbt models.

Virtual table and incremental models must keep dbt's alias equal to the model name. A custom `alias` (or a custom alias-generation macro) makes dbt `ref()` resolve to a different environment-view name than dbt-virtual creates; the planner and routing macro fail early with an error when they detect this configuration.

---

### `virtual_env_epoch`
- **Type**: `integer`
- **Default**: `1`
- **Example**:
  ```sql
  {{ config(materialized='incremental', virtual_env=true, virtual_env_epoch=2) }}
  ```
- **What it does**:
  Included in the Merkle-DAG fingerprint hash payload. Incrementing `virtual_env_epoch` (e.g. from `1` to `2`) forces a brand-new fingerprint (`<model>__<new_fp>`) without changing the model's SQL query—useful when external source data was backfilled out-of-band and you want to force a clean table version.

---

### `physical_version`
- **Type**: `string | None`
- **Default**: `None`
- **Example**:
  ```sql
  {{ config(materialized='incremental', virtual_env=true, physical_version='58cce796d2a1') }}
  ```
- **What it does**:
  SQLMesh-style version pinning. Overrides the computed SHA-256 fingerprint for this model with the exact `physical_version` string provided, pinning the model to `analytics_physical.<model>__58cce796d2a1`.

---

### `forward_only`
- **Type**: `boolean`
- **Default**: `false`
- **Example**:
  ```sql
  {{ config(materialized='incremental', virtual_env=true, forward_only=true) }}
  ```
- **What it does**:
  Marks the model configuration as forward-only in fingerprint metadata, pairing with `physical_version` or `virtual_add_columns` when evolving large incremental tables without historical backfills.

---

### `virtual_add_columns`
- **Type**: `list[string]`
- **Default**: `[]`
- **Example**:
  ```sql
  {{
    config(
      materialized='incremental',
      virtual_env=true,
      virtual_add_columns=['discount_amount NUMERIC', 'coupon_code STRING']
    )
  }}
  ```
- **What it does**:
  When you add new columns to an incremental model, `clone_if_new_fp()` first clones the existing `prod` table (`CREATE TABLE new_fp CLONE prod_fp`), which has the *old* column schema. When `virtual_add_columns` is specified, `clone_if_new_fp()` immediately runs:
  ```sql
  ALTER TABLE `analytics_physical`.`orders_metrics__newfp`
  ADD COLUMN IF NOT EXISTS discount_amount NUMERIC;
  ALTER TABLE `analytics_physical`.`orders_metrics__newfp`
  ADD COLUMN IF NOT EXISTS coupon_code STRING;
  ```
  before dbt executes the incremental `INSERT` / `MERGE`.

---

### Routine Materialization Parameters (`table_function`, `function`, `procedure`)

When virtualizing database routines:

```sql
{{
  config(
    materialized='table_function',
    parameters='min_orders INT64, start_date DATE',
    virtual_env=true
  )
}}
SELECT customer_id, orders
FROM {{ tvf_test_ref('orders_metrics') }}
WHERE orders >= min_orders
  AND order_date >= start_date
```

| Parameter | Used By | Description |
|---|---|---|
| `materialized` | `table_function`, `function`, `procedure` | Selects the custom routine materialization provided by `dbt-virtual`. |
| `parameters` | All routines | Comma-separated SQL parameter signature (e.g. `"n INT64"` or `"min_o INT64"`). |
| `returns` | `function` (and Snowflake/Postgres `table_function`) | Return type declaration (e.g. `"INT64"` or `"TABLE(customer_id VARCHAR, orders NUMBER)"`). Not required for BigQuery `table_function` (which infers output columns from the `AS (SELECT ...)` query). |

---

## 4. Runtime CLI / `dbt --vars` Variables

These internal variables are automatically passed by the `dbt-virtual` CLI when invoking `dbt`, or can be passed manually via `dbt build --vars '{...}'`:

| Variable Name | Type | Description |
|---|---|---|
| `_dbt_virtual_fps` | `dict[str, str]` | Precomputed map of `{model_name: fingerprint}` from `target/virtual_fingerprints.json`. If omitted, `dbt-virtual` falls back to computing Merkle-DAG fingerprints in pure Jinja (`get_fingerprint.sql`). |
| `_dbt_virtual_release` | `str` | Release identifier (e.g. `"v2026.10.27"` or git SHA) recorded in `analytics_physical._releases` and `analytics_physical._environments` by `sync_env_view()`. |
| `_dbt_virtual_start` | `str` | Optional start timestamp/date of an incremental backfill window (`--start`), recorded in `analytics_physical._intervals`. |
| `_dbt_virtual_end` | `str` | Optional end timestamp/date of an incremental backfill window (`--end`), recorded in `analytics_physical._intervals`. |
| `tvf_test_fixtures` | `bool` | When `true`, `{{ tvf_test_ref('model') }}` resolves to `<model>__fixture` so BigQuery TVFs can be unit-tested with `ANY TABLE` synthetic CTE inputs. |
