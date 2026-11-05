# Configuration & Parameters Reference

This page documents every configuration parameter supported by `dbt-virtual`, both at the **project level** (`vars.dbt_virtual` in `dbt_project.yml`) and at the **model level** (`config(...)` in SQL files or `dbt_project.yml`).

---

## 1. Complete `dbt_project.yml` Example

Here is the standard `vars.dbt_virtual` block in `dbt_project.yml`, for a project whose environments each expose their models in one schema:

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

on-run-start:
  # Clone the base environment's table for changed incremental models BEFORE dbt materializes them.
  - "{{ dbt_virtual.seed_incremental_clones() }}"
```

A project that publishes to **several datasets** in production (for example marts in `analytics` and `analytics_gold`, table functions in a dataset that applications query) maps each environment to a set of schemas instead; see [`env_schemas`](#env_schemas) below for the full BigQuery example.

Always set the keys you rely on: `vars` are not deep-merged, so a `vars.dbt_virtual` block in your project replaces the package's own block as a whole, and any key you leave out takes the built-in default listed below (`macros/fingerprint/get_fingerprint.sql`, `get_config`).

---

## 2. Project-Level Parameters (`vars.dbt_virtual`) Explained

### `enabled`
- **Type**: `boolean`
- **Default**: `true`
- **What it does**:
  Master kill-switch for `dbt-virtual`. When set to `true`, models with `virtual_env: true` are routed to `physical_schema` with content-addressed `<model>__<fp>` aliases and managed via virtual views.
- **When to change it**:
  Set `enabled: false` if you ever want to temporarily bypass virtual environments and run standard in-place dbt builds. Because `--vars` replaces the whole `dbt_virtual` mapping, pass the complete block with `enabled: false`, or set it in `dbt_project.yml`.

---

### `physical_schema`
- **Type**: `string`
- **Default when omitted**: `"dbt_virtual_physical"` (every example in these guides sets `"analytics_physical"`)
- **What it does**:
  Defines the one shared warehouse dataset/schema where:
  1. **Immutable physical tables** of every `table` / `incremental` virtual model are materialized (`analytics_physical.<model>__<fingerprint>`), whatever the model's custom schema and whatever environment built it.
  2. **State registry tables** are stored:
     - `analytics_physical._environments` (active model/routine pointers per environment, with the object's custom schema in `schema_key`)
     - `analytics_physical._releases` (immutable release history and routine DDL templates for promotion & rollback)
     - `analytics_physical._locks` (TTL-based concurrency leases per environment)
     - `analytics_physical._intervals` (processed `[start_ts, end_ts]` backfill intervals per fingerprint)
  The registry is created the first time it is needed. A registry created before `schema_key` existed gets the column added (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS schema_key`) on the next run; rows without it mean "the environment's default schema".
- **Best Practice**:
  Grant write permissions on `analytics_physical` to your CI/CD service account, while BI tools (Looker, Tableau, Metabase, Hex) and end users only query the environment schemas (`analytics`, `analytics_staging`, ...). `dbt-virtual drop-env` refuses to drop this schema.

---

### `physical_schemas_by_location` (multi-region BigQuery)
- **Type**: `dict[string, string]` (location to physical schema)
- **Default when omitted**: `{}` (one physical schema, one registry)
- **Example**:
  ```yaml
  vars:
    dbt_virtual:
      physical_schema: "rt_virtual_physical"            # default and EU
      physical_schemas_by_location:
        EU: "rt_virtual_physical"
        US: "rt_virtual_physical_us"
      schema_locations:                                  # optional: enables the mismatch check
        rt: EU
        rt_staging: EU
        rt_us: US
        rt_staging_us: US
      env_schemas:
        prod:    {default: "rt",         us: "rt_us"}
        staging: {default: "rt_staging", us: "rt_staging_us"}
  models:
    my_project:
      +virtual_env: true
      us_marts:
        +schema: us
        +virtual_location: US
  ```
- **What it does**:
  A BigQuery view cannot read a table in another location, `CLONE` needs the same location and a job runs in one location, so each location gets its own physical schema and its own registry (`_environments`, `_releases`, `_intervals`, `_locks`). A model's location is `+virtual_location` (or `meta.virtual_location`), else the profile's `location`. `physical_schema` is the schema for every location not listed and is iterated as a registry only when you set it yourself. A location with no entry and no `physical_schema`, or a model with no location, is a compiler error.
  - **Routing**: physical tables go to their location's schema. Environment views and routines go to the environment dataset of the model's custom schema: map the custom schema of US models (`schema: us`) to a US dataset in `env_schemas`, as above.
  - **Validation**: with `schema_locations` (dataset to location), parsing fails when a virtual model's physical dataset or the dataset of any environment in `env_schemas` is declared in another location than the model. Datasets you do not declare are not checked. With `schema_locations` set, an unmapped environment such as `pr_42` gets one schema per base schema (`rt_pr_42`, `rt_us_pr_42`).
  - **Operations**: `promote`, `rollback`, `audit`, `gc`, `drop-env`, locks and `status` run once per location (one job per location); promote and rollback fail only when no location has the release. The `location` argument of the macros (`dbt run-operation virtual_promote --args '{..., location: US}'`) limits an operation to one location (`default` stands for `physical_schema`).
  - **Datasets** the package creates are created with `OPTIONS (location = ...)`.
  - **Jobs**: a dbt BigQuery connection has one job location. Leave `location` unset in the profile so BigQuery infers it from the dataset in each statement, or build each location with its own target and `--select`. Not verified on real BigQuery (see decision [`20261105_physical_location_per_region`](../decisions/20261105_physical_location_per_region.md)).

---

### `environment`
- **Type**: `string` (may contain Jinja)
- **Default when omitted**: `target.name`
- **What it does**:
  Identifies the logical virtual environment currently being built or queried (for example: `staging`, `prod`, `pr_42`, `dev_alice`). It decides which `env_schemas` entry views and routines are written to and under which environment their pointers are recorded in `_environments`.
- **Precedence**: `dbt-virtual build --env <name>` passes `--vars '{_dbt_virtual_env: <name>}'`, which wins over this setting (`get_current_env`). The setting matters for bare `dbt run` / `dbt build` and for `on-run-start` hooks.
- **Jinja is rendered**: dbt only renders a `var` whose whole value is a string, so Jinja nested inside the `dbt_virtual` mapping arrives unrendered; `dbt-virtual` renders every string value itself. Covered by `tests/test_dbt_duckdb_regressions.py::test_bare_dbt_run_renders_nested_config_and_registers_physical_fingerprint` (with `"{{ var('_dbt_virtual_env', target.name) }}"`).
- **Mapping several dbt targets to one environment**: if production runs under several profile targets (a scheduler service account, a backfill target, ...), map all of them to `prod` so they update the same environment:
  ```yaml
  vars:
    dbt_virtual:
      environment: "{{ 'prod' if target.name in ['prod', 'prod_scheduler', 'prod_backfill'] else env_var('DBT_VIRTUAL_ENV', target.name) }}"
  ```
- **Ephemeral environments**: with `environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"`, `DBT_VIRTUAL_ENV=pr_123 dbt build --target staging` builds into the `pr_123` environment without a new profile target. `dbt-virtual build --env pr_123` instead passes `--target pr_123` to dbt, so it needs an output named `pr_123` in `profiles.yml` (see [CLI reference](cli_and_operations.md)). Prefer `dbt-virtual build`: a bare `dbt build` cannot include upstream fingerprints in physical table names ([tech debt](../tech_debt/bare_dbt_run_parse_time_fingerprint.md)).

---

### `base_environment`
- **Type**: `string`
- **Default**: `"prod"`
- **What it does**:
  Specifies the canonical production environment name. It serves four purposes:
  1. **Incremental Clone Source (`seed_incremental_clones`)**: When a model's SQL changes in `staging` or `pr_123`, `seed_incremental_clones()` (run by `dbt-virtual build` and by the `on-run-start` hook, before dbt materializes anything) looks up the active physical table for `base_environment` (`prod`) in `_environments` and clones it (`CREATE TABLE new_fp CLONE prod_fp`) so your incremental model starts with full production history.
  2. **Planner Baseline (`dbt-virtual plan`)**: `base_env_state` in the state file (`dbt-virtual status --base-env prod --output state.json`) decides whether a modified incremental model is `SEED_INCREMENTAL` (base table exists) or `FULL_BUILD`.
  3. **Schema of unmapped environments**: an environment not listed in `env_schemas` gets `<base_environment default schema>_<env>` (see below).
  4. **Teardown Guardrail (`virtual_drop_env`)**: `dbt-virtual drop-env` refuses to drop `base_environment`, `physical_schema`, or any schema that belongs to another environment listed in `env_schemas`.

---

### `ref_resolution`
- **Type**: `string` (`"env_view"` | `"physical"` | `"hybrid"`)
- **Default**: `"env_view"`
- **What it does**:
  Controls how `{{ ref('upstream_model') }}` resolves when compiling a downstream model (requires the `ref()` override shown in [Getting Started](getting_started.md)).

| Mode | How `{{ ref('orders') }}` Compiles | Best For |
|---|---|---|
| **`"env_view"`** *(Default, Option A)* | `analytics_staging.orders` (the virtual environment view, in the schema of `orders`' custom schema) | **Recommended for almost all projects**, especially BigQuery projects with Table Functions (`TVF`), views, or external BI queries. When `orders` is promoted or updated, downstream views and TVFs automatically read the new table through the environment view without needing to be recompiled. |
| **`"physical"`** *(Option B)* | `analytics_physical.orders__58cce796d2a1` (the exact physical table) | Pure table/incremental pipelines with no TVFs where you want every compiled table query pinned directly to its upstream physical table identifier at compile time. |
| **`"hybrid"`** *(Option C)* | Routes routine/view callers (`table_function`, `virtual_function`, `function`, `procedure`, `view`) to `analytics_staging.orders`, and table/incremental callers to `analytics_physical.orders__58cce796d2a1` | Projects that mix incremental tables with BigQuery TVFs and want direct physical table reads during incremental builds while keeping TVFs bound to stable environment view names. |

`"explicit"` (the former Option D) and unknown values are rejected with a compile error; `"explicit"` used to behave exactly like `"physical"` (see [the decision](../decisions/20261105_ref_resolution_explicit_is_rejected.md)).

---

### `env_schemas`
- **Type**: `dict[string, string | dict[string, string]]`
- **Default when omitted**: `{"staging": "staging", "prod": "prod"}`
- **What it does**:
  Maps each logical environment to the warehouse dataset(s)/schema(s) where its environment views and routines (`table_function`, `virtual_function`, `procedure`) are created. Physical tables never go here; they always go to `physical_schema`.

Each environment entry takes one of two forms:

| Form | Example | Where an object with `config(schema='<c>')` lands |
|---|---|---|
| **String** | `staging: "analytics_staging"` | Always in that one schema, whatever `<c>` is (every custom schema collapses into it). |
| **Mapping** | `prod: {default: "analytics", spacetime: "spacetime"}` | Models without a custom schema go to `default`. A listed custom schema goes to its own entry. An unlisted custom schema `<c>` goes to `<default>_<c>`, the rule of dbt's default `generate_schema_name`. If `default` is missing, `target.schema` is used. |

**Multi-dataset BigQuery example.** Production keeps the datasets consumers already query; staging and dev use one dataset each:

```yaml
vars:
  dbt_virtual:
    physical_schema: "analytics_physical"
    environment: "{{ 'prod' if target.name in ['prod', 'prod_scheduler'] else env_var('DBT_VIRTUAL_ENV', target.name) }}"
    base_environment: "prod"
    env_schemas:
      prod:
        default: "analytics"        # models without config(schema=...)
        spacetime: "spacetime"      # config(schema='spacetime'): table functions applications call
        # config(schema='gold') is not listed, so it becomes "analytics_gold"
      staging: "analytics_staging"  # one dataset for everything
```

With models `daily` (table, no custom schema), `orders` (table, `schema='gold'`) and `tvf_bookings` (`table_function`, `schema='spacetime'`):

| Object | `prod` | `staging` | `pr_42` (not listed) |
|---|---|---|---|
| `daily` env view | `analytics.daily` | `analytics_staging.daily` | `analytics_pr_42.daily` |
| `orders` env view | `analytics_gold.orders` | `analytics_staging.orders` | `analytics_pr_42.orders` |
| `tvf_bookings` | `spacetime.tvf_bookings` | `analytics_staging.tvf_bookings` | `analytics_pr_42.tvf_bookings` |
| Physical tables | `analytics_physical.daily__<fp>`, `analytics_physical.orders__<fp>` | same tables | same tables |

The same layout, run on DuckDB, is covered by `tests/test_dbt_duckdb_multi_schema.py`. When `tvf_bookings` is promoted from staging to prod, its references to `analytics_staging.orders` are rewritten to `analytics_gold.orders`, because each referenced virtual object keeps its own placeholder in the stored DDL (see [Routines](routines_and_tvf_testing.md)).

- **Automatic fallback for unlisted environments**:
  An environment that is **not** listed in `env_schemas` (for example `pr_42` or `dev_yuri`) gets **one** schema, `<base_environment default schema>_<env>`, for every custom schema:
  - `pr_42` → `"analytics_pr_42"`
  - `dev_yuri` → `"analytics_dev_yuri"`
  Characters other than letters, digits and `_` in the environment name become `_`. If `base_environment` itself is not listed, the prefix is `target.schema`. `sync_env_view` and `promote` create the schema when it does not exist, so ephemeral PR environments need no `dbt_project.yml` edit.
- **Changing a mapping later**: the registry stores each object's custom schema (`schema_key`), not its full location, so promote, rollback, demotion and `drop-env` always use the current mapping (decision [`20261104_per_custom_schema_env_datasets`](../decisions/20261104_per_custom_schema_env_datasets.md)).

---

### `retention_releases`
- **Type**: `integer`
- **Default**: `5`
- **What it does**:
  Used by `dbt-virtual gc` (`virtual_gc` macro). Protects all physical tables (`<model>__<fp>`) referenced by the **most recent `N` releases of each environment** in `analytics_physical._releases` (candidate, promoted and rolled-back releases), so you can always roll back at least `N` releases, however many PR releases were built meanwhile.

---

### `retention_days`
- **Type**: `integer`
- **Default**: `7`
- **What it does**:
  Used by `dbt-virtual gc` (`virtual_gc` macro). In addition to `retention_releases`, any physical table created or referenced within the last `retention_days` days is protected from garbage collection—even if it was created by an aborted or unpromoted staging/PR build.

---

### `partial_build_vars`
- **Type**: list of var names
- **Default**: `[]`
- **What it does**:
  Names of dbt vars whose presence means a windowed build (e.g. `["backfill_start", "ci_core_window_start"]`). When one is set, `sync_env_view` registers the table as `build_quality = 'partial'` and `virtual_promote` refuses it. dbt `--empty` and `_dbt_virtual_partial: true` mark a build partial without being listed. Do not give these vars a default in `dbt_project.yml` unless every run is a windowed one: a set var counts as partial.

---

### `coverage_ignore_before`
- **Type**: date string (e.g. `"2026-01-01"`)
- **Default**: unset
- **What it does**:
  The BigQuery partition coverage audit ignores partitions older than this date (`--coverage-ignore-before` / the macro argument override it).

---

### `require_cli_fingerprints`
- **Type**: `boolean` (or a string that renders to `true` / `1` / `yes`, e.g. `"{{ env_var('DBT_VIRTUAL_STRICT', 'false') }}"`)
- **Default**: `false`
- **What it does**:
  Refuses bare dbt runs that would name virtual tables without upstream fingerprints ([tech debt](../tech_debt/bare_dbt_run_parse_time_fingerprint.md)). When `true`, a `dbt run`, `dbt build` or `dbt retry` whose selection contains a virtual `table` or `incremental` model fails in `on-run-start`, before any model is built, unless `_dbt_virtual_fps` or the `DBT_VIRTUAL_FPS` environment variable is set. `dbt-virtual build` and `dbt-virtual dbt` always set it, so the CLI is unaffected; so are `compile`, `parse`, `ls`, `docs` and `run-operation`, and selections with only views or routines.
- **Requires** the package's `on-run-start` hook: either `"{{ dbt_virtual.seed_incremental_clones() }}"` or `"{{ dbt_virtual.on_run_start() }}"` (both run the check, then seed clones).
- **Error you will see**: `dbt-virtual: require_cli_fingerprints is true but var('_dbt_virtual_fps') is not set, so N selected virtual table model(s) (...) would get physical names without upstream fingerprints ...`.

---

### `guard_shared_tables`
- **Type**: `boolean` (or a string rendering to `false` / `0` / `no`)
- **Default**: `true`
- **What it does**:
  A narrow write guard for runs without CLI fingerprints (no `_dbt_virtual_fps`, no `DBT_VIRTUAL_FPS`), see [tech debt](../tech_debt/bare_dbt_run_parse_time_fingerprint.md). In `on-run-start` (`on_run_start()` or `seed_incremental_clones()`), for each selected virtual `table` / `incremental` model it computes the run-time Merkle fingerprint from the graph. If the physical table the model would write is registered in `_environments` for another environment with a different fingerprint, the run fails before anything is built. A table only the current environment points at, or nobody, is never refused. Bare runs register the run-time fingerprint, so a rebuild with unchanged parents passes.
- **Does not cover**: names. A bare run still names `down__<own hash>`; the guard only stops it from overwriting another environment's table. Use `dbt-virtual dbt ...` or `DBT_VIRTUAL_FPS` for correct names. Tables registered by a bare run before this guard existed carry the alias suffix as fingerprint and are refused once, until rebuilt via the CLI.

---

### `use_ctas_for_clone`
- **Type**: `boolean`
- **Default**: `false`
- **What it does**:
  On BigQuery, Snowflake and Databricks (experimental, see [Warehouse Support](warehouse_support.md)), seeds and adoptions use `CREATE TABLE ... AS SELECT *` instead of a zero-copy `CLONE`. Meant for emulators and engines without `CLONE`; a target named `emulator` gets the same behaviour (`macros/adapters/dispatch.sql`, `*__clone_table_sql`). DuckDB and PostgreSQL always copy with `CREATE TABLE ... AS SELECT *`.

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

Virtual table and incremental models must keep dbt's alias equal to the model name. A custom `alias` (or a custom alias-generation macro) makes dbt `ref()` resolve to a different environment-view name than dbt-virtual creates; the planner and routing macro fail early with an error when they detect this configuration. Virtual model names must also be unique across the project (the planner fails on duplicates), because pointers are keyed by name.

---

### `virtual_location`
- **Type**: `string` (a BigQuery location such as `EU`, `US`, `europe-west1`)
- **Default**: the target's `location`, else none
- **What it does**:
  Location of the model's datasets in a project that sets [`physical_schemas_by_location`](#physical_schemas_by_location-multi-region-bigquery). Chooses the physical schema and registry of the model. Case is ignored. dbt-bigquery has no model-level `location` config, so this key is specific to dbt-virtual.

---

### `schema` (dbt's custom schema)
- **Type**: `string`
- **Default**: none
- **What it does**:
  For a virtual `table` / `incremental` model it decides where the **environment view** is created (the physical table always goes to `physical_schema`); for a routine it decides where the routine is created. The schema is looked up in [`env_schemas`](#env_schemas) for the current environment.

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
  Marks the model as forward-only, pairing with `physical_version` or `virtual_add_columns` when evolving large incremental tables without historical backfills. Like dbt's `on_schema_change`, it is metadata about how later runs treat the table, so it is **not** part of the fingerprint: setting or changing it does not create a new physical table.

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
  When you add new columns to an incremental model, `seed_incremental_clones()` first clones the existing `prod` table (`CREATE TABLE new_fp CLONE prod_fp`), which has the *old* column schema. When `virtual_add_columns` is specified, `seed_incremental_clones()` immediately runs:
  ```sql
  ALTER TABLE `analytics_physical`.`orders_metrics__newfp`
  ADD COLUMN IF NOT EXISTS discount_amount NUMERIC;
  ALTER TABLE `analytics_physical`.`orders_metrics__newfp`
  ADD COLUMN IF NOT EXISTS coupon_code STRING;
  ```
  before dbt executes the incremental `INSERT` / `MERGE`.

---

### Routine Materialization Parameters (`table_function`, `virtual_function`, `procedure`)

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
FROM {{ dbt_virtual.tvf_test_ref('orders_metrics') }}
WHERE orders >= min_orders
  AND order_date >= start_date
```

| Parameter | Used By | Description |
|---|---|---|
| `materialized` | `table_function`, `virtual_function`, `procedure` | Selects the routine materialization provided by `dbt-virtual`. Use `virtual_function` for scalar UDFs: on dbt-core 1.11+ the built-in `function` materialization replaces the package's `function` (see [Routines](routines_and_tvf_testing.md)). |
| `parameters` | All routines | Comma-separated SQL parameter signature (e.g. `"n INT64"` or `"min_o INT64"`). |
| `returns` | `virtual_function` (and Snowflake/Postgres `table_function`) | Return type declaration (e.g. `"INT64"` or `"TABLE(customer_id VARCHAR, orders NUMBER)"`). Not required for BigQuery `table_function` (which infers output columns from the `AS (SELECT ...)` query). |

Projects that keep their **own** routine materializations register them with [`register_routine_deployment`](routines_and_tvf_testing.md).

---

## 4. Runtime CLI / `dbt --vars` Variables

These internal variables are passed by the `dbt-virtual` CLI when invoking `dbt`, or can be passed manually via `dbt build --vars '{...}'`:

| Variable Name | Type | Description |
|---|---|---|
| `_dbt_virtual_fps` | `dict[str, str]` | Map of `{model_name: fingerprint}` computed by `dbt-virtual plan` / `build` (also written to `.dbt_virtual/fingerprints.json`). If omitted (bare `dbt run`), the physical name comes from a pure-Jinja fallback that dbt evaluates at parse time, without upstream fingerprints and with a different hash than the CLI's ([tech debt](../tech_debt/bare_dbt_run_parse_time_fingerprint.md)). |
| `_dbt_virtual_env` | `str` | Environment being built; set by `dbt-virtual build --env`. Wins over `vars.dbt_virtual.environment`. |
| `_dbt_virtual_release` | `str` | Release identifier (e.g. `"v2026.10.27"` or git SHA) recorded in `analytics_physical._releases` and `analytics_physical._environments` by `sync_env_view()` and `register_routine_deployment()`. |
| `_dbt_virtual_start` | `str` | Optional start timestamp/date of an incremental backfill window (`--start`), recorded in `analytics_physical._intervals`. |
| `_dbt_virtual_end` | `str` | Optional end timestamp/date of an incremental backfill window (`--end`), recorded in `analytics_physical._intervals`. |
| `tvf_test_fixtures` | `bool` | When `true`, `{{ dbt_virtual.tvf_test_ref('model') }}` resolves to `<model>__fixture` so BigQuery TVFs can be unit-tested with `ANY TABLE` synthetic CTE inputs. |
