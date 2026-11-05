# Migrating an Existing dbt Project (`dbt-virtual adopt`)

If you have an existing production dbt project (for example on BigQuery or Snowflake) with **multi-terabyte incremental tables**, a critical question is:

> **"Will adopting `dbt-virtual` force a full rebuild of our huge production tables or incur massive warehouse compute costs?"**

**Answer: No (`$0` compute, `0` bytes scanned).**

`dbt-virtual` includes a first-class **`dbt-virtual adopt`** command (`virtual_adopt` macro) that migrates your existing production tables into `analytics_physical.<model>__<fp>` using **zero-copy `CLONE`** (`CREATE TABLE ... CLONE ...`), replaces the legacy table with a virtual view pointing to the clone, and seeds the `_environments` and `_releases` state registries—all as metadata-only operations!

---

## 1. Cost Breakdown When Migrating Huge Tables on BigQuery / Snowflake

| Operation | What Happens | Compute Cost | Storage Cost |
|---|---|---|---|
| **1. Initial Adoption (`dbt-virtual adopt --env prod`)** | Runs `CREATE TABLE analytics_physical.<model>__<fp> CLONE analytics.<model>` | **$0.00** (Metadata-only operation in BigQuery & Snowflake; 0 bytes scanned) | **$0.00 extra** (BigQuery & Snowflake clones share underlying storage blocks with the base table; once `analytics.<model>` is converted to a `VIEW`, total storage is 1x) |
| **2. First Staging Build (`dbt-virtual build --env staging`)** | Unchanged models are classified as **`REUSE`**. Changed incremental models zero-copy `CLONE` from `prod` (`SEED_INCREMENTAL`) and only process the new delta partition! | **$0.00** for unchanged models; **delta-only** for changed incrementals | Only new delta bytes written |
| **3. Promoting Staging → Prod (`dbt-virtual promote`)** | Swaps `CREATE OR REPLACE VIEW analytics.<model> AS SELECT * FROM analytics_physical.<model>__<fp>` | **$0.00** (eliminates the entire second production `dbt build`!) | **$0.00** |
| **4. Querying Virtual Views (`SELECT * FROM analytics.<model> WHERE dt = ...`)** | BigQuery & Snowflake push partition and cluster predicates directly through `SELECT *` views | **0% overhead** (exact same bytes scanned as querying the physical table directly) | N/A |

---

## 2. Step-by-Step Migration of an Existing Project

You can migrate your existing dbt project either **gradually (one folder or model at a time)** or **all at once**.

### Step 1: Add `dbt-virtual` to `packages.yml` & `dbt_project.yml`

1. Add `dbt-virtual` to `packages.yml` and run `dbt deps`:
   ```yaml
   packages:
     - git: "https://github.com/Flip-to/dbt-virtual.git"
       revision: main
   ```

2. Add the `vars.dbt_virtual` block to `dbt_project.yml` and opt in your target model folder(s) with `+virtual_env: true`:
   ```yaml
   vars:
     dbt_virtual:
       enabled: true
       physical_schema: "analytics_physical"
       environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"
       base_environment: "prod"
       ref_resolution: "env_view"
       env_schemas:
         staging: "analytics_staging"
         prod: "analytics"
       retention_releases: 5
       retention_days: 7

   on-run-start:
     # Clone the base environment's table for changed incremental models BEFORE dbt materializes them.
     - "{{ dbt_virtual.seed_incremental_clones() }}"

   models:
     my_existing_project:
       # Start with one folder (e.g. metrics) or enable across your marts:
       metrics:
         +virtual_env: true
         +post-hook:
           - "{{ dbt_virtual.sync_env_view() }}"
   ```

   If production publishes to several datasets, use the mapping form of `env_schemas` instead (see [Section 3](#3-projects-with-several-datasets-and-their-own-routine-materializations)).

3. Add the routing overrides in `macros/virtual_routing_overrides.sql` (if you already have a custom `generate_schema_name`, keep its body and only add the `is_virtual_node` guard at the top):
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

---

### Step 2: Adopt Existing Production Tables in Zero Compute (`dbt-virtual adopt`)

Instead of rebuilding your existing huge tables in `analytics`, run **`dbt-virtual adopt`** against `prod`. It compiles the project with your default target, computes fingerprints, and handles every virtual `table` / `incremental` model; routines are not adopted (the first `build` + `promote` registers them). Fingerprints no longer depend on the target that compiled the project, so the tables adopted here are `REUSE` in later staging and PR builds; if your SQL hard-codes per-target schema names outside `ref()`, pass them with `--known-schema` (see [How It Works](how_it_works.md)).

=== "Option A: Full Cutover (`dbt-virtual adopt`)"

    Clones every opted-in table from `analytics.<model>` into `analytics_physical.<model>__<fp>`, replaces `analytics.<model>` with a virtual view pointing to that clone, and registers the fingerprint in `_environments` and `_releases`:

    ```bash
    dbt-virtual adopt --env prod --release v_initial_adopt
    ```

    **What SQL does this run for each model (e.g. `huge_events`)?**
    ```sql
    -- 1. Zero-copy CLONE existing production table into analytics_physical (0 compute, 0 extra storage!)
    CREATE TABLE IF NOT EXISTS `my_proj`.`analytics_physical`.`huge_events__58cce796d2a1`
    CLONE `my_proj`.`analytics`.`huge_events`;

    -- 2. Drop the legacy table in analytics and immediately replace with the virtual view
    DROP TABLE IF EXISTS `my_proj`.`analytics`.`huge_events`;
    CREATE OR REPLACE VIEW `my_proj`.`analytics`.`huge_events` AS
    SELECT * FROM `my_proj`.`analytics_physical`.`huge_events__58cce796d2a1`;

    -- 3. Record active pointer in analytics_physical._environments & _releases
    MERGE `my_proj`.`analytics_physical`._environments ...
    ```

=== "Option B: Two-Phase Safe Cutover (`--keep-existing-table`)"

    If you want to populate `analytics_physical` and `_environments` **first** without touching your existing `analytics.<model>` production tables until your first `promote`:

    ```bash
    # Phase 1: Zero-copy CLONE prod tables into analytics_physical and register in _environments,
    # leaving analytics.<model> untouched as a physical table for now:
    dbt-virtual adopt --env prod --release v_initial_adopt --keep-existing-table
    ```

    Then run your normal staging build (`dbt-virtual build --env staging --release v1`).
    When you run `dbt-virtual promote --from staging --to prod --release v1`, `virtual_promote` automatically detects that `analytics.<model>` is still a legacy physical `TABLE`, drops it, and creates the `VIEW` pointing to `analytics_physical.<model>__<fp>`!

---

### Step 3: Bootstrap `staging` from `prod` in Zero Compute

Once `prod` is adopted, initialize `staging` so all staging views point to the adopted physical tables:

```bash
dbt-virtual init-env --env staging --from prod
```

Now verify with `dbt-virtual plan`:
```bash
dbt-virtual status --env staging --base-env prod --output state.json
dbt-virtual plan --env staging --state-file state.json
```

Every unchanged `table` / `incremental` model should show **`REUSE` (0 compute)**. Routines show `ROUTINE_DEPLOY` until the first staging build registers them. A model that shows `FULL_BUILD` although its SQL did not change usually reads a per-target schema that is not a `ref()`; see `--known-schema`.

---

## 3. Projects with Several Datasets and Their Own Routine Materializations

A common BigQuery layout: the prod target writes marts to `analytics` and `analytics_gold` (custom schema `gold`), and table functions that applications call to a `spacetime` dataset, often through the project's own `table_function` materialization; dev and staging targets put everything in one dataset. dbt-virtual can keep that layout as is.

### Step A: Describe each environment's datasets in `env_schemas`

```yaml
vars:
  dbt_virtual:
    physical_schema: "analytics_physical"
    # Every production target builds into the `prod` environment.
    environment: "{{ 'prod' if target.name in ['prod', 'prod_scheduler'] else env_var('DBT_VIRTUAL_ENV', target.name) }}"
    base_environment: "prod"
    env_schemas:
      prod:
        default: "analytics"        # models without config(schema=...)
        spacetime: "spacetime"      # listed: the dataset applications already query
        # gold is not listed, so config(schema='gold') -> "analytics_gold"
      staging: "analytics_staging"  # one dataset for everything
```

Write the mapping so it produces the same datasets your `generate_schema_name` produces for the prod target today; consumers keep querying `analytics_gold.orders` and `spacetime.tvf_bookings`. `dbt-virtual adopt --env prod` then clones `analytics_gold.orders` (not `analytics.orders`) for a model with `config(schema='gold')`. PR environments still get one dataset each (`analytics_pr_42`). Full rules: [`env_schemas`](configuration_reference.md).

### Step B: Keep your routine materializations, add one call

If your project defines its own `table_function` (or `function`, `procedure`) materialization, keep it and call `dbt_virtual.register_routine_deployment(model, target_relation, 'table_function', sql, ddl=<the statement you ran>)` right after the `CREATE` statement. The call records the routine and its DDL so `promote` and `rollback` can replay it in another environment with every reference to a virtual object re-pointed (`analytics_staging.orders` → `analytics_gold.orders`). It is a no-op for non-virtual models. Full example and argument table: [Routines → Your Own Routine Materializations](routines_and_tvf_testing.md).

Two details decide which materialization dbt runs:
- Declare yours with `adapter='bigquery'`. The package ships `table_function, adapter='bigquery'`, and dbt prefers an adapter-specific materialization over a `default` one before it looks at which project defines it.
- On dbt-core 1.11+ a package can no longer override the built-in `function` materialization, but the root project can. A root-project `function` declared with `adapter='bigquery'` is chosen over both the package's and dbt-core's (dbt-core 1.12.6 `dbt/contracts/graph/manifest.py`, `find_materialization_macro_by_name`); a `default` one only beats dbt-core's `function, default` if your adapter ships no `function` of its own (not verified for dbt-bigquery). Models that used the package's `function` switch to `materialized='virtual_function'`.

### Step C: Adopt, build, promote

```bash
# 1. Adopt prod tables in place (keep them as tables until the first promote)
dbt-virtual adopt --env prod --release v_initial_adopt --keep-existing-table

# 2. Build staging: tables are REUSE, routines are deployed into analytics_staging and registered
dbt-virtual status --env staging --base-env prod --output state.json
dbt-virtual build --env staging --state-file state.json --release v1

# 3. Promote: views into analytics / analytics_gold, routines into spacetime with re-pointed references
dbt-virtual promote --from staging --to prod --release v1 --audit
```

A virtual routine that reads a **non-virtual** model in a custom schema keeps the location it had in staging after promotion (only references to virtual objects, and to the environment's default schema, are re-pointed). Make the models your routines read virtual, or read them through sources. `view` models with `virtual_env: true` are not promoted (no registry pointer is recorded for them); see [How It Works → Known Limitations](how_it_works.md).

---

## 4. Frequently Asked Questions (FAQ) for Existing Projects

### Q1: What if we only want to virtualize our 10 largest incremental tables first?
Because `dbt-virtual` is **opt-in** via `virtual_env: true`, you can enable it on just a single model (`{{ config(virtual_env=true) }}`) or a single subdirectory in `dbt_project.yml`. Non-virtualized models continue to build in place in `analytics_staging` / `analytics` and can freely `{{ ref() }}` virtualized models (and vice versa) because virtualized models expose `<env_schema>.<model>` views at the exact same relation path as before!

### Q2: Does BigQuery charge double storage when `CLONE` is used?
**No.** In BigQuery (and Snowflake), `CREATE TABLE ... CLONE ...` only references the base table's storage blocks at creation time. You are only billed for **new or modified data blocks** written after the clone is created. And once our Janitor GC (`dbt-virtual gc --retention-releases 5 --retention-days 7`) drops superseded table versions past your retention window, storage footprint stays clean.

### Q3: Does querying through the virtual view (`analytics.<model>`) hurt BigQuery partition pruning?
**No.** A `SELECT * FROM analytics_physical.<model>__<fp>` view is completely transparent to the BigQuery and Snowflake query optimizers. Filtering `WHERE event_date = '2026-10-27'` on the virtual view `analytics.huge_events` prunes partitions identically to querying `analytics_physical.huge_events__58cce796d2a1` directly.
