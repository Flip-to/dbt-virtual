# Routines (`table_function`, `virtual_function`, `procedure`) & TVF Testing

One of the hardest problems in BigQuery and Snowflake data engineering is managing **database routines** (`TABLE FUNCTION`, scalar `FUNCTION`, and `PROCEDURE`) across `staging`, `prod`, and PR environments:
- Unlike tables, **routines have no time travel** (`FOR SYSTEM_TIME AS OF` does not work on routines).
- A routine in `analytics_staging` must call other routines and views in `analytics_staging`, while the promoted routine in `analytics` must call routines and views in `analytics` (or, with several datasets per environment, in `analytics_gold`, `spacetime`, ...).
- Routines have strict creation order dependencies: a `TABLE FUNCTION` that calls a scalar `FUNCTION` and queries a `VIEW` will fail to compile in `prod` if deployed before the `FUNCTION` and `VIEW` exist!

`dbt-virtual` handles all three: it records each routine's DDL as a template, re-points it at the target environment, and deploys in dependency order.

---

## 1. Routine Materializations

`dbt-virtual` ships three routine materializations in `macros/materializations/` (`function.sql`, `table_function.sql`, `procedure.sql`). Each runs the `CREATE OR REPLACE` statement and then calls `register_routine_deployment` for virtual nodes.

### Tier 1: Scalar SQL Function (`materialized='virtual_function'`)

Create `models/routines/udf_double_orders.sql`:
```sql
{{
  config(
    materialized='virtual_function',
    parameters='n INT64',
    returns='INT64',
    virtual_env=true
  )
}}
n * 2
```

When built in `staging`, `dbt-virtual` executes:
```sql
CREATE OR REPLACE FUNCTION `my_project`.`analytics_staging`.`udf_double_orders`(n INT64)
RETURNS INT64
AS (
n * 2
)
```

!!! warning "Use `virtual_function`, not `function`, on dbt-core 1.11+"
    dbt-core 1.11 added a built-in `function` materialization (for its `functions/` resource type), and dbt ignores a package's override of a built-in materialization (`require_explicit_package_overrides_for_builtin_materializations`). A model with `materialized='function'` then runs dbt-core's materialization and fails with `'NoneType' object has no attribute 'type'` (observed with dbt-core 1.12.6). The package still defines `function` for older dbt-core versions and for projects that re-export it from their root project; `virtual_function` does the same thing under a name dbt-core does not own.

### Tier 3: Table-Valued Function (`materialized='table_function'`)

Create `models/routines/tvf_orders_report.sql`:
```sql
{{
  config(
    materialized='table_function',
    parameters='min_orders INT64',
    virtual_env=true
  )
}}
SELECT
    customer_id,
    orders,
    {{ ref('udf_double_orders') }}(orders) AS doubled_orders
FROM {{ dbt_virtual.tvf_test_ref('orders_metrics') }}
WHERE orders >= min_orders
```

When built in `staging`, `dbt-virtual` executes:
```sql
CREATE OR REPLACE TABLE FUNCTION `my_project`.`analytics_staging`.`tvf_orders_report`(min_orders INT64)
AS (
  SELECT
      customer_id,
      orders,
      `my_project`.`analytics_staging`.`udf_double_orders`(orders) AS doubled_orders
  FROM `my_project`.`analytics_staging`.`orders_metrics`
  WHERE orders >= min_orders
)
```

On BigQuery the materialization first drops a **procedure** of the same name, if any, so a routine can change kind.

### Tier 4: Stored Procedure (`materialized='procedure'`)

Create `models/routines/sp_refresh_summary.sql`. On BigQuery the model body is placed right after the signature, so it must be a `BEGIN ... END` block:
```sql
{{
  config(
    materialized='procedure',
    parameters='run_date DATE',
    virtual_env=true
  )
}}
BEGIN
  SELECT run_date;
END
```

On Snowflake and PostgreSQL the body is wrapped in `$$ ... $$` with `LANGUAGE SQL`; DuckDB has no stored procedures, so the statement is a no-op (`SELECT 1`).

### Custom schemas
`config(schema='spacetime')` on a routine creates it in the environment's schema for `spacetime` (`spacetime` in prod, `analytics_staging` in a single-schema staging; see [`env_schemas`](configuration_reference.md)).

---

## 2. How Zero-Rebuild Routine Promotion Works

When `register_routine_deployment` runs during `dbt-virtual build --env staging --release v1`:
1. It takes the routine DDL that ran and replaces every reference to a **virtual object** in the staging environment (`analytics_staging.orders_metrics`, `analytics_staging.udf_double_orders`) with a placeholder that names that object's custom schema: `__DBT_VIRTUAL_ENV_SCHEMA_<schema_key>__`, or `__DBT_VIRTUAL_ENV_SCHEMA__` for objects without a custom schema. Other names qualified with staging's default schema get `__DBT_VIRTUAL_ENV_SCHEMA__` too. Only schema-qualified names (`analytics_staging.x`, `` `analytics_staging`.x ``) are replaced, not longer identifiers that merely contain the schema name (`raw_analytics_staging.events`, `analytics_staging_flag`).
2. It stores that `routine_ddl_template` in `analytics_physical._releases` with its `schema_key` and a `dependency_tier` (`1` function, `3` table function, `4` procedure; `2` is used for models), and points `_environments` at the routine.
3. At the end of the build, `virtual_snapshot_release` adds unchanged routines (and tables) of the environment to the release, with their latest template.

Placeholders are per **object**, not per schema, because a single-dataset staging holds objects that live in different datasets in production.

When you run `dbt-virtual promote --from staging --to prod --release v1`:
1. `virtual_promote` reads the release and deploys environment views first (routines may read them), then routines in DAG order: each routine after the virtual views and routines it reads, and at equal depth UDFs, then table functions, then procedures. A table function that calls a UDF therefore always finds it.
2. For each routine it replaces every placeholder with the target environment's schema for that custom schema (`analytics`, `analytics_gold`, `spacetime`, ...) and runs the DDL. Routines whose fingerprint is already active in `prod` are skipped.
3. On BigQuery it then looks the routine up in `<schema>.INFORMATION_SCHEMA.ROUTINES`. The result is not checked, so this lookup does not fail a promote by itself; a failing `CREATE` statement does. Other adapters skip the lookup.
4. `dbt-virtual rollback --env prod` re-executes the previous release's templates in the same order, which gives routines the **time travel** the warehouse does not have.

The stored `dependency_tier` is kept for reference; the deployment order comes from the resource type and the project's DAG at promote time.

References to **non-virtual** models in another schema are not re-pointed: the promoted routine keeps reading the location the building environment used. Make a routine's dependencies virtual (or sources).

---

## 3. Isolated Unit Testing for BigQuery TVFs (`tvf_test_ref`)

Testing complex BigQuery Table Functions (`TVF`) normally requires creating physical seed tables in BigQuery first.

BigQuery supports passing table-valued arguments to TVFs using the `ANY TABLE` parameter type and `TABLE <cte_name>` syntax. `dbt-virtual` provides the `tvf_test_ref()` macro so the **exact same TVF SQL** can run in production against your virtual view *and* in unit tests against zero-I/O synthetic CTE fixtures!

### How `tvf_test_ref` Works

In your TVF model (`models/routines/tvf_orders_report.sql`), use `dbt_virtual.tvf_test_ref('orders_metrics')` instead of `ref('orders_metrics')`:

```sql
SELECT
    customer_id,
    orders
FROM {{ dbt_virtual.tvf_test_ref('orders_metrics') }}
WHERE orders >= min_orders
```

- **Normal Mode (`tvf_test_fixtures=false`, default)**:
  `tvf_test_ref('orders_metrics')` calls `ref('orders_metrics')`. With the project's `ref()` override pointing at `dbt_virtual.virtual_ref` (see [Getting Started](getting_started.md)), that resolves to the environment view, `` `my_project`.`analytics_staging`.`orders_metrics` ``.
- **Fixture Test Mode (`tvf_test_fixtures=true`)**:
  `tvf_test_ref('orders_metrics')` emits the bare table parameter identifier `orders_metrics__fixture`. You can deploy a test TVF with signature `(orders_metrics__fixture ANY TABLE, min_orders INT64)` and test it with pure in-memory CTEs:

```sql
WITH synthetic_orders AS (
  SELECT 'customer_a' AS customer_id, 5 AS orders
  UNION ALL
  SELECT 'customer_b' AS customer_id, 50 AS orders
)
SELECT *
FROM `my_project.analytics_staging._test_tvf_orders_report`(
  TABLE synthetic_orders,
  10
)
-- Returns only 'customer_b' (50 orders), without reading any table.
```

---

## 4. Your Own Routine Materializations (`register_routine_deployment`)

Projects that already have their own routine materializations (minified bodies, `OPTIONS(...)`, extra grants, ...) do not have to switch to the package's. Keep yours and add one call after the `CREATE` statement:

```sql
dbt_virtual.register_routine_deployment(node, target_relation, routine_type, body_sql, ddl=none)
```

| Argument | Meaning |
|---|---|
| `node` | The model being built (`model` inside a materialization). The call is a no-op for nodes without `virtual_env: true`, so no guard is needed. |
| `target_relation` | The relation the routine was created at (`this`, or the relation you built). |
| `routine_type` | `'function'`, `'table_function'` or `'procedure'`. It sets the tier and the `DROP` statement used on demotion; any other value is treated as a procedure. |
| `body_sql` | The routine body. Used to generate the DDL template when `ddl` is not given. |
| `ddl` | The statement that actually ran. Pass it whenever it differs from what the package would generate; it is what promote and rollback replay. |

BigQuery example, wrapping a project's own `table_function` materialization:

```sql
{% materialization table_function, adapter='bigquery' %}
  {%- set target_relation = api.Relation.create(
    database=this.database, schema=this.schema, identifier=this.identifier, type='view'
  ) -%}

  {#- Your existing DDL, unchanged. -#}
  {%- set ddl -%}
CREATE OR REPLACE TABLE FUNCTION {{ target_relation }}({{ config.get('parameters', '') }})
AS (
{{ sql }}
)
  {%- endset -%}

  {{ run_hooks(pre_hooks) }}
  {% call statement('main') -%}
    {{ ddl }}
  {%- endcall %}

  {#- The one line dbt-virtual needs: record what ran so promote and rollback can replay it. -#}
  {%- do dbt_virtual.register_routine_deployment(model, target_relation, 'table_function', sql, ddl=ddl) -%}

  {{ run_hooks(post_hooks) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
```

Declare it with `adapter='bigquery'`. dbt chooses a materialization by adapter first and by project second (dbt-core 1.12.6, `MaterializationCandidate.__lt__` in `dbt/contracts/graph/manifest.py`): the package ships `table_function, adapter='bigquery'`, which beats a `default` one from your project; with `adapter='bigquery'` on both, your root project's wins.

---

## 5. Adapter Notes

- **DuckDB**: dbt-duckdb ships its own `table_function` materialization (`adapter='duckdb'`), which dbt picks over the package's `default` one, so DuckDB table functions are **not virtualized** (no pointer, no template, not promoted). `virtual_function` works and creates a DuckDB `MACRO` (`tests/test_dbt_duckdb_multi_schema.py`). Procedures are a no-op.
- **Snowflake / PostgreSQL**: the package's `table_function, default` materialization applies. Set `returns` (`TABLE(...)`); without it the DDL says `RETURNS TABLE()` on Snowflake and `RETURNS RECORD` on PostgreSQL.
- See the [Warehouse Support Matrix](warehouse_support.md) for the DDL per adapter.
