# Routines (`table_function`, `function`, `procedure`) & TVF Testing

One of the hardest problems in BigQuery and Snowflake data engineering is managing **database routines** (`TABLE FUNCTION`, scalar `FUNCTION`, and `PROCEDURE`) across `staging`, `prod`, and PR environments:
- Unlike tables, **routines have no time travel** (`FOR SYSTEM_TIME AS OF` does not work on routines).
- A routine in `analytics_staging` must call other routines and views in `analytics_staging`, while the promoted routine in `analytics` must call routines and views in `analytics`.
- Routines have strict creation order dependencies: a `TABLE FUNCTION` that calls a scalar `FUNCTION` and queries a `VIEW` will fail to compile in `prod` if deployed before the `FUNCTION` and `VIEW` exist!

`dbt-virtual` solves all three problems natively.

---

## 1. Routine Materializations

`dbt-virtual` ships three custom dbt materializations in `macros/materializations/routines.sql`:

### Tier 1: Scalar SQL Function (`materialized='function'`)

Create `models/routines/udf_double_orders.sql`:
```sql
{{
  config(
    materialized='function',
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
RETURNS INT64 AS (
  n * 2
)
```

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

### Tier 4: Stored Procedure (`materialized='procedure'`)

Create `models/routines/sp_refresh_summary.sql`:
```sql
{{
  config(
    materialized='procedure',
    parameters='run_date DATE',
    virtual_env=true
  )
}}
SELECT run_date
```

---

## 2. How Zero-Rebuild Routine Promotion Works

When `register_routine_deployment` runs during `dbt build --target staging`:
1. It takes the rendered routine DDL and replaces the staging schema name (`analytics_staging`) with the placeholder token `__DBT_VIRTUAL_ENV_SCHEMA__`.
2. It stores that `routine_ddl_template` in `analytics_physical._releases` along with its **`dependency_tier`**:
   - **Tier 1**: `function` (Scalar UDFs)
   - **Tier 2**: `model` (Virtual Environment Views → Physical Fingerprint Tables)
   - **Tier 3**: `table_function` (TVFs that query Tier 2 Views and call Tier 1 UDFs)
   - **Tier 4**: `procedure` (Stored Procedures that call Tier 1–3 objects)

When you run `dbt-virtual promote --from staging --to prod --release v1`:
1. `virtual_promote` queries `_releases` ordered by `dependency_tier ASC, model_name ASC`.
2. It substitutes `__DBT_VIRTUAL_ENV_SCHEMA__` with `analytics` (`prod`'s schema) and executes the DDL in strict tier order (`1 -> 2 -> 3 -> 4`).
3. It validates that every promoted routine exists in `analytics.INFORMATION_SCHEMA.ROUTINES`.
4. If you ever run `dbt-virtual rollback --env prod`, it re-executes the previous release's `routine_ddl_template` in `analytics`—giving you **instant time travel for BigQuery and Snowflake routines**!

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
  `tvf_test_ref('orders_metrics')` delegates directly to `dbt_virtual.virtual_ref('orders_metrics')`, resolving to `` `my_project`.`analytics_staging`.`orders_metrics` ``.
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
-- Returns only 'customer_b' (50 orders) in milliseconds with zero table I/O!
```
