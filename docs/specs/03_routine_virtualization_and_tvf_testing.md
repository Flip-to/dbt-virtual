# SPEC 03: Routine Virtualization (`table_function`, `function`, `procedure`) & TVF Testing

- **Status**: Draft v1.0
- **Component**: `macros/materializations/` & `macros/testing/tvf_test_ref.sql`

---

## 1. The Routine Problem in Virtual Data Environments

Standard dbt and SQLMesh virtual environments assume every node can be represented as a physical table behind a `SELECT *` view. In production BigQuery architectures (such as `Flip-to/flipto-dbt`), the serving layer consists of:
1. **Parameterized Table Functions (TVFs)**: `CREATE OR REPLACE TABLE FUNCTION <schema>.ft_report_*(...) AS (SELECT ...)` — inlined by the BigQuery query optimizer into a single query job with zero `CALL` orchestration overhead.
2. **Scalar SQL UDFs**: `CREATE OR REPLACE FUNCTION <schema>.udf_*(...) RETURNS <type> AS (<expr>)` — used to factor large routing `CASE` expressions out of TVFs so each routine stays well under BigQuery's 32 KB routine body limit.
3. **Stored Procedures**: `CREATE OR REPLACE PROCEDURE <schema>.<name>(...) BEGIN ... END`.

Because BigQuery routines **cannot be aliased by a `VIEW`** and **bind their referenced dataset names at creation time**, a virtual environment system must treat routines as first-class citizens.

---

## 2. Custom Materializations Included in `dbt-virtual`

### 2.1 `table_function` Materialization (`macros/materializations/table_function.sql`)

```sql
{{
  config(
    materialized='table_function',
    virtual_env=true,
    parameters='ds_start_date DATE, ds_end_date DATE, ds_granularity STRING',
    returns='TABLE<granular_date DATE, sessions INT64>'  -- optional; inferred if omitted
  )
}}
SELECT ...
FROM {{ tvf_test_ref('traffic_metrics_root') }}
WHERE granular_date BETWEEN ds_start_date AND ds_end_date
```

#### Key Behaviors:
1. **Environment-Scoped Target Relation**:
   - Deploys to `<env_schema>.<identifier>` (e.g., `spacetime_staging.ft_report_v9_traffic_metrics` in `staging`, `spacetime.ft_report_v9_traffic_metrics` in `prod`).
2. **Automatic Procedure → TVF Transition Handling**:
   - BigQuery throws `"Cannot replace routine of type PROCEDURE with a different type TABLE FUNCTION"` if a stored procedure previously occupied the same identifier.
   - `table_function` wraps a pre-step in an idempotent `BEGIN DROP PROCEDURE IF EXISTS {{ target_relation }}; EXCEPTION WHEN ERROR THEN END` block so migrations from stored procedures to TVFs succeed cleanly in every environment.
3. **Optional `RETURNS TABLE<...>` Contract**:
   - If `config.get('returns')` is provided, emits `RETURNS {{ config.get('returns') }}`; otherwise lets BigQuery infer the output table schema from the query body.

---

### 2.2 `function` Materialization (`macros/materializations/function.sql`)

For deterministic scalar SQL UDFs:

```sql
{{
  config(
    materialized='function',
    virtual_env=true,
    parameters='gl STRING, sentinel STRING',
    returns='ARRAY<STRING>'
  )
}}
CASE gl
  WHEN 'property' THEN [ COALESCE(property_uuid, sentinel) ]
  ELSE []
END
```

- Model body is a single scalar SQL expression (no `SELECT` wrapper, no trailing `;`).
- Deploys to `<env_schema>.<identifier>`.
- When a TVF in `<env_schema>` calls a sibling scalar UDF, it resolves via `{{ ref('udf_name') }}` to `<env_schema>.<udf_name>`.

---

### 2.3 `procedure` Materialization (`macros/materializations/procedure.sql`)

For BigQuery stored procedures:

```sql
{{
  config(
    materialized='procedure',
    virtual_env=true,
    parameters='run_date DATE'
  )
}}
BEGIN
  -- procedure statements
END
```

---

## 3. Atomic Routine Promotion & Rollback Without Recompilation

How does `dbt-virtual promote --from staging --to prod` or `dbt-virtual rollback --env prod` deploy or revert TVFs and UDFs in `prod` without running `dbt compile` or `dbt run` again?

### 3.1 Environment-Tokenized DDL Capture (`routine_ddl_template`)
When a routine is built in `staging` (`env_schema = spacetime_staging`):
1. The materialization compiles the full `CREATE OR REPLACE [TABLE] FUNCTION / PROCEDURE` DDL.
2. It replaces occurrences of `spacetime_staging` (the source environment's schema) with the canonical token `__DBT_VIRTUAL_ENV_SCHEMA__`.
3. It stores this tokenized DDL in `<physical_schema>._releases` (`routine_ddl_template` column) alongside the routine's fingerprint and dependency order (`udf` before `table_function` before `procedure`).

### 3.2 Zero-Build Promotion (`staging` → `prod`)
When promoting release `R` to `prod` (`env_schema = spacetime`):
1. **Step 1 (Scalar UDFs)**: For every `function` in release `R`, substitute `__DBT_VIRTUAL_ENV_SCHEMA__` → `spacetime` and execute the DDL.
2. **Step 2 (Root View Swaps)**: For every virtualized `model` in release `R`, execute `CREATE OR REPLACE VIEW spacetime.<model> AS SELECT * FROM spacetime_physical.<model>__<fp>`.
3. **Step 3 (TVFs & Procedures)**: For every `table_function` and `procedure` in release `R`, substitute `__DBT_VIRTUAL_ENV_SCHEMA__` → `spacetime` and execute the DDL.
4. **Step 4 (Registry Update)**: Update `<physical_schema>._environments` for `environment = 'prod'`.

### 3.3 Instant Routine + Data Rollback (`prod` → `prev_release`)
When rolling back `prod` to `prev_release`:
- Executes the exact same 4 steps using `prev_release`'s stored fingerprints and `routine_ddl_template`s!
- Because both the views (`spacetime.<root>`) and the routines (`spacetime.ft_report_*`, `spacetime.udf_*`) revert together, a TVF **never** reads a root table whose schema or column set it does not match.

---

## 4. TVF Fixture Injection Macro (`tvf_test_ref`)

dbt's native `unit_tests` cannot test BigQuery `TABLE FUNCTION` models because dbt `unit_tests` mock `ref()` as CTEs in a bare `SELECT` and cannot pass TVF arguments (`DS_*`).

`dbt-virtual` provides `{{ tvf_test_ref('model_name') }}`:
- **Normal / Prod / Staging Mode (`var('tvf_test_fixtures', false) == false`)**:
  Returns `{{ ref('model_name') }}` (byte-identical to standard `ref()`).
- **Fixture Test Mode (`--vars '{tvf_test_fixtures: true}'`)**:
  Emits the bare identifier `model_name__fixture`, allowing a test runner to wrap the compiled query in:
  ```sql
  CREATE OR REPLACE TABLE FUNCTION __IT_DATASET__._test_my_tvf(
    model_name__fixture ANY TABLE,
    ds_start_date DATE,
    ds_end_date DATE
  ) AS (
    <compiled_body>
  );
  ```
  and test combinatorial TVF logic against synthetic `TEMP TABLE` fixtures using `EXCEPT DISTINCT` assertions.
