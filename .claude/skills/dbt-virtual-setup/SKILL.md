---
name: dbt-virtual-setup
description: Set up or migrate an existing dbt project (BigQuery, Snowflake, DuckDB, PostgreSQL, Databricks) to use dbt-virtual in zero compute (`dbt-virtual adopt`), wire `packages.yml`, `dbt_project.yml`, and routing macros (`generate_schema_name`, `generate_alias_name`), and verify the setup.
user-invocable: true
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Set Up or Migrate a dbt Project to `dbt-virtual`

Canonical references (maintain DRY — always read these guides for full parameter tables and edge cases):
- Getting Started: `docs/guides/getting_started.md`
- Zero-Compute Existing Project Migration: `docs/guides/migrating_existing_project.md`
- Configuration & Parameters Reference: `docs/guides/configuration_reference.md`

## Workflow

1. **Inspect the Target dbt Project**:
   - Read `dbt_project.yml`, `packages.yml` (or `dependencies.yml`), and any existing `macros/generate_schema_name.sql` or `macros/generate_alias_name.sql`.
   - Identify the warehouse adapter (`bigquery`, `snowflake`, `duckdb`, `postgres`, `databricks`), the production schema name (e.g. `analytics`), and the staging schema name (e.g. `analytics_staging`).
   - List the datasets each target publishes to: every distinct `config(schema=...)` / `+schema:` value and where the project's `generate_schema_name` puts it for the prod target vs the dev/staging targets. Several prod datasets means the mapping form of `env_schemas` (step 3).
   - List the profile targets that are "production" (e.g. `prod`, `prod_scheduler`); they all need to map to the `prod` environment.
   - Look for the project's own routine materializations (`{% materialization table_function`, `function`, `procedure` in `macros/`) and for models with `materialized='function'`.
   - Ask the user which model folder(s) or specific models they want to opt into `+virtual_env: true` first (recommend starting with heavy `incremental` / `table` marts or routines).

2. **Install `dbt-virtual` Package & CLI**:
   - Add `dbt-virtual` to `packages.yml`:
     ```yaml
     packages:
       - git: "https://github.com/Flip-to/dbt-virtual.git"
         revision: main
     ```
   - Run `dbt deps` and install the CLI (`uv pip install git+https://github.com/Flip-to/dbt-virtual.git`).

3. **Configure `vars.dbt_virtual` and Model Hooks in `dbt_project.yml`**:
   - Add `vars.dbt_virtual` (`enabled`, `physical_schema`, `environment`, `base_environment`, `ref_resolution`, `env_schemas`, `retention_releases`, `retention_days`) following `docs/guides/configuration_reference.md`. Set every key: `vars` are not deep-merged, and omitted keys fall back to built-in defaults (`physical_schema: dbt_virtual_physical`, `env_schemas: {staging: staging, prod: prod}`).
   - `environment`: when several targets are production, map them with Jinja, e.g. `"{{ 'prod' if target.name in ['prod', 'prod_scheduler'] else env_var('DBT_VIRTUAL_ENV', target.name) }}"`.
   - `env_schemas`: a string per environment when it uses one schema; for prod with several datasets, a mapping `{default: analytics, <custom_schema>: <dataset>, ...}` that reproduces today's prod layout (an unlisted custom schema `c` becomes `<default>_c`). Unlisted environments (PRs) get `<prod default schema>_<env>`.
   - Add the top-level hook that seeds changed incremental models before dbt materializes them:
     ```yaml
     on-run-start:
       - "{{ dbt_virtual.seed_incremental_clones() }}"
     ```
   - On opted-in model folders, add:
     ```yaml
     +virtual_env: true
     +post-hook:
       - "{{ dbt_virtual.sync_env_view() }}"
     ```
   - Do not add the old `clone_if_new_fp()` pre-hook (it no longer clones; remove it if present).
   - Recommend `require_cli_fingerprints: true` once deployments go through `dbt-virtual build`: bare `dbt run` / `build` of virtual tables then fails instead of naming tables without upstream fingerprints.
   - Routines: on dbt-core 1.11+ switch `materialized='function'` models to `virtual_function`. If the project keeps its own routine materializations, add `dbt_virtual.register_routine_deployment(model, target_relation, '<function|table_function|procedure>', sql, ddl=<executed DDL>)` after the `CREATE` and declare them with `adapter='<adapter>'` (see `docs/guides/routines_and_tvf_testing.md` section 4 and `docs/guides/migrating_existing_project.md` section 3).
   - Tell the user that DuckDB table functions are not virtualized (dbt-duckdb's own `table_function` materialization wins).

4. **Wire Routing Overrides Without Breaking Non-Virtualized Models**:
   - In `macros/generate_schema_name.sql` and `macros/generate_alias_name.sql` (or `macros/virtual_routing_overrides.sql`), add the `if dbt_virtual.is_virtual_node(node)` guard at the top so non-virtualized models preserve their existing schema/alias behavior (see `docs/guides/getting_started.md` Step 3).
   - Add the `ref()` override (`{{ return(dbt_virtual.virtual_ref(*varargs, **kwargs)) }}`) so refs follow `ref_resolution`; without it, refs to virtual tables compile to the physical `<model>__<fp>` name.
   - Make sure `profiles.yml` has an output named after each environment the CLI builds (`dbt-virtual plan/build --env X` runs dbt with `--target X`).

5. **Zero-Compute Adoption of Existing Production Tables (If Migrating an Existing Project)**:
   - Follow `docs/guides/migrating_existing_project.md`:
     - For a **two-phase zero-risk cutover** (recommended when production tables are multi-TB):
       ```bash
       dbt-virtual adopt --env prod --release v_initial_adopt --keep-existing-table
       ```
     - For an **immediate cutover** (zero-copy `CLONE`s `prod` tables to `<physical_schema>.<model>__<fp>`, replaces `prod` tables with virtual views, and initializes `staging`):
       ```bash
       dbt-virtual adopt --env prod --release v_initial_adopt
       dbt-virtual init-env --env staging --from prod
       ```

   - `adopt` only handles `table` / `incremental` models; routines are registered by the first `dbt-virtual build` and moved to prod by `promote`.

6. **Verify**:
   - Run `dbt-virtual status --env staging --base-env prod --output state.json` and `dbt-virtual plan --env staging --state-file state.json` to confirm virtualized nodes resolve cleanly and unchanged adopted tables classify as `REUSE` (0 compute). Without `--state-file`, nothing is `REUSE`.
   - An unchanged model that shows `FULL_BUILD` usually reads a per-target schema outside `ref()` (hard-coded dataset, per-target source schema): pass it with `--known-schema` (see `docs/guides/how_it_works.md`).
