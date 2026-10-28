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
   - Add `vars.dbt_virtual` (`enabled`, `physical_schema`, `environment`, `base_environment`, `ref_resolution`, `env_schemas`, `retention_releases`, `retention_days`) following `docs/guides/configuration_reference.md`.
   - On opted-in model folders, add:
     ```yaml
     +virtual_env: true
     +pre-hook:
       - "{{ dbt_virtual.clone_if_new_fp() }}"
     +post-hook:
       - "{{ dbt_virtual.sync_env_view() }}"
     ```

4. **Wire Routing Overrides Without Breaking Non-Virtualized Models**:
   - In `macros/generate_schema_name.sql` and `macros/generate_alias_name.sql` (or `macros/virtual_routing_overrides.sql`), add the `if dbt_virtual.is_virtual_node(node)` guard at the top so non-virtualized models preserve their existing schema/alias behavior (see `docs/guides/getting_started.md` Step 3).

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

6. **Verify**:
   - Run `dbt compile` and `dbt-virtual plan --env staging` to confirm virtualized nodes resolve cleanly and unchanged adopted models classify as `REUSE` (0 compute).
