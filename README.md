# `dbt-virtual`

**SQLMesh-style Virtual Data Environments for dbt** — content-addressed physical tables (`<model>__<fp>`), zero-copy `CLONE` incremental seeding, per-environment BigQuery routine virtualization (`table_function`, `function`, `procedure`), zero-rebuild view-swap promotion, and instant rollback.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## Why `dbt-virtual`?

In standard dbt deployments:
1. Promoting `staging` → `prod` requires **rebuilding every table a second time** in production—doubling warehouse compute costs and risking data drift between the staging run and production run.
2. Rolling back a bad production release requires slow table restores, while **BigQuery routines (`TABLE FUNCTION`, `FUNCTION`, `PROCEDURE`) have no time travel at all**.
3. Migrating an existing mature dbt codebase to SQLMesh often breaks dbt-native ecosystems: custom Jinja materializations (`table_function`, `function`, `procedure`), `snowplow_unified` manifest hooks, `dbt-checkpoint`, the `sqlfluff` dbt templater, Elementary, and Slim CI.

**`dbt-virtual`** brings Virtual Data Environments directly into dbt as a hybrid dbt macro package + lightweight CLI.

| Capability | Standard dbt | `dbt-virtual` |
|---|---|---|
| **Physical Storage** | Mutable in-place table (`prod.orders`) | Content-addressed (`physical.orders__8f3a9c1b2d4e`) |
| **Environment Isolation** | Separate physical rebuilds per target | Lightweight per-environment `VIEW` layers (`staging.orders`, `prod.orders`) |
| **Modified Incrementals** | Full refresh or manual clone | Automatic zero-copy `CREATE TABLE ... CLONE` from active `prod` table |
| **Promote `staging` → `prod`** | Full second `dbt build` in prod | **Zero rebuild** — metadata-only view swap + routine deploy |
| **Rollback `prod`** | Manual revert PR + snapshot restore | **Instant rollback** — repoint views & redeploy previous release's routines |
| **BigQuery TVFs / UDFs / Procs** | Not supported natively | First-class `table_function`, `function`, `procedure` materializations + `tvf_test_ref` |

---

## Architecture & Specifications

Full engineering specifications live under [`docs/`](docs/SPEC.md):
- **[Master Architecture Spec (`docs/SPEC.md`)](docs/SPEC.md)** — Problem, goals, architecture diagram, and roadmap.
- **[SPEC 01: Merkle-DAG Fingerprinting & Canonical SQL Normalization](docs/specs/01_fingerprinting_and_merkle_dag.md)** — Target-invariant SQL normalization and topological Merkle hashing.
- **[SPEC 02: Routing, `ref()` Resolution Modes, and Zero-Copy `CLONE` Seeding](docs/specs/02_routing_ref_modes_and_clone_seeding.md)** — Default `env_view` (Option A) resolution, alternative `physical` / `hybrid` / `explicit` modes, and incremental cloning.
- **[SPEC 03: Routine Virtualization (`table_function`, `function`, `procedure`) & TVF Testing](docs/specs/03_routine_virtualization_and_tvf_testing.md)** — Atomic promotion/rollback of BigQuery TVFs, scalar UDFs, and stored procedures.
- **[SPEC 04: Lifecycle Operations, State Registry, Rollback & Janitor GC](docs/specs/04_lifecycle_promote_rollback_and_gc.md)** — `_environments` and `_releases` schemas, `plan`, `build`, `promote`, `rollback`, `diff`, and `gc`.
- **[SPEC 05: Multi-Warehouse Adapter Dispatch & Integration Guide](docs/specs/05_adapter_dispatch_and_flipto_integration.md)** — BigQuery, DuckDB, and Snowflake dispatch + Cloud Workflows integration.

---

## Quickstart

### 1. Install the dbt Package & CLI

Add to `packages.yml`:
```yaml
packages:
  - git: "https://github.com/Flip-to/dbt-virtual.git"
    revision: main
```

Install the Python CLI (optional, for `dbt-virtual plan/build/promote/rollback/gc`):
```bash
uv pip install git+https://github.com/Flip-to/dbt-virtual.git
```

### 2. Configure `dbt_project.yml`

```yaml
vars:
  dbt_virtual:
    enabled: true
    physical_schema: "spacetime_physical"
    environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"
    base_environment: "prod"
    ref_resolution: "env_view"   # Default Option A ('env_view'). Also supports 'physical', 'hybrid'
    env_schemas:
      staging: "spacetime_staging"
      prod: "spacetime"
    retention_releases: 5
    retention_days: 7

models:
  my_project:
    metrics:
      +virtual_env: true
      +pre-hook:
        - "{{ dbt_virtual.clone_if_new_fp() }}"
      +post-hook:
        - "{{ dbt_virtual.sync_env_view() }}"
    gold:
      procedures:
        +virtual_env: true
```

### 3. Wire Schema & Alias Routing in Your Project's Macros

In your project's `macros/generate_schema_name.sql` and `macros/generate_alias_name.sql`:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {# ... your existing schema routing ... #}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
{%- endmacro %}
```

### 4. Lifecycle Commands

```bash
# 1. Plan & inspect Merkle-DAG fingerprints
dbt-virtual plan --env staging

# 2. Build in staging (seeds new incremental fingerprints via CLONE from prod)
dbt-virtual build --env staging --release v2026.10.27

# 3. Zero-rebuild promote from staging -> prod (swaps views + deploys routines)
dbt-virtual promote --from staging --to prod --release v2026.10.27

# 4. Instant rollback to previous release on gate failure
dbt-virtual rollback --env prod

# 5. Janitor garbage collection of unreferenced physical tables
dbt-virtual gc --dry-run
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
