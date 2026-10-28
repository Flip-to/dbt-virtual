# `dbt-virtual`

**SQLMesh-style Virtual Data Environments for dbt** — content-addressed physical tables (`<model>__<fp>`), zero-copy `CLONE` incremental seeding, per-environment BigQuery routine virtualization (`table_function`, `function`, `procedure`), zero-rebuild view-swap promotion, and instant rollback.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## Why `dbt-virtual`?

In standard dbt deployments:
1. Promoting `staging` → `prod` requires **rebuilding every table a second time** in production—doubling warehouse compute costs and risking data drift between the staging run and production run.
2. Rolling back a bad production release requires slow table restores, while **BigQuery routines (`TABLE FUNCTION`, `FUNCTION`, `PROCEDURE`) have no time travel at all**.
3. Migrating an existing mature dbt codebase to SQLMesh often breaks dbt-native ecosystems: custom Jinja materializations (`table_function`, `function`, `procedure`), package manifest hooks, `dbt-checkpoint`, the `sqlfluff` dbt templater, Elementary, and Slim CI.

**`dbt-virtual`** brings Virtual Data Environments directly into dbt as a hybrid dbt macro package + lightweight CLI.

| Capability | Standard dbt | `dbt-virtual` |
|---|---|---|
| **Supported Warehouses** | Adapter-specific | **BigQuery**, **Snowflake**, **DuckDB**, **PostgreSQL**, **Databricks** (all verified in CI via emulators/engines) |
| **Physical Storage** | Mutable in-place table (`prod.orders`) | Content-addressed (`analytics_physical.orders__8f3a9c1b2d4e`) |
| **Environment Isolation** | Separate physical rebuilds per target | Lightweight per-environment `VIEW` layers (`analytics_staging.orders`, `analytics.orders`) |
| **Modified Incrementals** | Full refresh or manual clone | Automatic zero-copy `CLONE` (BigQuery / Snowflake / Databricks) or CTAS seed (DuckDB / Postgres) from active `prod` table |
| **Promote `staging` → `prod`** | Full second `dbt build` in prod | **Zero rebuild** — metadata-only view swap + routine deploy |
| **Rollback `prod`** | Manual revert PR + snapshot restore | **Instant rollback** — repoint views & redeploy previous release's routines |
| **TVFs / UDFs / Procedures** | Not supported natively | First-class `table_function`, `function`, `procedure` materializations + `tvf_test_ref` |

---

## Architecture, Specs, Decisions & Tech Debt

All written documentation follows [`docs/conventions/docs_and_specs.md`](docs/conventions/docs_and_specs.md) and is enforced in CI by [`scripts/check_docs_layout.py`](scripts/check_docs_layout.py):

### Specifications (`docs/specs/`)
- **[Spec Index (`docs/specs/README.md`)](docs/specs/README.md)**
- **[`20261027_dbt_virtual_architecture.md`](docs/specs/20261027_dbt_virtual_architecture.md)** — Master architecture, problem statement, alternatives, rollout, and verification.
- **[`20261027_merkle_dag_fingerprinting.md`](docs/specs/20261027_merkle_dag_fingerprinting.md)** — Target-invariant SQL normalization and topological Merkle hashing.
- **[`20261027_routing_ref_modes_and_clone_seeding.md`](docs/specs/20261027_routing_ref_modes_and_clone_seeding.md)** — Default `env_view` (Option A) resolution, alternative `physical` / `hybrid` / `explicit` modes, and incremental cloning.
- **[`20261027_routine_virtualization_and_tvf_testing.md`](docs/specs/20261027_routine_virtualization_and_tvf_testing.md)** — Atomic promotion/rollback of BigQuery TVFs, scalar UDFs, and stored procedures.
- **[`20261027_lifecycle_promote_rollback_and_gc.md`](docs/specs/20261027_lifecycle_promote_rollback_and_gc.md)** — `_environments` and `_releases` schemas, `plan`, `build`, `promote`, `rollback`, `diff`, and `gc`.
- **[`20261027_adapter_dispatch_and_emulator_ci.md`](docs/specs/20261027_adapter_dispatch_and_emulator_ci.md)** — Multi-warehouse adapter dispatch & BigQuery emulator CI integration.

### Decisions & Tech Debt
- **[Decision Index (`docs/decisions/README.md`)](docs/decisions/README.md)** — Architectural decision records (`DECISION(<slug>)`).
- **[Tech Debt Index (`docs/tech_debt/README.md`)](docs/tech_debt/README.md)** — Tracked debt items (`TECH_DEBT(<slug>)`) linked to `.beans/`.
- **[Developer & Agent Guide (`CLAUDE.md` / `AGENTS.md`)](CLAUDE.md)** — Rules, skills, A2V track, and local commands.

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
# 1. Plan & inspect Merkle-DAG fingerprints (classifies REUSE, SEED_INCREMENTAL, FULL_BUILD, ROUTINE_DEPLOY)
dbt-virtual plan --env staging

# 2. Build in staging (skips REUSE models in 0 compute; seeds new incremental fingerprints via CLONE from prod)
dbt-virtual build --env staging --release v2026.10.27

# 3. Check column-level schema compatibility (BREAKING vs NON_BREAKING) before promotion
dbt-virtual diff --base-manifest prod_manifest.json --fail-on-breaking

# 4. Run pre-promotion quality gate (verifies candidate tables are non-empty and within row-drop thresholds)
dbt-virtual audit --from staging --to prod --release v2026.10.27 --max-row-drop-pct 15.0

# 5. Zero-rebuild promote from staging -> prod (swaps views + deploys routines in tier order 1->2->3->4)
dbt-virtual promote --from staging --to prod --release v2026.10.27 --audit

# 6. Export live warehouse state (active environment pointers & physical tables)
dbt-virtual status --env staging --base-env prod

# 7. Instant rollback to previous release on gate failure
dbt-virtual rollback --env prod

# 8. Ephemeral PR environments (0-compute bootstrap from prod & teardown on PR close)
dbt-virtual init-env --env pr_123 --from prod
dbt-virtual drop-env --env pr_123

# 9. Janitor garbage collection of unreferenced physical tables
dbt-virtual gc --dry-run
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
