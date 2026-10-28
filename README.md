# `dbt-virtual`

**SQLMesh-style Virtual Data Environments for dbt** (first-class **BigQuery** + **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks**) — `sqlglot` AST Merkle-DAG fingerprinting (`<model>__<fp>`), zero-copy `CLONE` incremental seeding with post-clone column migration, per-environment routine virtualization (`table_function`, `function`, `procedure`), pre-promotion quality gates (`audit`), concurrency lease locks (`_locks`), interval tracking (`_intervals`), zero-rebuild view-swap promotion (with partial promotion & automatic demotion), and instant rollback.

[![CI](https://github.com/Flip-to/dbt-virtual/actions/workflows/ci.yml/badge.svg)](https://github.com/Flip-to/dbt-virtual/actions/workflows/ci.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub_Pages-indigo?logo=materialformkdocs)](https://flip-to.github.io/dbt-virtual/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

> **📖 Read the Full Documentation**: **[https://flip-to.github.io/dbt-virtual/](https://flip-to.github.io/dbt-virtual/)**
> - [Step-by-Step Getting Started Guide](docs/guides/getting_started.md)
> - [Complete Configuration & Parameters Reference](docs/guides/configuration_reference.md)
> - [How It Works & Ref Resolution Modes (`env_view` vs `physical` vs `hybrid`)](docs/guides/how_it_works.md)
> - [Routines (`table_function`, `function`, `procedure`) & `tvf_test_ref`](docs/guides/routines_and_tvf_testing.md)
> - [CLI & Lifecycle Operations Reference](docs/guides/cli_and_operations.md)
> - [CI/CD & Production Workflows](docs/guides/ci_cd_workflows.md)
> - [Multi-Warehouse Support Matrix (BigQuery, Snowflake, DuckDB, Postgres, Databricks)](docs/guides/warehouse_support.md)

---

## Why `dbt-virtual`?

In standard dbt deployments:
1. Promoting `staging` → `prod` requires **rebuilding every table a second time** in production—doubling warehouse compute costs and risking data drift between the staging run and production run.
2. Rolling back a bad production release requires slow table restores, while **BigQuery & Snowflake routines (`TABLE FUNCTION`, `FUNCTION`, `PROCEDURE`) have no time travel at all**.
3. Migrating an existing mature dbt codebase to SQLMesh often breaks dbt-native ecosystems: custom Jinja materializations (`table_function`, `function`, `procedure`), package manifest hooks, `dbt-checkpoint`, the `sqlfluff` dbt templater, Elementary, and Slim CI.

**`dbt-virtual`** brings Virtual Data Environments directly into dbt as a hybrid dbt macro package + lightweight CLI.

| Capability | Standard dbt | `dbt-virtual` |
|---|---|---|
| **Supported Warehouses** | Adapter-specific | **BigQuery**, **Snowflake**, **DuckDB**, **PostgreSQL**, **Databricks** (all verified in CI via emulators/engines) |
| **Physical Storage** | Mutable in-place table (`prod.orders`) | Content-addressed (`analytics_physical.orders__8f3a9c1b2d4e`) |
| **Environment Isolation** | Separate physical rebuilds per target | Lightweight per-environment `VIEW` layers (`analytics_staging.orders`, `analytics.orders`) |
| **Modified Incrementals** | Full refresh or manual clone | Automatic zero-copy `CLONE` (BigQuery / Snowflake / Databricks) or CTAS seed (DuckDB / Postgres) from active `prod` table |
| **Promote `staging` → `prod`** | Full second `dbt build` in prod | **Zero rebuild** — metadata-only view swap + routine deploy + obsolete view demotion |
| **Rollback `prod`** | Manual revert PR + snapshot restore | **Instant rollback** — repoint views & redeploy previous release's routines |
| **TVFs / UDFs / Procedures** | Not supported natively | First-class `table_function`, `function`, `procedure` materializations + `tvf_test_ref` |

---

## Step-by-Step Quickstart

### Step 1: Install the dbt Package & CLI

Add `dbt-virtual` to your dbt project's `packages.yml`:
```yaml
packages:
  - git: "https://github.com/Flip-to/dbt-virtual.git"
    revision: main
```
Then run:
```bash
dbt deps
```

Install the Python CLI (for `dbt-virtual plan`, `build`, `diff`, `audit`, `promote`, `rollback`, `status`, `gc`):
```bash
uv pip install git+https://github.com/Flip-to/dbt-virtual.git
```

### Step 2: Configure `dbt_project.yml`

Add the `dbt_virtual` configuration block under `vars:` in `dbt_project.yml`, and opt in the model folders you want virtualized:

```yaml
vars:
  dbt_virtual:
    # Master switch: set to false to fall back to standard dbt behavior everywhere
    enabled: true

    # Shared schema/dataset where immutable physical tables (<model>__<fp>) and
    # registry tables (_environments, _releases, _locks, _intervals) are stored
    physical_schema: "analytics_physical"

    # Active virtual environment name (reads DBT_VIRTUAL_ENV, defaults to target.name)
    environment: "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"

    # Canonical production environment used as the source for zero-copy CLONE seeding
    base_environment: "prod"

    # How {{ ref('model') }} resolves:
    # - 'env_view' (Default, Option A): resolves to <env_schema>.<model> virtual view
    # - 'physical' (Option B): resolves directly to <physical_schema>.<model>__<fp>
    # - 'hybrid'   (Option C): TVFs/views resolve to <env_schema>.<model>; incrementals to physical
    ref_resolution: "env_view"

    # Mapping of logical environment names to warehouse schemas/datasets
    # (Any environment not listed here defaults to "analytics_<env>", e.g. pr_42 -> analytics_pr_42)
    env_schemas:
      staging: "analytics_staging"
      prod: "analytics"

    # Janitor GC safety window: keep physical tables from the last N releases AND last M days
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

### Step 3: Wire Schema & Alias Routing Macros

Create (or update) `macros/generate_schema_name.sql` and `macros/generate_alias_name.sql` in your dbt project so virtualized models automatically route their physical tables to `physical_schema` with `<model>__<fp>` aliases:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_schema_name(custom_schema_name, node)) }}
{%- endmacro %}

-- macros/generate_alias_name.sql
{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_alias_name(custom_alias_name, node)) }}
{%- endmacro %}
```

### Step 4: Run the Virtual Environment Lifecycle

```bash
# 1. Compile your dbt project so target/manifest.json is up to date
dbt compile --target staging

# 2. Plan & inspect Merkle-DAG fingerprints (classifies REUSE, SEED_INCREMENTAL, FULL_BUILD, ROUTINE_DEPLOY)
dbt-virtual plan --env staging

# 3. Build in staging (skips REUSE models in 0 compute; seeds new incremental fingerprints via CLONE from prod)
dbt-virtual build --env staging --release v2026.10.27

# 4. Check column-level schema compatibility (BREAKING vs TYPE_WIDENED vs NON_BREAKING)
dbt-virtual diff --base-manifest prod_manifest.json --fail-on-breaking

# 5. Run pre-promotion quality gate (verifies candidate tables are non-empty and within row-drop thresholds)
dbt-virtual audit --from staging --to prod --release v2026.10.27 --max-row-drop-pct 15.0

# 6. Zero-rebuild promote from staging -> prod (swaps views + deploys routines in tier order 1->2->3->4)
dbt-virtual promote --from staging --to prod --release v2026.10.27 --audit

# 7. Export live warehouse state (active environment pointers & physical tables)
dbt-virtual status --env staging --base-env prod

# 8. Instant rollback to previous release if needed
dbt-virtual rollback --env prod

# 9. Ephemeral PR environments (0-compute bootstrap from prod & teardown on PR close)
dbt-virtual init-env --env pr_123 --from prod
dbt-virtual drop-env --env pr_123

# 10. Janitor garbage collection of unreferenced physical tables
dbt-virtual gc --dry-run
```

---

## Configuration Parameters at a Glance

### Project-Level `vars.dbt_virtual` (`dbt_project.yml`)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `true` | Master toggle. When `false`, all macros fall back to standard dbt behavior. |
| `physical_schema` | `str` | `"analytics_physical"` | Dataset/schema storing physical fingerprint tables (`<model>__<fp>`) and registry metadata (`_environments`, `_releases`, `_locks`, `_intervals`). |
| `environment` | `str` | `target.name` | Current virtual environment being built (e.g., `staging`, `prod`, `pr_42`). Typically set via `"{{ env_var('DBT_VIRTUAL_ENV', target.name) }}"`. |
| `base_environment` | `str` | `"prod"` | Reference environment used by `clone_if_new_fp()` to seed new incremental table fingerprints via zero-copy `CLONE` and protected from `drop-env`. |
| `ref_resolution` | `str` | `"env_view"` | Controls how `ref()` resolves virtualized models: `"env_view"` (Option A, default), `"physical"` (Option B), `"hybrid"` (Option C), or `"explicit"` (Option D). |
| `env_schemas` | `dict[str, str]` | `{"staging": "analytics_staging", "prod": "analytics"}` | Maps logical environment names to their consumer-facing dataset/schema names. Any unmapped `<env>` defaults to `"analytics_<env>"`. |
| `retention_releases` | `int` | `5` | Number of most-recent releases per environment whose physical tables are protected from `dbt-virtual gc`. |
| `retention_days` | `int` | `7` | Minimum age in days before an unreferenced physical table is eligible for `dbt-virtual gc`. |

### Model-Level `config(...)` Parameters

| Config Key | Type | Default | Description |
|---|---|---|---|
| `virtual_env` | `bool` | `false` | Opts a model or routine into `dbt-virtual` content-addressed routing and virtual views. |
| `virtual_env_epoch` | `int` | `1` | Bump this integer to force a brand-new fingerprint and full rebuild of a model without changing its SQL. |
| `physical_version` | `str` | `None` | Pin a model's fingerprint to a specific string (e.g. `"58cce796d2a1"`) to reuse an existing physical table during forward-only changes. |
| `forward_only` | `bool` | `false` | Marks a model change as forward-only in fingerprint metadata. |
| `virtual_add_columns` | `list[str]` | `[]` | List of `<col_name> <DATA_TYPE>` clauses automatically added via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` right after cloning from `prod` in `clone_if_new_fp()`. |
| `materialized` | `str` | `"view"` | Supports standard dbt (`incremental`, `table`, `view`) plus routine materializations (`table_function`, `function`, `procedure`). |
| `parameters` | `str` | `""` | Routine signature parameters (e.g., `"min_orders INT64, region STRING"`) for `table_function`, `function`, or `procedure`. |
| `returns` | `str` | `""` | Return type (e.g., `"INT64"` or `"TABLE(customer_id STRING, orders INT64)"`) for scalar `function` or Snowflake/Postgres `table_function`. |

---

## Architecture, Specs, Decisions & Tech Debt

All written documentation follows [`docs/conventions/docs_and_specs.md`](docs/conventions/docs_and_specs.md) and is enforced in CI by [`scripts/check_docs_layout.py`](scripts/check_docs_layout.py):

### User Guides (`docs/guides/`)
- **[Step-by-Step Getting Started (`docs/guides/getting_started.md`)](docs/guides/getting_started.md)**
- **[Complete Configuration & Parameters Reference (`docs/guides/configuration_reference.md`)](docs/guides/configuration_reference.md)**
- **[How It Works & Ref Resolution Modes (`docs/guides/how_it_works.md`)](docs/guides/how_it_works.md)**
- **[Routines & TVF Unit Testing (`docs/guides/routines_and_tvf_testing.md`)](docs/guides/routines_and_tvf_testing.md)**
- **[CLI & Lifecycle Operations Reference (`docs/guides/cli_and_operations.md`)](docs/guides/cli_and_operations.md)**
- **[CI/CD & Production Workflows (`docs/guides/ci_cd_workflows.md`)](docs/guides/ci_cd_workflows.md)**
- **[Warehouse Support Matrix (`docs/guides/warehouse_support.md`)](docs/guides/warehouse_support.md)**

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

## License

Apache License 2.0 — see [LICENSE](LICENSE).
