# `dbt-virtual` — Agent & Developer Guide

SQLMesh-style Virtual Data Environments for **dbt** (first-class **BigQuery** + **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks**): `sqlglot` AST Merkle-DAG fingerprinting (`<model>__<fp>`), zero-copy `CLONE` incremental seeding with post-clone column migration, per-environment routine virtualization (`table_function`, `function`, `procedure`), pre-promotion quality gates (`audit`), concurrency lease locks (`_locks`), interval tracking (`_intervals`), zero-rebuild view-swap promotion (with partial promotion & automatic demotion), and instant rollback.

## Project Structure

```
dbt_project.yml               # dbt package metadata & default vars('dbt_virtual')
pyproject.toml                # Python package & CLI entry point (`dbt-virtual`)
mkdocs.yml                    # Zensical configuration (GitHub Pages)
macros/
  adapters/dispatch.sql       # Multi-warehouse adapter.dispatch (bigquery__, snowflake__, databricks__, duckdb__, postgres__, default__)
  fingerprint/
    get_fingerprint.sql       # get_fingerprint() (with 3-level Merkle parent walk fallback), get_config(), is_virtual_node()
  lifecycle/
    hooks.sql                 # Pre/post hooks: clone_if_new_fp() (with virtual_add_columns), sync_env_view() (with _intervals), register_routine_deployment()
    operations.sql            # run-operation macros: virtual_promote, virtual_audit, virtual_rollback, virtual_acquire_lock, virtual_release_lock, virtual_export_state, virtual_init_env, virtual_drop_env, virtual_gc
  materializations/
    routines.sql              # table_function, function, and procedure materializations
  routing/
    virtual_routing.sql       # virtual_schema_name(), virtual_alias_name(), virtual_ref(), virtual_is_incremental()
  testing/
    tvf_test_ref.sql          # Fixture injection macro (`ANY TABLE`) for BigQuery TVFs
src/dbt_virtual/
  fingerprint.py              # Merkle-DAG fingerprinting, sqlglot AST canonicalizer, physical_version pinning & column extractor
  planner.py                  # Plan classification (REUSE, SEED_INCREMENTAL, FULL_BUILD, ROUTINE_DEPLOY) & column schema diffing (NON_BREAKING, TYPE_WIDENED, BREAKING)
  cli.py                      # `dbt-virtual` CLI (plan, build, run, diff, audit, promote, rollback, status, adopt, init-env, drop-env, gc)
scripts/
  check_docs_layout.py        # Offline CI guard for specs, decisions, tech_debt, beans, and links
  run_emulator_e2e.py         # End-to-end test harness against Flip-to/bigquery-emulator
  emulator_e2e_gap_steps.py   # Emulator E2E steps 20-25: data diff, promote by CLONE, authorize_env_views, view metadata, expire envs, shared-table guard
  run_snowflake_emulator_e2e.py # End-to-end test harness against nnnkkk7/snowflake-emulator & PostgreSQL
  run_bigquery_sandbox_e2e.py # Opt-in lifecycle E2E against REAL BigQuery in throwaway datasets (not in CI)
tests/
  test_fingerprint.py         # Unit + hypothesis property-based fuzz tests for fingerprinting & SQL normalization
  test_cli_and_macros.py      # Unit tests for CLI subcommands, planner, and schema diffing
  test_cli_run_and_views.py   # CLI `run` command and view planning
  test_macros_execution.py    # Jinja macro execution harness tests across all adapters
  test_dispatch_coverage.py   # Every dispatched macro has a `default__` implementation
  test_duckdb_e2e.py          # In-process DuckDB virtual environment lifecycle E2E test
  test_dbt_duckdb_project_e2e.py # Real dbt-core + dbt-duckdb full CLI project integration test
  test_dbt_duckdb_regressions.py # Real dbt-duckdb regressions (`Project` helper): seeding, partial releases, drop-env guard
  test_dbt_duckdb_multi_schema.py # Real dbt-duckdb: one schema per custom schema per environment
  test_dbt_duckdb_views_and_run.py # Real dbt-duckdb: virtual views and `dbt-virtual run`
  test_dbt_duckdb_adopt_objects.py # Real dbt-duckdb: adopt registers deployed views and routines
  test_dbt_duckdb_small_gaps.py # Real dbt-duckdb: `ref_resolution` validation, `status` without a registry
  test_dbt_parse_warehouses.py # `dbt parse` on BigQuery/Snowflake profiles with fake credentials (uv; skipped offline)
  test_dbt_bigquery_emulator_e2e.py # Real dbt-core + dbt-bigquery lifecycle against the BigQuery emulator (needs DBT_VIRTUAL_BQ_EMULATOR; skipped otherwise)
  bq_emulator_project.py      # Helper: throwaway dbt project + profile (`api_endpoint`) for the emulator tests
  test_sqlmesh_parity_fixes.py # Gaps found by comparing with SQLMesh (unregistered tables, DAG-depth deploy order, GC-ed rollback)
  test_require_cli_fingerprints.py # `require_cli_fingerprints` guard against bare dbt runs
  test_check_docs_layout.py   # Unit tests for docs/tech-debt/decision/bean layout guard
```

Written docs live in the repo-root `docs/` tree (`guides/`, `specs/`, `decisions/`, `findings/`,
`runbooks/`, `conventions/`, `tech_debt/`, `templates/`). Names are `snake_case`, specs carry a
`status` frontmatter, decisions are one file each with `Context` / `Decision` / `Why` /
`Alternatives` / `Impact` / `Who` (`DECISION(<slug>)` in code), and known debt is a
`docs/tech_debt/<slug>.md` file plus a `TECH_DEBT(<slug>)` comment in code. Standard and CI gate:
`docs/conventions/docs_and_specs.md`, `scripts/check_docs_layout.py`. Every claim in a doc, PR or
comment carries its evidence or is phrased as a question: `.claude/rules/evidence.md`. Every bean
carries an A2V track (`Asked`, `Built`, `Proven`, `Reviewed`, `Shipped`, `Verified`) with Who /
Why / Evidence: `.claude/rules/a2v-track.md`.

## Common Commands

Python dependencies and tests are managed with **uv**:

```bash
uv run pytest -q                                              # Run all unit, property-based and real-dbt tests (about 30 min; the real-dbt files dominate)
uv run pytest tests/test_dbt_parse_warehouses.py -v            # dbt parse on BigQuery/Snowflake (needs uv and network to install the adapters)
DBT_VIRTUAL_BQ_EMULATOR=http://127.0.0.1:9050 uv run --extra emulator pytest -n 4 tests/test_dbt_bigquery_emulator_e2e.py   # real dbt-bigquery on a running emulator (9 min on 4 workers, 2026-11-05)
uv run ruff check src tests scripts                           # Lint Python code
uv run python scripts/check_docs_layout.py                    # Check docs, tech debt, decisions & beans
uv run python scripts/check_docs_layout.py --write-index      # Regenerate README.md indexes in docs/
uv run python scripts/run_emulator_e2e.py                     # Run E2E lifecycle test on Flip-to/bigquery-emulator
uv run python scripts/run_snowflake_emulator_e2e.py           # Run E2E lifecycle test on nnnkkk7/snowflake-emulator & Postgres
uv run --with zensical zensical build --clean --strict        # Build GitHub Pages documentation site locally with Zensical
```

## Key Architectural Decisions & Conventions

- **Default `ref()` resolution is `env_view` (Option A)**: `{{ dbt_virtual.virtual_ref('A') }}` resolves virtualized tables to `<env_schema>.A` while physical writes go to `<physical_schema>.A__<fp>`. Modes `physical` (Option B), `hybrid` (Option C), and `explicit` (Option D) are also supported via `var('dbt_virtual').ref_resolution` (`docs/decisions/20261027_default_ref_resolution_env_view.md`, `DECISION(default_ref_resolution_env_view)`).
- **Hybrid Packaging**: Every lifecycle operation works both via the `dbt-virtual` Python CLI (`plan`, `build`, `run`, `diff`, `audit`, `promote`, `rollback`, `status`, `adopt`, `init-env`, `drop-env`, `gc`) and via `dbt run-operation` macros (`docs/decisions/20261027_hybrid_package_and_bigquery_first.md`).
- **CLI flags**: `plan` and `build` take `--target` (dbt profile target, default the `--env` name) and `--vars` (JSON extra dbt vars); `build` also takes `--defer` / `--state`; `promote`, `audit` and `rollback` take `--no-lock` / `--lock-owner` (lease lock in `_locks`). `run --env <env>` refreshes an environment's unchanged table/incremental models in place and refuses a changed fingerprint unless `--allow-changes`.
- **Environments, views and guards** (`docs/specs/20261105_multi_dataset_envs_views_run_and_guards.md`): `env_schemas.<env>` may map each custom schema to its own dataset (`schema_key` in the registry); virtual views are registered and promoted like routines; `ref_resolution` accepts `env_view`, `physical`, `hybrid` only (`explicit` is rejected); `require_cli_fingerprints` blocks bare `dbt run` of virtual tables; `adopt` registers deployed tables, views and routines.
- **Multi-Emulator CI**: We test end-to-end SQL execution against `Flip-to/bigquery-emulator`, `nnnkkk7/snowflake-emulator`, `postgres:16-alpine`, real `dbt-core` + `dbt-duckdb`, and real `dbt-core` + `dbt-bigquery` on the BigQuery emulator (`tests/test_dbt_bigquery_emulator_e2e.py`, gated on the emulator fixes in bean 0031) in `.github/workflows/ci.yml`; a `dbt parse` job runs the package against BigQuery and Snowflake profiles with fake credentials. `scripts/run_bigquery_sandbox_e2e.py` is an opt-in run against real BigQuery.

## Agent Skills (`.claude/skills/`)

- **Setup & Zero-Compute Project Migration (`/dbt-virtual-setup`)**: `.claude/skills/dbt-virtual-setup/SKILL.md` — interactive workflow to wire `packages.yml`, `dbt_project.yml`, `generate_schema_name` / `generate_alias_name`, and run `dbt-virtual adopt` in zero compute (see `docs/guides/agent_skills.md`).
- **Day-to-Day Lifecycle Operations (`/dbt-virtual-ops`)**: `.claude/skills/dbt-virtual-ops/SKILL.md` — workflow for `status`, `plan`, `build`, `diff`, `audit`, `promote`, `rollback`, `init-env`, `drop-env`, and `gc`.
- **Record a decision (`/record-decision`)**: `.claude/skills/record-decision/SKILL.md`, copy `docs/templates/decision.md` to `docs/decisions/YYYYMMDD_<slug>.md`, optionally add `DECISION(<slug>)` in code, and run `uv run python scripts/check_docs_layout.py --write-index`.
- **Record or pay off tech debt (`/record-tech-debt`)**: `.claude/skills/record-tech-debt/SKILL.md`, copy `docs/templates/tech_debt.md` to `docs/tech_debt/<slug>.md`, link an open bean in `.beans/`, add `TECH_DEBT(<slug>)` at the code site, and run `uv run python scripts/check_docs_layout.py --write-index`.
- **Track beans & A2V (`/a2v-track`)**: Every PR has a bean in `.beans/` with an `## A2V track` table (`.claude/rules/beans-tracking.md`, `.claude/rules/a2v-track.md`, `.claude/skills/a2v-track/SKILL.md`).
