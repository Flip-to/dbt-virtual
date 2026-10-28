# `dbt-virtual` — Agent & Developer Guide

SQLMesh-style Virtual Data Environments for **dbt** (first-class **BigQuery** + **Snowflake**, **DuckDB**, **PostgreSQL**, and **Databricks**): `sqlglot` AST Merkle-DAG fingerprinting (`<model>__<fp>`), zero-copy `CLONE` incremental seeding with post-clone column migration, per-environment routine virtualization (`table_function`, `function`, `procedure`), pre-promotion quality gates (`audit`), concurrency lease locks (`_locks`), interval tracking (`_intervals`), zero-rebuild view-swap promotion (with partial promotion & automatic demotion), and instant rollback.

## Project Structure

```
dbt_project.yml               # dbt package metadata & default vars('dbt_virtual')
pyproject.toml                # Python package & CLI entry point (`dbt-virtual`)
mkdocs.yml                    # Material for MkDocs configuration (GitHub Pages)
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
  cli.py                      # `dbt-virtual` CLI (plan, build, diff, audit, promote, rollback, status, init-env, drop-env, gc)
scripts/
  check_docs_layout.py        # Offline CI guard for specs, decisions, tech_debt, beans, and links
  run_emulator_e2e.py         # End-to-end test harness against Flip-to/bigquery-emulator
  run_snowflake_emulator_e2e.py # End-to-end test harness against nnnkkk7/snowflake-emulator & PostgreSQL
tests/
  test_fingerprint.py         # Unit + hypothesis property-based fuzz tests for fingerprinting & SQL normalization
  test_cli_and_macros.py      # Unit tests for CLI subcommands, planner, and schema diffing
  test_macros_execution.py    # Jinja macro execution harness tests across all adapters
  test_duckdb_e2e.py          # In-process DuckDB virtual environment lifecycle E2E test
  test_dbt_duckdb_project_e2e.py # Real dbt-core + dbt-duckdb full CLI project integration test
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
uv run pytest -v                                              # Run all 26 unit, property-based, and dbt-duckdb E2E tests
uv run ruff check src tests scripts                           # Lint Python code
uv run python scripts/check_docs_layout.py                    # Check docs, tech debt, decisions & beans
uv run python scripts/check_docs_layout.py --write-index      # Regenerate README.md indexes in docs/
uv run python scripts/run_emulator_e2e.py                     # Run E2E lifecycle test on Flip-to/bigquery-emulator
uv run python scripts/run_snowflake_emulator_e2e.py           # Run E2E lifecycle test on nnnkkk7/snowflake-emulator & Postgres
uv run --with mkdocs-material mkdocs build --strict           # Build GitHub Pages documentation site locally
```

## Key Architectural Decisions & Conventions

- **Default `ref()` resolution is `env_view` (Option A)**: `{{ dbt_virtual.virtual_ref('A') }}` resolves virtualized tables to `<env_schema>.A` while physical writes go to `<physical_schema>.A__<fp>`. Modes `physical` (Option B), `hybrid` (Option C), and `explicit` (Option D) are also supported via `var('dbt_virtual').ref_resolution` (`docs/decisions/20261027_default_ref_resolution_env_view.md`, `DECISION(default_ref_resolution_env_view)`).
- **Hybrid Packaging**: Every lifecycle operation works both via the `dbt-virtual` Python CLI (`plan`, `build`, `diff`, `audit`, `promote`, `rollback`, `status`, `init-env`, `drop-env`, `gc`) and via `dbt run-operation` macros (`docs/decisions/20261027_hybrid_package_and_bigquery_first.md`).
- **Multi-Emulator CI**: We test end-to-end SQL execution against `Flip-to/bigquery-emulator`, `nnnkkk7/snowflake-emulator`, `postgres:16-alpine`, and real `dbt-core` + `dbt-duckdb` in `.github/workflows/ci.yml`.

## Agent Skills (`.claude/skills/`)

- **Setup & Zero-Compute Project Migration (`/dbt-virtual-setup`)**: `.claude/skills/dbt-virtual-setup/SKILL.md` — interactive workflow to wire `packages.yml`, `dbt_project.yml`, `generate_schema_name` / `generate_alias_name`, and run `dbt-virtual adopt` in zero compute (see `docs/guides/agent_skills.md`).
- **Day-to-Day Lifecycle Operations (`/dbt-virtual-ops`)**: `.claude/skills/dbt-virtual-ops/SKILL.md` — workflow for `status`, `plan`, `build`, `diff`, `audit`, `promote`, `rollback`, `init-env`, `drop-env`, and `gc`.
- **Record a decision (`/record-decision`)**: `.claude/skills/record-decision/SKILL.md`, copy `docs/templates/decision.md` to `docs/decisions/YYYYMMDD_<slug>.md`, optionally add `DECISION(<slug>)` in code, and run `uv run python scripts/check_docs_layout.py --write-index`.
- **Record or pay off tech debt (`/record-tech-debt`)**: `.claude/skills/record-tech-debt/SKILL.md`, copy `docs/templates/tech_debt.md` to `docs/tech_debt/<slug>.md`, link an open bean in `.beans/`, add `TECH_DEBT(<slug>)` at the code site, and run `uv run python scripts/check_docs_layout.py --write-index`.
- **Track beans & A2V (`/a2v-track`)**: Every PR has a bean in `.beans/` with an `## A2V track` table (`.claude/rules/beans-tracking.md`, `.claude/rules/a2v-track.md`, `.claude/skills/a2v-track/SKILL.md`).
