# `dbt-virtual` — Agent & Developer Guide

SQLMesh-style Virtual Data Environments for **dbt** on **BigQuery** (and adapter-dispatched warehouses): content-addressed physical tables (`<model>__<fp>`), zero-copy `CLONE` incremental seeding, per-environment routine virtualization (`table_function`, `function`, `procedure`), zero-rebuild view-swap promotion, and instant rollback.

## Project Structure

```
dbt_project.yml               # dbt package metadata & default vars('dbt_virtual')
pyproject.toml                # Python package & CLI entry point (`dbt-virtual`)
macros/
  adapters/dispatch.sql       # adapter.dispatch DDL (bigquery__, default__)
  fingerprint/                # get_fingerprint(), get_config(), is_virtual_node()
  lifecycle/
    hooks.sql                 # Pre/post hooks: clone_if_new_fp(), sync_env_view(), ensure_registry()
    operations.sql            # run-operation macros: virtual_promote, virtual_rollback, virtual_gc
  materializations/
    table_function.sql        # BigQuery TVF materialization (with procedure->TVF drop guard)
    function.sql              # BigQuery scalar SQL UDF materialization
    procedure.sql             # BigQuery stored procedure materialization
  routing/
    virtual_routing.sql       # virtual_schema_name(), virtual_alias_name(), virtual_ref()
  testing/
    tvf_test_ref.sql          # Fixture injection macro (`ANY TABLE`) for BigQuery TVFs
src/dbt_virtual/
  fingerprint.py              # Merkle-DAG fingerprinting & canonical SQL normalizer
  cli.py                      # `dbt-virtual` CLI (plan, build, promote, rollback, gc)
scripts/
  check_docs_layout.py        # Offline CI guard for specs, decisions, tech_debt, beans, and links
  run_emulator_e2e.py         # End-to-end test harness against Flip-to/bigquery-emulator
tests/
  test_fingerprint.py         # Unit tests for fingerprinting & SQL normalization
  test_check_docs_layout.py   # Unit tests for docs/tech-debt/decision/bean layout guard
```

Written docs live in the repo-root `docs/` tree (`specs/`, `decisions/`, `findings/`, `runbooks/`,
`conventions/`, `tech_debt/`, `templates/`). Names are `snake_case`, specs carry a `status`
frontmatter, decisions are one file each with `Context` / `Decision` / `Why` / `Alternatives` /
`Impact` / `Who` (`DECISION(<slug>)` in code), and known debt is a `docs/tech_debt/<slug>.md` file
plus a `TECH_DEBT(<slug>)` comment in code. Standard and CI gate:
`docs/conventions/docs_and_specs.md`, `scripts/check_docs_layout.py`. Every claim in a doc, PR or
comment carries its evidence or is phrased as a question: `.claude/rules/evidence.md`. Every bean
carries an A2V track (`Asked`, `Built`, `Proven`, `Reviewed`, `Shipped`, `Verified`) with Who /
Why / Evidence: `.claude/rules/a2v-track.md`.

## Common Commands

Python dependencies and tests are managed with **uv**:

```bash
uv run pytest -v                                              # Run Python unit tests
uv run ruff check src tests scripts                           # Lint Python code
uv run python scripts/check_docs_layout.py                    # Check docs, tech debt, decisions & beans
uv run python scripts/check_docs_layout.py --write-index      # Regenerate README.md indexes in docs/
uv run python scripts/run_emulator_e2e.py                     # Run E2E lifecycle test on bigquery-emulator
```

## Key Architectural Decisions & Conventions

- **Default `ref()` resolution is `env_view` (Option A)**: `{{ dbt_virtual.virtual_ref('A') }}` resolves virtualized tables to `<env_schema>.A` while physical writes go to `<physical_schema>.A__<fp>`. Modes `physical` (Option B) and `hybrid` (Option C) are also supported via `var('dbt_virtual').ref_resolution` (`docs/decisions/20261027_default_ref_resolution_env_view.md`, `DECISION(default_ref_resolution_env_view)`).
- **Hybrid Packaging**: Every lifecycle operation works both via the `dbt-virtual` Python CLI and via `dbt run-operation` macros (`docs/decisions/20261027_hybrid_package_and_bigquery_first.md`).
- **BigQuery Emulator in CI**: We test end-to-end SQL execution (registry creation, fingerprinted table builds, TVF creation, `virtual_promote` view swaps, and `virtual_rollback`) against `Flip-to/bigquery-emulator` (`ghcr.io/flip-to/bigquery-emulator:flipto` or built from `https://github.com/Flip-to/bigquery-emulator.git#flipto/main`).

## Governance Workflows

- **Record a decision**: Use `/record-decision` (`.claude/skills/record-decision/SKILL.md`), copy `docs/templates/decision.md` to `docs/decisions/YYYYMMDD_<slug>.md`, optionally add `DECISION(<slug>)` in code, and run `uv run python scripts/check_docs_layout.py --write-index`.
- **Record or pay off tech debt**: Use `/record-tech-debt` (`.claude/skills/record-tech-debt/SKILL.md`), copy `docs/templates/tech_debt.md` to `docs/tech_debt/<slug>.md`, link an open bean in `.beans/`, add `TECH_DEBT(<slug>)` at the code site, and run `uv run python scripts/check_docs_layout.py --write-index`.
- **Track beans & A2V**: Every PR has a bean in `.beans/` with an `## A2V track` table (`.claude/rules/beans-tracking.md`, `.claude/rules/a2v-track.md`).
