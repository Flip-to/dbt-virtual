---
# dbt-virtual-0001
title: 'Initial architecture spec, governance, macros, and CLI for dbt-virtual'
status: completed
type: task
created_at: 2026-10-27T20:20:00Z
updated_at: 2026-10-27T23:10:00Z
---

Scaffold `Flip-to/dbt-virtual` with full specifications (`docs/specs/`), step-by-step user guides (`docs/guides/`), architectural decisions (`docs/decisions/`), tech debt tracking (`docs/tech_debt/`), `scripts/check_docs_layout.py`, core dbt macros/materializations, Python CLI, `sqlglot` AST normalization, multi-adapter dispatch (BigQuery, Snowflake, DuckDB, PostgreSQL, Databricks), and CI/CD (including `Flip-to/bigquery-emulator`, `nnnkkk7/snowflake-emulator`, `postgres:16-alpine`, and `dbt-core` + `dbt-duckdb` E2E tests).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Build open-source SQLMesh-style virtual data environments for dbt with full specs, governance, and CI | chat request | 2026-10-27 |
| Built | Agent: Claude (session 8546528b-092b-4b9b-b7e2-853b31780db0), approved by Yuri Olive | Hybrid dbt macro package + Python CLI (`dbt-virtual`), Option A `env_view` default ref resolution (`docs/decisions/20261027_default_ref_resolution_env_view.md`) | commits on main | 2026-10-27 |
| Proven | CI | L2: 26 unit, `hypothesis` property-based, and real `dbt-duckdb` E2E tests + `check_docs_layout.py` + `Flip-to/bigquery-emulator` E2E + `nnnkkk7/snowflake-emulator` & Postgres E2E | `pytest` + `scripts/run_emulator_e2e.py` + `scripts/run_snowflake_emulator_e2e.py` | 2026-10-27 |
| Reviewed | Developer: Yuri Olive | Reviewed architecture, CI caching, SQLMesh line-by-line parity, and GitHub Pages docs | direct push to main | 2026-10-27 |
| Shipped | CI | Pushed to `Flip-to/dbt-virtual` `main` and deployed to GitHub Pages | GitHub Actions runs 40619216191 & 40619216390 | 2026-10-27 |
| Verified | CI | All 5 CI jobs green (Python 3.11/3.12/3.13 + BigQuery Emulator E2E + Snowflake/Postgres E2E in 51s) + GitHub Pages live | GitHub Actions runs 40619216191 & 40619216390 | 2026-10-27 |

## Todo

- [x] Write dated specs in `docs/specs/`, user guides in `docs/guides/`, and decision records in `docs/decisions/`
- [x] Add `CLAUDE.md`, `AGENTS.md`, `.claude/rules/`, `.claude/skills/`, and `.beans/`
- [x] Add `docs/tech_debt/`, `docs/templates/`, `docs/conventions/docs_and_specs.md`, and `scripts/check_docs_layout.py`
- [x] Add GitHub Actions CI/CD with `Flip-to/bigquery-emulator`, `nnnkkk7/snowflake-emulator`, Postgres, and `dbt-duckdb` E2E test harnesses + GitHub Pages (`mkdocs.yml`)
