---
# dbt-virtual-0001
title: 'Initial architecture spec, governance, macros, and CLI for dbt-virtual'
status: completed
type: task
created_at: 2026-10-27T20:20:00Z
updated_at: 2026-10-27T21:20:00Z
---

Scaffold `Flip-to/dbt-virtual` with full specifications (`docs/specs/`), architectural decisions (`docs/decisions/`), tech debt tracking (`docs/tech_debt/`), `scripts/check_docs_layout.py`, core dbt macros/materializations, Python CLI, `sqlglot` AST normalization, multi-adapter dispatch, and CI/CD (including `Flip-to/bigquery-emulator` integration).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Build open-source SQLMesh-style virtual data environments for dbt with full specs, governance, and CI | chat request | 2026-10-27 |
| Built | Agent: Claude (session 8546528b-092b-4b9b-b7e2-853b31780db0), approved by Yuri Olive | Hybrid dbt macro package + Python CLI (`dbt-virtual`), Option A `env_view` default ref resolution (`docs/decisions/20261027_default_ref_resolution_env_view.md`) | commits on main | 2026-10-27 |
| Proven | CI | L2: 21 unit + macro execution tests + `check_docs_layout.py` + 9-stage `Flip-to/bigquery-emulator` E2E workflow | `pytest` + `scripts/run_emulator_e2e.py` | 2026-10-27 |
| Reviewed | Developer: Yuri Olive | Reviewed architecture, CI caching, and SQLMesh parity gaps | direct push to main | 2026-10-27 |
| Shipped | CI | Pushed to `Flip-to/dbt-virtual` `main` | GitHub Actions run 40583821813 | 2026-10-27 |
| Verified | CI | All 4 CI jobs green (Python 3.10/3.11/3.12 + BigQuery Emulator E2E in 26s) | GitHub Actions run 40583821813 | 2026-10-27 |

## Todo

- [x] Write dated specs in `docs/specs/` and decision records in `docs/decisions/`
- [x] Add `CLAUDE.md`, `AGENTS.md`, `.claude/rules/`, `.claude/skills/`, and `.beans/`
- [x] Add `docs/tech_debt/`, `docs/templates/`, `docs/conventions/docs_and_specs.md`, and `scripts/check_docs_layout.py`
- [x] Add GitHub Actions CI/CD with `Flip-to/bigquery-emulator` E2E test harness
