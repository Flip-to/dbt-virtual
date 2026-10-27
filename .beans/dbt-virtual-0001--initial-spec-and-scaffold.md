---
# dbt-virtual-0001
title: 'Initial architecture spec, governance, macros, and CLI for dbt-virtual'
status: in-progress
type: task
created_at: 2026-10-27T20:20:00Z
updated_at: 2026-10-27T20:50:00Z
---

Scaffold `Flip-to/dbt-virtual` with full specifications (`docs/specs/`), architectural decisions (`docs/decisions/`), tech debt tracking (`docs/tech_debt/`), `scripts/check_docs_layout.py`, core dbt macros/materializations, Python CLI, and CI/CD (including `Flip-to/bigquery-emulator` integration).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Build open-source SQLMesh-style virtual data environments for dbt with full specs, governance, and CI | chat request | 2026-10-27 |
| Built | Agent: Claude (session 8546528b-092b-4b9b-b7e2-853b31780db0), approved by Yuri Olive | Hybrid dbt macro package + Python CLI (`dbt-virtual`), Option A `env_view` default ref resolution (`docs/decisions/20261027_default_ref_resolution_env_view.md`) | commits on main | 2026-10-27 |
| Proven | CI | L2: unit tests + `check_docs_layout.py` + emulator E2E workflow | `pytest` + `scripts/check_docs_layout.py` | 2026-10-27 |
| Reviewed | pending | | | |
| Shipped | pending | | | |
| Verified | pending | | | |

## Todo

- [x] Write dated specs in `docs/specs/` and decision records in `docs/decisions/`
- [x] Add `CLAUDE.md`, `AGENTS.md`, `.claude/rules/`, `.claude/skills/`, and `.beans/`
- [x] Add `docs/tech_debt/`, `docs/templates/`, `docs/conventions/docs_and_specs.md`, and `scripts/check_docs_layout.py`
- [x] Add GitHub Actions CI/CD with `Flip-to/bigquery-emulator` E2E test harness
