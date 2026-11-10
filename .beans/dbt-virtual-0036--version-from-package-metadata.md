---
# dbt-virtual-0036
title: '`dbt-virtual --version` reads the installed package version'
status: completed
type: bug
created_at: 2026-11-10T00:00:00Z
updated_at: 2026-11-10T00:00:00Z
---

`dbt-virtual --version` printed `dbt-virtual 0.1.0` with release 0.2.1 installed (seen 2026-11-09 in the flipto-dbt
pilot venv, `uv pip show dbt-virtual` said 0.2.1): `__version__` in `src/dbt_virtual/__init__.py` was a hard-coded
string that no release bumped. It now comes from `importlib.metadata`, so `pyproject.toml` is the only place the
version is written. Closes beans 0034 and 0035, whose PRs (#32, #33) merged on 2026-11-06 with the beans still open.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue the pilot work; stale version string found while installing v0.2.1 | chat request | 2026-11-10 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | `__version__` from package metadata | branch claude/dbt-virtual-version-metadata | 2026-11-10 |
| Proven | CI | L1: `tests/test_version.py` compares `__version__` with pyproject.toml; fails on main's hard-coded 0.1.0 (local run 2026-11-10) | CI run 42829367114, all 6 checks green | 2026-11-10 |
| Reviewed | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Diff and CI read before merge; merge authorized in chat | PR #35 | 2026-11-10 |
| Shipped | Developer: Yuri Olive | Merged with a merge commit | PR #35 | 2026-11-10 |
| Verified | n/a | | n/a: no warehouse behaviour | |
