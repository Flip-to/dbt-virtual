---
# dbt-virtual-0004
title: 'Preserve quoted comment markers during SQL fingerprint normalization'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

The comment stripping in `src/dbt_virtual/fingerprint.py` must not truncate SQL string literals or quoted identifiers containing `--` or `/*`. The current regex-based stripping can change valid SQL before fingerprinting and risks reusing a physical table for a different model definition.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Review dbt-virtual against SQLMesh and fix concrete bugs | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Replace regex comment removal with quote-aware scanning | `src/dbt_virtual/fingerprint.py` | 2026-10-28 |
| Proven | CI | L2: Regression tests distinguish quoted `--` and `/* */` text from plain values; all supported CI suites pass | GitHub Actions run 40775032046: Python 3.10, 3.11, 3.12 unit/doc checks and BigQuery, Snowflake, Postgres E2E passed; local pytest 29 passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
