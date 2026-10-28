---
# dbt-virtual-0016
title: 'Treat catalog snapshots as authoritative in plans'
status: completed
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T12:00:00Z
---

The planner currently assumes every registered fingerprint has a physical table, even when the supplied catalog snapshot says the table is absent. This can claim `REUSE` or `SEED_INCREMENTAL` for a missing table. Respect explicit catalog snapshots and require the base physical table to exist before choosing incremental seeding.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue review and convert stale-pointer planning bugs into regression tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Treat provided table catalog as authoritative and preserve pointer assumptions only when catalog data is omitted | `src/dbt_virtual/planner.py`; `src/dbt_virtual/cli.py`; `tests/test_cli_and_macros.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: stale base and target pointers plan full builds; state files without catalog data preserve compatibility | `uv run pytest -q` (28 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed | Agent: Claude (session 6f3c6805-18ef-442a-8407-0380c1bfc2e1), approved by Yuri Olive | Diff reviewed against main; no defects found | PR #14 review, 2026-10-28; merge approved by Yuri Olive in chat | 2026-10-28 |
| Shipped | Developer: Yuri Olive | Squash-merged to main | PR #14 merged | 2026-10-28 |
| Verified | CI | Post-merge main CI run | pending: main CI run after merge | pending |
