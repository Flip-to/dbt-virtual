---
# dbt-virtual-0024
title: 'Close companion state gaps (Snowplow manifest tracking)'
status: in-progress
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Deferred items of the companion state mechanism for manifest-tracked models; see
[companion_state_gaps](../docs/tech_debt/companion_state_gaps.md).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Virtualize Snowplow-manifest incrementals without losing history | Chat request 2026-11-05; tech debt companion_state_gaps | 2026-11-05 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Pays three gaps (failed build, gc, cross-location promote); the other items stay in the tech debt file with the reason | branch claude/dbt-virtual-companion-state-gaps; macros `companion_repair_failed`, `companion_forget_tables`, `restore_companion_state(source_*)` | 2026-11-05 |
| Proven | CI | L3: 4 DuckDB tests (real dbt) + 7 BigQuery-SQL harness tests + emulator steps 18, 20, 21; each fails without the macros (harness: 7 of 7 failed on main macros; DuckDB: 4 of 4 failed before the fix; emulator: step 18 assertion failed on main macros) | local runs 2026-11-05, CI pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
