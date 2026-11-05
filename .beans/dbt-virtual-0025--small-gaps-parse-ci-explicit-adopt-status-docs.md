---
# dbt-virtual-0025
title: 'Small gaps: parse CI, explicit ref mode, adopt views/routines, status without registry, docs'
status: in-progress
type: task
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Stacked on dbt-virtual-0023.

1. `dbt parse` job on BigQuery and Snowflake profiles (fake credentials); fails on the macros of 0f42ee9^.
2. `ref_resolution: explicit` and unknown values are rejected (decision 20261105_ref_resolution_explicit_is_rejected).
3. `virtual_adopt` registers deployed views and routines (templates from compiled SQL).
4. `virtual_export_state` returns empty state when the registry does not exist.
5. CLAUDE.md refreshed; spec 20261105_multi_dataset_envs_views_run_and_guards.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Close five small gaps found in the production-readiness review | Coordinator request 2026-11-05 | 2026-11-05 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Items above | branch claude/dbt-virtual-small-gaps | 2026-11-05 |
| Proven | CI | L3: tests named in the spec, each failing before its change (recorded in the commit messages) | pending: CI on the PR | |
| Reviewed | Reviewer: pending | | pending | |
| Shipped | Developer: Yuri Olive | | pending | |
| Verified | Developer: Yuri Olive | | pending: real BigQuery sandbox run | |
