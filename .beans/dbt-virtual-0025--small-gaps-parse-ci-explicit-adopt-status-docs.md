---
# dbt-virtual-0025
title: 'Small gaps: parse CI, explicit ref mode, adopt views/routines, status without registry, docs'
status: completed
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
| Proven | CI | L3: tests named in the spec, each failing before its change | CI on PR #19 head 80b7a59, all 6 jobs green (unit 3.10/3.11/3.12, BigQuery emulator E2E strict with no skipped steps, Snowflake+Postgres E2E, dbt parse BigQuery/Snowflake) | 2026-11-05 |
| Reviewed | Developer: Yuri Olive | Asked to merge after the coordinator review of every merged agent branch | Chat 2026-11-05 ("Resolva tudo por e de merge"); no line-by-line human review | 2026-11-05 |
| Shipped | Developer: Yuri Olive | | PR #19 merged to main | 2026-11-05 |
| Verified | Developer: Yuri Olive | | pending: real BigQuery run in the sandbox project (authorized views, view OPTIONS, cross-project CLONE, data diff SQL not yet run on BigQuery) | |
