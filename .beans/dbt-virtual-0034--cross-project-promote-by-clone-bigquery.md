---
# dbt-virtual-0034
title: 'Verify cross-project promote by clone on real BigQuery'
status: completed
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-10T00:00:00Z
---

Ran promote-by-clone between two real GCP projects (CI and prod) with the new `--xproject` mode of
`scripts/run_bigquery_sandbox_e2e.py`. Everything in the decision's "not verified" list that could be
checked with one identity passed, and one bug showed up: the routine template copied into the prod
`_releases` kept the CI project id, so a prod rollback failed. See
docs/findings/20261105_cross_project_promote_by_clone_bigquery.md and
docs/decisions/20261105_cross_project_promote_by_clone.md.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Cross-project clone, IAM and the routine-template rewrite were never run on BigQuery | chat request | 2026-11-05 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | `--xproject` sandbox mode; `retarget_project_in_ddl` stores the rewritten template | PR (draft), branch claude/dbt-virtual-xproject-verify | 2026-11-05 |
| Proven | CI | L3: emulator E2E and unit suites green on the emulator with Flip-to/bigquery-emulator#36 and #43 | CI run 42137953605 | 2026-11-06 |
| Reviewed | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Diff and CI read before merge; merge authorized in chat | PR #32 | 2026-11-06 |
| Shipped | Developer: Yuri Olive | Merged (merge commit), released in tag v0.2.1 | PR #32, 274a4fa | 2026-11-06 |
| Verified | Developer: Yuri Olive | Cross-project promote and rollback on real BigQuery (ondemand to a dev project) | sandbox run v2, 18/18 checks; least-privilege service accounts: pending: not tested | 2026-11-05 |
