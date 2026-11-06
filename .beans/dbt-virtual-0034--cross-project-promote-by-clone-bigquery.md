---
# dbt-virtual-0034
title: 'Verify cross-project promote by clone on real BigQuery'
status: in-progress
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
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
| Proven | pending | L3 local: real BigQuery run v2 18/18 checks; harness test and emulator step 21 fail without the fix; CI emulators pending | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | Least-privilege service accounts not tested | pending | |
