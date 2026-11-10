---
# dbt-virtual-0035
title: 'Emulator E2E for BigQuery clone chains (depth query, COPY at depth 3)'
status: completed
type: task
created_at: 2026-11-06T00:00:00Z
updated_at: 2026-11-10T00:00:00Z
---

The clone-depth logic of `clone_or_copy_table` (bean 0033) was covered only by macro harness tests, because the
emulator had no `table_type = 'CLONE'`, no `base_table_*` columns, no chain limit and no `CREATE TABLE ... COPY`, so the
emulator steps ran with `clone_chain_fallback: false`.

The emulator now has them (Flip-to/googlesqlite#38, Flip-to/bigquery-emulator#35). This change removes the
`clone_chain_fallback: false` workarounds from the emulator steps and adds step 30.5, which seeds a chain through
`seed_incremental_clones`: CLONE at depth 0, 1, 2, COPY at depth 3, then CLONE of the copy.

Depends on the two emulator PRs: CI uses the image built from `flipto/main` of the emulator, so the emulator E2E job
fails here until they are merged.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Test the clone-depth logic on the emulator | chat request | 2026-11-06 |
| Built | Agent: Claude (session dbt-virtual-clone-chain-e2e), approved by Yuri Olive | Remove the workarounds, add step 30.5; decision 20261105_bigquery_clone_chain_copy corrected | PR (draft), branch claude/dbt-virtual-clone-chain-e2e | 2026-11-06 |
| Proven | CI | L3: emulator E2E and unit suites green on the emulator with Flip-to/bigquery-emulator#36 and #43 | CI run 42138863307 | 2026-11-06 |
| Reviewed | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Diff and CI read before merge; merge authorized in chat | PR #33 | 2026-11-06 |
| Shipped | Developer: Yuri Olive | Merged (merge commit), released in tag v0.2.1 | PR #33, 23efa1d | 2026-11-06 |
| Verified | pending | Real BigQuery clone-chain behaviour was measured before this change (bean 0033); not re-run here | pending: re-run scripts/run_bigquery_sandbox_e2e.py on v0.2.1 | |
