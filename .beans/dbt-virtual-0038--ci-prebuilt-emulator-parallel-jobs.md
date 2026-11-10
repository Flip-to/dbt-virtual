---
# dbt-virtual-0038
title: 'CI: prebuilt emulator image and parallel emulator jobs'
status: in-progress
type: task
created_at: 2026-11-10T00:00:00Z
updated_at: 2026-11-10T00:00:00Z
---

A PR waited about 46 minutes (run 42829367114, 2026-11-10), dominated by the job "BigQuery Emulator E2E". That job ran,
in sequence on one runner: the emulator start (3 min 55 s, building from source), `scripts/run_emulator_e2e.py` (4 min 26 s),
`scripts/run_emulator_concurrency_upgrade_e2e.py` (3 min 42 s) and the real dbt-bigquery pytest file
(34 min, 4 workers on one shared emulator).

This change:

- starts the emulator through the composite action `.github/actions/start-bq-emulator`: it resolves the head commit of
  `flipto/main`, pulls the prebuilt `ghcr.io/flip-to/bigquery-emulator:flipto-<sha7>` (published by the emulator repo's
  `flipto-image` workflow), checks its revision label equals the commit, and builds from source only when the image is
  missing or does not match. CI stays pinned to the exact emulator revision it tests;
- splits the work into parallel jobs, each with its own emulator container: lifecycle script, concurrency and upgrade
  checks, and the real-dbt pytest file in three shards (`-k` selection; the third shard is the complement of the other two,
  so every test runs once). pytest still runs with `-n 4 --dist load`. No test is dropped.

Measurements are in the PR body.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | CI takes about 40 minutes, dominated by the emulator job | chat request | 2026-11-10 |
| Built | Agent: Claude (session dbt-virtual-ci-speed), approved by Yuri Olive | Prebuilt image by SHA plus parallel jobs; no test dropped | PR (draft), branch claude/dbt-virtual-ci-prebuilt-emulator | 2026-11-10 |
| Proven | pending | | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
