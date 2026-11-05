---
# dbt-virtual-0033
title: 'Pilot round 2: rollback over legacy tables, lock release, double clone, clone chain, composite diff key'
status: in-progress
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Five bugs from the second real-BigQuery pilot:

1. `rollback --to-release <adopt release>` failed on a legacy TABLE left at the env path by `adopt --keep-existing-table`
   (`CREATE OR REPLACE VIEW` on a table). Rollback now drops the legacy table (after its pre-flight), and promote
   replaces a legacy table even for an unchanged model.
2. A failed promote / rollback kept its lease (15 min). Package raise sites release it (`fail_releasing_lock`), and the
   CLI releases it when the operation fails (warehouse errors cannot be caught in Jinja). `virtual_acquire_lock` is untouched.
3. A new incremental was cloned twice per build. `_dbt_virtual_seeded` records what this invocation seeded.
4. BigQuery clone chains are limited to 3. See docs/decisions/20261105_bigquery_clone_chain_copy.md.
5. `diff-data` bisected on the first `diff_key` entry: now the first numeric entry; none numeric joins whole segments.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Five failures found by a real-BigQuery pilot | pilot 2026-11-05 | 2026-11-05 |
| Built | Agent: Claude (session dbt-virtual-pilot-round2), approved by Yuri Olive | One fix per finding, test first | PR (draft), branch claude/dbt-virtual-pilot-round2 | 2026-11-05 |
| Proven | pending | Tests in tests/test_pilot_round2.py, tests/test_dbt_duckdb_pilot_round2.py, emulator step 20; CI pending | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | Not run on real BigQuery: clone depth query and COPY | pending | |
