---
# dbt-virtual-0030
title: 'Concurrency and v0.1.0 -> current upgrade tests (DuckDB, BigQuery emulator, PostgreSQL)'
status: in-progress
type: task
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

1. Concurrency scenarios (`tests/registry_concurrency.py`) on DuckDB (pytest), the BigQuery emulator and
   PostgreSQL (scripts): concurrent registry flushes, interleaved upserts of one pointer, the `_locks`
   lease (simultaneous acquire, lifecycle, expired takeover), `virtual_promote(lock=True)`.
2. Upgrade v0.1.0 -> current on real dbt + DuckDB (`tests/test_dbt_duckdb_upgrade_v010.py`) and on the
   BigQuery emulator (`scripts/run_emulator_concurrency_upgrade_e2e.py`): registry written by the tag,
   then `status`, `plan`, `build`, `promote`, `rollback` with the current code.
3. Bug fixed: `status` / `rollback` / `drop-env` failed on a v0.1.0 registry (`build_quality` not found);
   `migrate_registry_if_old` migrates it first.
4. Known limitation recorded: docs/tech_debt/cli_lock_owner_shared_per_user_host.md.

The non-atomic DELETE+INSERT pointer upsert (DuckDB/PostgreSQL) is pinned by a strict xfail that points at
branch claude/dbt-virtual-registry-merge; this bean does not change the upsert macros.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Concurrency and upgrade paths of the registry and lease lock were untested | Chat request 2026-11-05 | 2026-11-05 |
| Built | Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Scenarios shared by all warehouses; fix for the v0.1.0 registry read failure | branch claude/dbt-virtual-concurrency-upgrade-tests | 2026-11-05 |
| Proven | pending | | pending: CI on the draft PR | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending: real BigQuery run (the emulator does not serialize or conflict DML) | |
