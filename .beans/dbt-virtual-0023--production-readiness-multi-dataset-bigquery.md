---
# dbt-virtual-0023
title: 'Production readiness for multi-dataset BigQuery projects'
status: in-progress
type: feature
created_at: 2026-11-04T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

Make dbt-virtual usable by a real BigQuery dbt project that publishes to several datasets, has
its own routine materializations, and builds dev/PR/prod with different datasets. Stacked on
dbt-virtual-0021 (PR #18).

1. Fingerprints are the same in every environment: database/schema of three-part references to
   dependency nodes are stripped; seeds (checksum) and snapshots count as dependencies
   (`test_ref_locations_do_not_change_fingerprints_across_environments`: equal fingerprints with
   the change, different without it, measured 2026-11-04).
2. One schema per custom schema per environment (decision
   `docs/decisions/20261104_per_custom_schema_env_datasets.md`), `schema_key` registry column with
   automatic migration, per-object routine placeholders, public `register_routine_deployment`.
3. `virtual_function` materialization: dbt-core 1.11+ ships `function`, which overrides the
   package's (`dbt/contracts/graph/manifest.py:1139-1163`, dbt-core 1.12.6), so
   `materialized='function'` models failed with "'NoneType' object has no attribute 'type'".
4. Promotion order: environment views before routines (a UDF reading a model view failed with
   "schema analytics_gold does not exist" when UDFs went first).
5. `ensure_registry` runs DDL only when the registry is missing or lacks `schema_key`.
6. Test harness `return()` now stops the macro, as dbt does.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Make dbt-virtual productive for the team's BigQuery project, fix everything found in the review | Chat request 2026-11-04 ("temos que arrumar tudo e permitir esse repo ficar produtivo para a gnt") | 2026-11-04 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | Items above | branch claude/dbt-virtual-prod-ready (48d2c41, b390217) | 2026-11-05 |
| Proven | CI | L3: `tests/test_dbt_duckdb_multi_schema.py` (real dbt-duckdb) and the fingerprint tests | pending: CI on the PR | |
| Reviewed | Reviewer: pending | | pending | |
| Shipped | Developer: Yuri Olive | | pending | |
| Verified | Developer: Yuri Olive | | pending: real BigQuery sandbox run and a pilot in the team's project | |
