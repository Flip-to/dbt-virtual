---
status: accepted
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Routing, `ref()` resolution modes, and zero-copy `CLONE` incremental seeding

## Problem

1. **Cold-start full refreshes on new fingerprints**: When an incremental model gets a new fingerprint `<fp_new>`, `<physical_schema>.<model>__<fp_new>` does not exist yet. Standard dbt evaluates `is_incremental()` as `False` and runs an expensive full historical rebuild instead of an incremental update on top of existing production data.
2. **Downstream `ref()` coupling**: When model or routine `B` calls `{{ ref('A') }}` on a virtualized model `A`, we must decide whether `B` compiles against the virtual environment view (`<env_schema>.A`) or the physical table (`<physical_schema>.A__<fp_A>`).

## Decision

1. **Physical vs. Virtual Routing (`macros/routing/virtual_routing.sql`)**:
   - Virtualized `table` and `incremental` models write to `<physical_schema>.<model>__<fp>`.
   - Virtualized routines (`table_function`, `function`, `procedure`) and views deploy to `<env_schema>.<model>`.
2. **`ref()` Resolution Modes (Default Option A + Configurable Strategy)**:
   - See decision [20261027_default_ref_resolution_env_view.md](../decisions/20261027_default_ref_resolution_env_view.md).
   - **Mode A (`ref_resolution: "env_view"`, default)**: Resolves `{{ ref('A') }}` to `<env_schema>.A`. Downstream views and BigQuery TVFs read the virtual view, so promoting or rolling back `A` via view swap requires zero recompilation of unchanged downstream readers.
   - **Mode B (`ref_resolution: "physical"`)**: Resolves `{{ ref('A') }}` directly to `<physical_schema>.A__<fp_A>`.
   - **Mode C (`ref_resolution: "hybrid"`)**: Physical tables bind `<physical_schema>.A__<fp_A>`, while routines and views bind `<env_schema>.A`.
   - **Mode D (`ref_resolution: "explicit"`)**: Standard `ref()` is untouched; users call `{{ dbt_virtual.virtual_ref('A') }}` explicitly.
3. **Zero-Copy `CLONE` Seeding (`macros/lifecycle/hooks.sql` `clone_if_new_fp`)**:
   - Before an `incremental` model runs (when `--full-refresh` is not set and `<model>__<fp_new>` does not exist), query `<physical_schema>._environments` for `base_environment` (`prod`) and execute `CREATE TABLE IF NOT EXISTS <physical>.<model>__<fp_new> CLONE <physical>.<model>__<fp_base>`.
4. **Post-Hook View Sync (`sync_env_view`)**:
   - Point `<env_schema>.<model>` at `<physical_schema>.<model>__<fp_new>` and upsert `_environments`.

## Alternatives

- **Always rebuild new fingerprints from scratch**: Rejected; full refreshes of multi-year incremental tables are prohibitively expensive and slow.
- **Hardcode only Mode A or only Mode B**: Rejected; making `ref_resolution` a config setting (`env_view` default, `physical` and `hybrid` supported) satisfies both instant TVF view-swapping and strict physical lineage use cases.

## Rollout

1. Implement `virtual_schema_name`, `virtual_alias_name`, and `virtual_ref` in `macros/routing/virtual_routing.sql`.
2. Implement `clone_if_new_fp` and `sync_env_view` in `macros/lifecycle/hooks.sql`.
3. Verify partition pruning through virtual views (dry-run benchmark on BigQuery confirmed 438,213,812 bytes scanned both directly and through a `SELECT *` view over a 194.4 GB partitioned table).

## Verification

- Verify `virtual_schema_name`, `virtual_alias_name`, `clone_if_new_fp`, and `sync_env_view` in the BigQuery emulator E2E test (`scripts/run_emulator_e2e.py`).
