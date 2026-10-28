---
status: implemented
ticket: None
bean: dbt-virtual-0001
prs: []
---

# Merkle-DAG fingerprinting and canonical SQL normalization

## Problem

A virtual data environment relies on content-addressed physical tables (`<model>__<fp>`). If the fingerprint `<fp>` is computed over raw uncompiled SQL (`node.raw_code`), it misses changes to shared Jinja macros and `dbt_project.yml` variables (`var()`), reusing a stale physical table. Conversely, if `<fp>` is computed over raw `node.compiled_code`, target-specific schema names (`analytics_staging` vs. `analytics`) and self-references (`{{ this }}`) change the hash between `staging` and `prod`, forcing a rebuild on promotion.

## Decision

Compute each virtualized node's 12-character hex SHA-256 fingerprint (`src/dbt_virtual/fingerprint.py`) over a canonical JSON payload containing:

1. **Normalized & AST-Canonicalized Compiled SQL (`normalized_compiled_sql`)**:
   - Block comments (`/* ... */`) and line comments (`-- ...`) stripped and whitespace runs collapsed.
   - Any existing `__<fp>` suffix stripped.
   - Known environment and physical schema prefixes replaced with `__DBT_VIRTUAL_SCHEMA__.`.
   - Self-identifier (`node.alias` / `node.name`) replaced with `__DBT_VIRTUAL_THIS__`.
   - Parsed and canonicalized through **`sqlglot` AST** (`canonicalize_sql_ast()`) with graceful fallback to token normalization for custom DDL extensions.
2. **Normalized raw SQL (`normalized_raw_sql`)**:
   - Included alongside `normalized_compiled_sql` so edits inside an unexecuted `{% if is_incremental() %}` or `{% else %}` branch still change the fingerprint.
3. **Physical materialization config (`config`)**:
   - `materialized`, `incremental_strategy`, `unique_key`, `partition_by`, `cluster_by`, `on_schema_change`, `parameters`, `returns`, `virtual_env_epoch`, `forward_only`, and optional `physical_version` pinning.
4. **Upstream fingerprints (`upstream_fingerprints`)**:
   - Topological Merkle-DAG map `{parent_unique_id: parent_fp}` for all upstream nodes that also set `virtual_env: true`.

When running bare `dbt run` without the `dbt-virtual` CLI, `macros/fingerprint/get_fingerprint.sql` provides a pure-Jinja fallback hash (`local_md5`) that walks `model.depends_on.nodes` up to 3 levels in `graph.nodes`.

## Alternatives

- **Hash `node.raw_code` only**: Rejected; misses macro and `var()` changes.
- **Hash `node.compiled_code` without normalization**: Rejected; `staging` and `prod` compiles produce different hashes, preventing zero-rebuild promotion.
- **Regex normalization only without AST canonicalization**: Replaced; combining schema/suffix token stripping with `sqlglot` AST canonicalization makes the hash resilient to formatting and casing changes while preserving a fallback for custom BigQuery `ANY TABLE` syntax.

## Rollout

1. Ship `src/dbt_virtual/fingerprint.py` with `compute_manifest_fingerprints()`, `compute_node_fingerprint()`, `normalize_sql()`, `canonicalize_sql_ast()`, and `extract_sql_output_columns()`.
2. Ship `macros/fingerprint/get_fingerprint.sql` to consume `var('_dbt_virtual_fps')` with the 3-level Merkle-DAG pure-Jinja fallback ([jinja_fingerprint_no_merkle_parents.md](../tech_debt/jinja_fingerprint_no_merkle_parents.md), paid in bean `dbt-virtual-0002`).

## Verification

- Pin target-schema invariance (`test_target_schema_invariance`), SQL/config sensitivity (`test_sql_or_config_change_invalidates_fingerprint`), Merkle-DAG parent propagation (`test_merkle_dag_upstream_propagation`), `sqlglot` AST canonicalization (`test_sqlglot_ast_canonicalization_and_column_extraction`), `physical_version` pinning (`test_physical_version_pinning_and_forward_only_config`), and `hypothesis` property-based fuzzing (`test_property_based_sql_normalization_and_dag_determinism`) in `tests/test_fingerprint.py`.
