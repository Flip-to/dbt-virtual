---
status: accepted
kind: developer
who: "Developer: Yuri Olive"
ticket: None
spec: docs/specs/20261027_routing_ref_modes_and_clone_seeding.md
prs: []
---

# Default `ref()` resolution for virtualized models is `env_view` (Option A), with `physical`, `hybrid`, and `explicit` supported

## Context

When a downstream model or routine `B` calls `{{ ref('A') }}` on a virtualized model `A` (`virtual_env: true`), `A`'s physical data lives in `<physical_schema>.A__<fp>`, while each environment exposes a virtual view `<env_schema>.A` (`SELECT * FROM <physical_schema>.A__<fp>`). We needed to decide what `{{ ref('A') }}` compiles to by default.

## Decision

Set `ref_resolution: "env_view"` (Option A) as the default in `dbt-virtual`, so `{{ ref('A') }}` resolves to the virtual environment view `<env_schema>.A`, while also implementing configurable support for `physical` (Option B), `hybrid` (Option C), and `explicit` (Option D) in `macros/routing/virtual_routing.sql`.

## Why

- **Instant view-swap promotion and rollback for TVFs and downstream views**: In BigQuery, table functions (`CREATE TABLE FUNCTION`) bind their referenced relation names at compile time and cannot be aliased by a view. When a TVF in `spacetime` reads the environment view `spacetime.traffic_metrics_root`, promoting or rolling back `traffic_metrics_root` by swapping that view immediately changes what the TVF serves without needing to recompile or redeploy unchanged TVFs.
- **Zero partition pruning penalty**: Measured by dry-run on BigQuery on 2026-10-26 (`Flip-to/flipto-dbt#509`): querying a 194,437,234,666-byte partitioned root table directly vs. through a `SELECT *` view vs. through a compiled TVF reading that view estimated the exact same 438,213,812 bytes.
- **Future flexibility preserved**: Supporting `ref_resolution: "physical"` and `"hybrid"` in the same macro (`DECISION(default_ref_resolution_env_view)`) lets teams switch modes via `dbt_project.yml` without rewriting model SQL.

## Alternatives

- **Option B (`physical` as default)**: Resolves `{{ ref('A') }}` directly to `<physical_schema>.A__<fp>`. Provides strict physical immutability across table chains, but requires re-executing `CREATE OR REPLACE TABLE FUNCTION` for every downstream TVF whenever a root table's fingerprint changes. Kept as a supported config option (`ref_resolution: "physical"`).
- **Option C (`hybrid` as default)**: Resolves `table`/`incremental` models to `<physical_schema>.A__<fp>` and routines/views to `<env_schema>.A`. Kept as a supported config option (`ref_resolution: "hybrid"`).
- **Option D (`explicit` macro only)**: Leaves `ref()` untouched and requires `{{ dbt_virtual.virtual_ref('A') }}`. Rejected as default because it requires editing every `ref()` call in opted-in models.

## Impact

- Default configuration in `dbt_project.yml` and `macros/fingerprint/get_fingerprint.sql` sets `ref_resolution: "env_view"`.
- Governed in code by `DECISION(default_ref_resolution_env_view)` in `macros/routing/virtual_routing.sql`.
- Revisit if concurrent builds mutating the same environment schema in-flight become a common pattern (where `hybrid` or `physical` would be preferred).

## Who

Developer decision: Yuri Olive, 2026-10-27 ("lets go with A but document we can change to other options or support other options in the future").
