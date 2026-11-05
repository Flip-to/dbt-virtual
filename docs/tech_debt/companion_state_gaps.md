---
status: open
area: lifecycle
impact: medium
effort: m
bean: dbt-virtual-0024
---

# Companion state is restored at lifecycle operations only

## What

`TECH_DEBT(companion_state_gaps)` in `macros/lifecycle/companion_state.sql`. Companion state
(see [the decision](../decisions/20261105_companion_state_for_manifest_tracked_models.md)) is
written by the seed hook, promote, rollback and reuse sync. Still not covered after dbt-virtual-0024
paid three items (see "Paid in dbt-virtual-0024"):

1. A bare `dbt run` without `seed_incremental_clones()` in on-run-start never moves state. Kept: the
   state follows the table only where dbt-virtual runs; a hook-less run bypasses every other
   dbt-virtual step too (`require_cli_fingerprints` already refuses it when enabled), so a
   companion-only guard would be a second guard for the same mistake.
2. `_companion_state` snapshots are not versioned: a snapshot taken while an environment sat on a
   table is used for every later restore of that table, even when the table was consumed further
   in another environment without that environment saving again. Not measured; asked as a question:
   does a long-lived snapshot for a table that two environments advance independently ever restore a
   stale row? (`companion_snapshot_load` takes the newest row by `recorded_at`.)
3. A promote or rollback to a fingerprint no environment points at and nothing saved (built before
   this feature) warns and leaves the target's rows alone. Kept: deleting the rows would make the
   package replay from its start date on top of an existing table, and guessing a row from the
   table's data (`max(ts)`) is specific to each package. The warning names the model and table.
4. `companion_on_full_build: reset` deletes only the model's own rows; the package then replays
   from its start date for every model in that run (`get_run_limits`, "New Snowplow incremental
   model. Backfilling"), not only the rebuilt one. Kept: that behaviour is inside the package, not
   in dbt-virtual.
5. Environments that share one manifest table (no `env_tables`, a fixed `schema.table`) share state:
   a staging run advances what prod reads. Kept: a guard would have to know which environments are
   meant to share (single-environment projects do), so it needs a config flag; not designed yet.
6. Only the `key_column = model name` shape is handled; a manifest keyed by something else is not.
   Kept until a project needs it.
7. With `physical_schemas_by_location` (several BigQuery regions) the companion macros read
   `get_config().get('physical_schema')`, not the location of the node's registry. Not measured; no
   test covers companion state with more than one location.

## Paid in dbt-virtual-0024

- A build that fails after the seed hook moved the state: `companion_repair_failed` (called by
  `flush_registry` in on-run-end, both registry write modes) puts the environment's rows back to
  the table its pointer still names. Tests: `test_failed_build_puts_the_environments_state_back`
  (DuckDB), `test_failed_build_restores_the_state_of_the_table_the_environment_still_points_at`
  (BigQuery SQL), emulator step 27.
- `virtual_gc` deletes the `_companion_state` rows of the tables it drops (`companion_forget_tables`).
  Tests: `test_gc_deletes_saved_state_of_the_tables_it_drops` (DuckDB and BigQuery SQL), emulator step 18.
- Promote across locations (`source_database` / `source_schema`, or `physical_locations`) restored no
  state; it now reads the source environment's live rows and the rows saved in the source registry.
  Tests: `test_cross_location_promote_*` (DuckDB, BigQuery SQL), emulator step 28
  (real `CREATE TABLE ... CLONE`, emulator flipto/main a5289ad).

## Why it exists

The first version had to be correct for seed, full build, promote and rollback; these are the
cases that needed a different mechanism (a transactional multi-table swap, or state inside the
package).

## Consequence

Item 1 and 5 can leave a manifest row that describes another table than the one the environment
points at. Not measured beyond the tests in `tests/test_dbt_duckdb_companion_state.py`.

## Fix

Per item: 2 needs a measurement first; 5 needs a config flag that says which environments share a
table; 6 and 7 need a project that uses them.

## Workaround meanwhile

Use `dbt-virtual build` (it seeds before every build), keep `companion_on_full_build: error`, and
give each environment its own manifest table through `env_tables`.
