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
written by the seed hook, promote, rollback and reuse sync. Not covered:

1. A bare `dbt run` without `seed_incremental_clones()` in on-run-start never moves state.
2. Seeding writes the environment's rows in on-run-start, before the build. A build that fails
   leaves the environment's pointer on the old table with the new table's rows.
3. `_companion_state` rows are never collected (`virtual_gc` does not know them).
4. A promote or rollback to a fingerprint no environment points at and nothing saved (built before
   this feature) warns and leaves the target's rows alone.
5. `companion_on_full_build: reset` deletes only the model's own rows; the package then replays
   from its start date for every model in that run (`get_run_limits`, "New Snowplow incremental
   model. Backfilling"), not only the rebuilt one.
6. Environments that share one manifest table (no `env_tables`) share state: a staging run advances
   what prod reads.
7. Only the `key_column = model name` shape is handled; a manifest keyed by something else is not.

## Why it exists

The first version had to be correct for seed, full build, promote and rollback; these are the
cases that needed a different mechanism (a transactional multi-table swap, or state inside the
package).

## Consequence

Cases 1 and 2 can leave a manifest row that describes another table than the one the environment
points at. Not measured beyond the tests in `tests/test_dbt_duckdb_companion_state.py`.

## Fix

Restore state in a post-build step that runs only after the model succeeded; delete `_companion_state`
rows in `virtual_gc` together with their tables; take the pointer and the rows in one place.

## Workaround meanwhile

Use `dbt-virtual build` (it seeds before every build), keep `companion_on_full_build: error`, and
give each environment its own manifest table through `env_tables`.
