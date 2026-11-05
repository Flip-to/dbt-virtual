---
status: accepted
kind: developer
who: "Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# State tables that describe a virtual model's table move with that table

## Context

Snowplow's incremental packages keep one row per model name (`model`, `last_success`) in a
manifest table, and derive the input window of every tagged model from it
(`snowplow_utils` `get_incremental_manifest_status` and `get_run_limits`, read 2026-11-05 in
flipto-dbt's `dbt_packages`). A model rebuilt under dbt-virtual lives in a new table
`<model>__<fp>`, but its manifest row is keyed by model name. When the new table is built in full
(no clone source, or `--full-refresh`), the row still says "consumed up to X", so the build reads
only the window after X and drops the history, silently. Promote and rollback swap tables without
the manifest row that describes them, so a rollback to a table that lacks the last window leaves a
row that says it was consumed. Which manifest an environment reads is decided by the project
(flipto: `snowplow__manifest_schema` follows the target's schema on non-prod targets), not by
dbt-virtual.

## Decision

- `vars.dbt_virtual.companion_state` lists state tables (`table`, `key_column`, optional `tag`,
  `models`, `env_tables`) tied to virtual incremental models by model name.
- While an environment points at a fingerprint, its live rows ARE that table's state. When the
  environment's pointer moves away (seed, build, promote, rollback, reuse sync) the rows are saved
  in `<physical_schema>._companion_state` against the old fingerprint (`companion_depart`).
- The state follows the table: seeding by clone copies the base environment's rows; using a table another environment built,
  promote, rollback and reuse sync write the rows of that fingerprint (live rows
  of an environment pointing at it, else the saved ones) into the target environment's table.
- A tracked model about to be built in full (no clone source, or `--full-refresh` unless the model
  sets `full_refresh: false`) while its environment holds state for it fails the run in
  on-run-start. `companion_on_full_build: reset` deletes the rows instead, so the package
  backfills as it does for a new model.
- Code: `DECISION(companion_state_for_manifest_tracked_models)` in
  `macros/lifecycle/companion_state.sql`.

## Why

State is recorded on departure, not at the end of a run, because package `on-run-end` hooks run
after the root project's: with `on-run-end: dbt_virtual...` in the root, the snapshot read the
manifest before Snowplow's hook wrote it (measured 2026-11-04 in
`tests/test_dbt_duckdb_companion_state.py`, hook log order `regress.on-run-end.0` then
`fake_snowplow.on-run-end.1`). Live rows are correct until the pointer leaves, so nothing has to
run after the package. `env_tables` exists because dbt-virtual cannot know how a project resolves
its manifest per target.

## Alternatives

- Compiler error only (no restore): safe but leaves rollback and promote wrong; kept as the default
  for full builds (`companion_on_full_build: error`).
- Record at on-run-end: wrong because of the hook order above.
- Make the manifest itself virtual, keyed by `<model>__<fp>`: needs changes inside the package.

## Impact

Projects without `companion_state` are unaffected (`_companion_state` is created on first use).
Remaining gaps: `docs/tech_debt/companion_state_gaps.md`.

## Who

Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive.

## Prior art

SQLMesh keeps processed intervals on each snapshot, i.e. per fingerprint and not per model name
(`Snapshot.intervals` / `dev_intervals`, `add_interval`, `missing_intervals` in
`sqlmesh/core/snapshot/definition.py`, read 2026-11-05). A new version starts with no intervals and
is backfilled; a table reused by another environment brings its intervals with it. Companion state
approximates that for a package that keys its manifest by model name: per `(model, fingerprint)`
rows in `_companion_state`, copied into the manifest the environment reads. dbt-virtual's own
`_intervals` table records build windows but nothing reads it to decide a window.
