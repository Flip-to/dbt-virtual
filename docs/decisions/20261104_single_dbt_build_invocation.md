---
status: accepted
kind: developer
who: "Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# `dbt-virtual build` is one compile plus one build; hooks do the rest

## Context

`cmd_build` ran `dbt compile`, `run-operation virtual_sync_reused_views`,
`run-operation virtual_seed_incremental_clones`, `dbt build` and
`run-operation virtual_snapshot_release`: up to five dbt processes, each paying a full project
parse, which is slow on large projects.

## Decision

- The CLI passes the plan in vars of the build: `_dbt_virtual_reuse` (`{name: fingerprint}`),
  `_dbt_virtual_seed` (model names) and `_dbt_virtual_snapshot` (true with `--release`).
- `dbt_virtual.run_start()` (package on-run-start) syncs REUSE views and seeds incremental clones,
  so the clones exist before dbt decides whether an incremental table is new.
  `dbt_virtual.flush_registry(results)` (package on-run-end) writes the registry rows and, when no
  node failed, completes the release (`virtual_snapshot_release`) from the graph and
  `_dbt_virtual_fps`.
- A plan with nothing to build and no `--select` runs one
  `run-operation virtual_finalize_plan` (REUSE sync + snapshot): `dbt build` of an empty selection
  returns before any hook runs (not verified for every dbt version; a run-operation needs no such
  assumption), and an unscoped `dbt build` would build the whole project.
- The run-operation macros stay for manual use.

## Why

The hooks run inside the one process that already parsed the project.

## Alternatives

- `dbt build --select <zero-cost node>`: needs a node the project does not have.
- Keep run-operations for the empty-plan case only (chosen, as `virtual_finalize_plan`).

## Impact

- A manual `dbt build` is unchanged unless it passes those vars.
- The snapshot is skipped, with a log line, when any node errored or a test failed, as before
  (the CLI returned before snapshotting on a non-zero exit).

## Who

Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive.
