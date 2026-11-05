---
status: accepted
kind: developer
who: "Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# Registry rows are written once per run in on-run-end, not per model post-hook

## Context

Every virtual table model's post-hook (`sync_env_view`) ran a `MERGE` into the unpartitioned
`<physical_schema>._environments` and `INSERT`s into `_releases` / `_intervals`; routines did the
same in `register_routine_deployment`. With dbt `threads: 8-16` these statements run concurrently
against one table. BigQuery documents at most two mutating DML statements per table running at
once (the rest queue), and conflicting statements fail after retries with "Could not serialize
access to table ... due to concurrent update", which fails a model whose table was already built.
It also cost one extra BigQuery job per model. Not measured in this repo: no BigQuery run with
many threads was made while writing this decision (the DuckDB tests prove the batching, not the
contention).

## Decision

- `vars.dbt_virtual.registry_write_mode` is `batch` (default) or `per_model`.
- In `batch` mode the post-hook only creates or replaces the environment view (it must stay
  immediate: downstream models read it). `dbt_virtual.flush_registry(results)` (on-run-end) writes
  the registry rows of every virtual node whose result is `success`: one multi-row `MERGE` (or
  `DELETE` + `INSERT` outside BigQuery) into `_environments`, one multi-row `INSERT` into
  `_releases`, one into `_intervals` (when `--start/--end` are set), in chunks of 200 objects.
- There is no shared in-run buffer. The rows are recomputed in on-run-end from `results` (alias
  `<name>__<fingerprint>`, schema, config, compiled code of each node), the vars and the graph;
  routine DDL templates are rebuilt with `create_routine_ddl_sql` from the node's compiled code.
  An explicit `ddl` passed to `register_routine_deployment` cannot be recomputed, so that call
  still writes immediately.
- The package's `dbt_project.yml` defines the `on-run-start` / `on-run-end` hooks (dbt runs
  package hooks; checked with dbt-core 1.12.6 on run, build and compile), so projects need no
  change. If the hook is missing from the graph, `sync_env_view` logs a warning and falls back to
  per-model writes instead of losing the pointers.
- `flush_registry` is idempotent for `_releases`: it first removes the release's `candidate` rows
  for the same objects, so a second hook (a project that also lists it) does not duplicate rows.
- `virtual_sync_reused_views`, `virtual_promote`, `virtual_rollback` stay run-operations.

## Why

Computing rows in on-run-end needs no mutable state shared between threads (dbt gives each node
its own Jinja context), and a failed node simply is not in the success set, so its pointer is not
written. N concurrent statements become 3-4 sequential ones.

## Alternatives

- A shared buffer (a module object reachable from Jinja): works only by relying on dbt internals
  and is lost between processes.
- Write a per-model row into a log table and merge later: still N concurrent DML statements.
- Keep per-model writes and lower `threads`: gives up the parallelism of the run.

## Impact

- A run killed before on-run-end leaves views created but no pointers; the next run (or
  `dbt-virtual build`) writes them. `per_model` restores the old behavior.
- Custom materializations that call `register_routine_deployment` with a hand-built `ddl` keep
  immediate writes.
- Revisit when a BigQuery run with 16 threads shows whether the contention is real at the scale
  of the target projects.

## Who

Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive.
