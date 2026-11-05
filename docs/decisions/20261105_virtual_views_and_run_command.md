---
status: accepted
kind: developer
who: "Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: docs/specs/20261027_routing_ref_modes_and_clone_seeding.md
prs: []
---

# Virtual views are registered like routines, and `dbt-virtual run` refreshes data in place

## Context

A model with `virtual_env: true` and `materialized='view'` was built by dbt in the environment
schema but never recorded in `_environments` / `_releases`, so `virtual_promote`,
`virtual_rollback`, `virtual_init_env` and demotion ignored it. Separately, an unchanged table or
incremental model is `REUSE`, so `dbt-virtual build` never re-runs it and nothing refreshed
production data on a schedule.

## Decision

- `sync_env_view` registers a virtual view through `register_view_deployment`: resource type
  `view`, the node's `schema_key`, its fingerprint, and the DDL template
  `CREATE OR REPLACE VIEW <env_schema>.<name> AS <compiled sql>` with per-object environment
  placeholders (`env_placeholders_sql`), stored like a routine template.
- Promote, rollback and init-env deploy objects in this order: environment views of tables, then
  views and routines together by DAG depth (a view can read a view or a UDF; a routine can read a
  view), then UDFs before TVFs before procedures within one depth. Demotion drops with
  `DROP VIEW`.
- The planner treats a view like a routine: `REUSE` when its fingerprint is already registered in
  the environment, otherwise `VIEW_DEPLOY` (built by `dbt build`). A view has no
  `<name>__<fp>` table, so its `physical_identifier` is its name.
- `dbt-virtual run --env <env>` runs `dbt build` over the environment's virtual table and
  incremental models with the same vars as `build` and no release. It refuses (exit 2) when a
  model's fingerprint differs from the registered one, unless `--allow-changes`.

## Why

- Views need no physical table, only a definition; the routine template mechanism already
  replays a definition in another environment and resolves references per target environment.
- Ordering by depth across views and routines is the only order that is right when a view calls a
  UDF, which "all views, then all routines" would get wrong.
- `build` (not `run`) is used by the CLI `run` so each model's tests run right after it and a
  failing test stops the models downstream from refreshing on top of bad data.
- A changed fingerprint means a new physical table, i.e. a deploy; keeping that in `build`
  keeps releases the only way code reaches production.

## Alternatives

- Rebuild views in the target environment with `dbt run` on promote: needs the project checkout
  at promote time and contradicts zero-rebuild promotion.
- Views first, then routines (strict): fails for a view that calls a UDF.
- `dbt run` for the CLI `run`: no tests between models.

## Impact

- `_environments` / `_releases` gain rows with `resource_type = 'view'`; registries without them
  behave as before.
- Views are registered when dbt builds them; a project must rebuild its views once (a release
  `build`) before promoting them. Warehouse-specific view options (BigQuery labels, expiration)
  are not part of the template.
- Revisit if views need options beyond the compiled SELECT.

## Who

Developer decision drafted by an agent subagent (session
9d590ac4-0b78-4fbf-91ee-2f18573cbc66); approved by Yuri Olive.
