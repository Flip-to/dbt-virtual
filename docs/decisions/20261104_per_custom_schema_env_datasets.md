---
status: accepted
kind: developer
who: "Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: docs/specs/20261027_routing_ref_modes_and_clone_seeding.md
prs: []
---

# An environment can expose virtual objects in one schema per custom schema

## Context

`env_schemas` mapped each environment to a single schema, so every virtual view, environment
view and routine of that environment landed in it, whatever `config(schema=...)` said. Projects
that publish to several consumer-facing datasets (for example table functions in a dedicated
dataset that applications query, and marts in `<prod>_gold`) could not adopt dbt-virtual without
moving those objects and breaking their consumers. Their non-production targets usually put
everything in one dataset, so the layout differs per environment.

## Decision

- `env_schemas.<env>` is either a schema name (every custom schema collapses into it, as before)
  or a mapping `{default: <schema>, <custom_schema>: <schema>, ...}`. An unlisted custom schema
  `c` becomes `<default>_c`, the rule of dbt's default `generate_schema_name`.
- Unmapped environments (`pr_42`) keep one schema, `<base_env_default_schema>_<env>`.
- `_environments` and `_releases` record each object's custom schema in a `schema_key` column
  (added with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` on existing registries), so promote,
  rollback, demotion and drop-env know where an object lives in any environment.
- Routine DDL templates replace references to virtual objects with a placeholder that names the
  object's custom schema (`__DBT_VIRTUAL_ENV_SCHEMA_<key>__`); promotion resolves each one in the
  target environment. The single `__DBT_VIRTUAL_ENV_SCHEMA__` placeholder of older templates still
  resolves to the default schema. Code: `DECISION(per_custom_schema_env_datasets)` in
  `macros/fingerprint/get_fingerprint.sql`.

## Why

It keeps every existing configuration working (a string mapping behaves exactly as before) and
lets a project describe, per environment, the same layout its `generate_schema_name` produces, so
consumers keep querying the datasets they already use. Placeholders per object, not per schema,
are needed because in a single-dataset environment one schema holds objects that live in
different schemas in production.

## Alternatives

- Call the project's own `generate_schema_name` for the target environment: it reads `target`,
  which belongs to the current run, so it cannot compute another environment's schema during
  promote or rollback.
- One environment schema per custom schema for every environment, including PR environments:
  multiplies datasets per pull request; kept to one schema for unmapped environments instead.
- Store the full view location instead of a key: breaks when a mapping changes; the key lets
  the current mapping decide.

## Impact

- Registry tables gain `schema_key`; rows written before this change have `NULL`, which means
  the default schema, i.e. the previous behaviour.
- Virtual routines that read non-virtual models still bind to the building environment's
  location of those models; their dependencies should be virtual (or sources).
- Revisit if projects need per-environment rules that a static mapping cannot express.

## Who

Developer decision. Made by Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66) during the
2026-11-04 review, approved by Yuri Olive ("temos que arrumar tudo e permitir esse repo ficar
produtivo para a gnt").
