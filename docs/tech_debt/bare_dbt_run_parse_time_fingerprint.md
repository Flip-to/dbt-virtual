---
status: open
area: fingerprinting
impact: high
effort: m
bean: dbt-virtual-0022
---

# Bare `dbt run` names physical tables without upstream fingerprints

## What

Without `dbt-virtual build` (which passes `_dbt_virtual_fps`), the physical name `<model>__<fp>`
comes from `virtual_alias_name`, which dbt calls while parsing. dbt runs `generate_alias_name`
with `execute = False` (`GenerateNameProvider.execute = False`, dbt-core 1.12.6
`dbt/context/providers.py:1083-1084`), so `get_fingerprint` skips the parent walk and the name
only reflects the model's own SQL and config (`TECH_DEBT(bare_dbt_run_parse_time_fingerprint)`
in `macros/routing/virtual_routing.sql`).

## Why it exists

The parent walk added for [jinja_fingerprint_no_merkle_parents](jinja_fingerprint_no_merkle_parents.md)
runs only at execution time. At parse time dbt has not resolved `depends_on` or compiled the
parents, so pure Jinja cannot compute a Merkle fingerprint for the alias.

## Consequence

Measured 2026-11-04 with dbt-core 1.12.6 + dbt-duckdb 1.11.1 (two table models `up` and
`down = select x from ref('up')`, bare `dbt run --target staging` twice, changing `up` in
between): `up` moved to a new table (`up__04307bd8a7d7` → `up__6ae5353118e8`) but `down` stayed
`down__82dcf1d23a15` and was rebuilt in place. If another environment (e.g. prod) points at that
physical table, a bare run in staging changes the data prod reads.

## Fix

Options, not decided: make bare runs refuse to rebuild a physical table that another environment
points at when the run-time Merkle fingerprint differs from the one recorded for it; or require
`_dbt_virtual_fps` for virtual table models and fail fast without it.

## Workaround meanwhile

Build virtual models only through `dbt-virtual build` (it passes `_dbt_virtual_fps`, so the alias
includes upstream fingerprints). For a bare `dbt run`, bump `config(virtual_env_epoch=...)` on
downstream models of a changed model.
