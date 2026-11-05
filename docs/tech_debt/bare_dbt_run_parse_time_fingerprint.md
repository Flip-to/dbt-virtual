---
status: open
area: fingerprinting
impact: medium
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

## Status 2026-11-05: still open, impact reduced

A bare `dbt run` (no `_dbt_virtual_fps`, no `DBT_VIRTUAL_FPS`) still names `down__<own hash>`:
dbt evaluates `generate_alias_name` at parse with `execute = False`, before `depends_on` is
resolved, so no Jinja or hook can put parent fingerprints in that name. That cannot be fixed inside
dbt. What exists now (tests in `tests/test_dbt_passthrough.py`, run 2026-11-05, dbt-core 1.12.6 +
dbt-duckdb 1.11.1):

- `dbt-virtual dbt <dbt args>` computes the fingerprints in Python and passes `_dbt_virtual_fps`:
  `test_passthrough_builds_the_same_physical_names_as_dbt_virtual_build`.
- `export DBT_VIRTUAL_FPS=$(dbt-virtual plan --print-fps)` then plain `dbt run` names tables with
  parent fingerprints: `test_env_var_fingerprints_make_plain_dbt_run_use_merkle_names` (asserts the
  same run without it does not).
- `guard_shared_tables` (default true) refuses a bare run that would rewrite a table another
  environment points at with a different fingerprint:
  `test_guard_refuses_bare_run_that_would_rewrite_a_table_prod_points_at`; allowed when parents are
  unchanged or only the current environment uses the table.

It stays open because a bare run with no other environment pointing at the tables still misnames
them (an in-place rebuild in dev, no data of another environment changes), and nothing forces users
onto the passthrough or the env var except `require_cli_fingerprints`. Pay it by making
`require_cli_fingerprints` the default.

## Fix

Options, not decided: make bare runs refuse to rebuild a physical table that another environment
points at when the run-time Merkle fingerprint differs from the one recorded for it; or require
`_dbt_virtual_fps` for virtual table models and fail fast without it.

The second option now exists as an opt-in flag, `vars.dbt_virtual.require_cli_fingerprints`
(default `false`), checked by `assert_cli_fingerprints()` in `macros/lifecycle/hooks.sql`. It
does not pay this item: with the default, a bare `dbt run` still behaves as described above.
Paying it means either turning the flag on by default or the first option.

## Workaround meanwhile

Preferred: `dbt-virtual dbt <args>` or `DBT_VIRTUAL_FPS` (see docs/guides/cli_and_operations.md).

Build virtual models only through `dbt-virtual build` (it passes `_dbt_virtual_fps`, so the alias
includes upstream fingerprints). For a bare `dbt run`, bump `config(virtual_env_epoch=...)` on
downstream models of a changed model.

To enforce this, set `require_cli_fingerprints: true` under `vars.dbt_virtual` and use the
`"{{ dbt_virtual.on_run_start() }}"` on-run-start hook (`seed_incremental_clones()` as the hook
runs the same check). A `dbt run` / `dbt build` / `dbt retry` whose selection includes a virtual
table or incremental model then fails in on-run-start, before anything is built, unless
`_dbt_virtual_fps` is set. `dbt compile`, `parse`, `ls`, `docs generate` and `run-operation` are
not affected. Evidence (dbt-core 1.12.6 + dbt-duckdb 1.11.1, run 2026-11-04), in
`tests/test_require_cli_fingerprints.py`:

- `test_bare_dbt_run_fails_before_building` (both hooks) and `test_bare_dbt_build_fails_too`:
  non-zero exit, the error names the flag and `dbt-virtual build`, no physical table and no env
  view exist. Against the code before the guard (commit `b390217`) the bare run exited 0.
- `test_commands_that_do_not_build_are_not_guarded`: `compile`, `parse`, `ls`, `docs generate`.
- `test_bare_run_of_only_non_table_virtual_models_is_allowed`: selecting only a virtual view.
- `test_build_with_cli_fingerprints_succeeds`: `dbt build` with `_dbt_virtual_fps` builds.
- `test_flag_off_keeps_bare_dbt_run_working`: the default keeps today's behaviour.
