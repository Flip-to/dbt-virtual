---
status: accepted
kind: developer
who: "Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: docs/specs/20261027_routing_ref_modes_and_clone_seeding.md
prs: []
---

# `ref_resolution: explicit` is rejected with a clear error

## Context

`docs/decisions/20261027_default_ref_resolution_env_view.md` listed Option D, `explicit`: standard
`ref()` untouched, callers invoke `dbt_virtual.virtual_ref('A')` when they want the environment
view. `macros/routing/virtual_routing.sql` only rewrote relations in `env_view` and `hybrid`, so
`explicit` returned the physical relation, exactly like `physical`
(`docs/guides/configuration_reference.md` said so). A project that set it got no per-call control.

## Decision

- `explicit` and any other unknown `ref_resolution` value raise a compile error naming the
  supported values (`env_view`, `physical`, `hybrid`). It is checked in `virtual_ref`, so
  `dbt parse` fails too, not only `dbt run`.
- To keep today's behaviour a project sets `physical`.

## Why

- The intent is not implementable with the current wiring: the project's `ref()` override
  (`docs/guides/getting_started.md`) calls `dbt_virtual.virtual_ref`, which is also the macro an
  explicit caller would call. Jinja cannot tell the two call sites apart, so one macro cannot be
  "untouched" for the override and "env view" for the explicit call.
- A mode that silently behaves like another one is worse than an error: a typo such as
  `enviroment` also fell through to the physical relation.

## Alternatives

- Add a second macro (for example `virtual_env_ref`) that always resolves to the environment view
  and make `virtual_ref` return the raw relation in `explicit` mode. Possible, but it is new API
  nobody asked for; revisit if a project needs per-call control (question for the owner: does any?).
- Keep `explicit` as an alias of `physical` and only fix the docs. Rejected: keeps the silent
  fall-through for typos.

## Impact

- Projects that set `ref_resolution: "explicit"` fail at parse until they set `physical`.
- Test: `tests/test_dbt_duckdb_small_gaps.py` (real dbt-duckdb): `explicit` and `enviroment` fail
  `dbt parse` with the message; before the change `dbt parse` returned 0 (run 2026-11-05).

## Who

Developer decision drafted by an agent subagent (session
9d590ac4-0b78-4fbf-91ee-2f18573cbc66); approved by Yuri Olive.
