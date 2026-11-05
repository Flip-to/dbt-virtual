---
status: open
area: lifecycle
impact: low
effort: s
bean: dbt-virtual-0027
---

# REUSE pointers can carry the quality of the invocation, not of the reused release

## What

`TECH_DEBT(reuse_pointer_build_quality)` in `macros/lifecycle/guards.sql`. `build_quality()` is
computed per dbt invocation (`flags.EMPTY`, `partial_build_vars`, `_dbt_virtual_partial`). A pilot
reported that in a `--empty` run the REUSE tables (zero-compute pointers to an existing full build)
were also recorded as `build_quality = 'partial'`, so promote refuses them.

## Why it exists

The quality stamp does not know whether the table was built in this invocation. Not reproduced in
this repo: the pilot report is the only evidence (2026-11-05); the code path that stamps REUSE
pointers was not traced.

## Consequence

A REUSE pointer from a `--empty` run cannot be promoted although its table is a full build.

## Fix

A REUSE pointer keeps the `build_quality` of the release row of the reused table. Needs a test
with a `--empty` build next to a REUSE table.

## Workaround meanwhile

Run the build that is meant to be promoted without `--empty`.
