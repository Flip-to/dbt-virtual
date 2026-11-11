---
# dbt-virtual-0040
title: 'Remove the package macro `is_incremental`, which replaced dbt''s builtin in consuming projects'
status: in-progress
type: bug
created_at: 2026-11-11T00:00:00Z
updated_at: 2026-11-11T00:00:00Z
---

`macros/lifecycle/hooks.sql` defined `is_incremental()` (a pass-through to `dbt.is_incremental()`, "kept for
backward compatibility"). A non-root package macro with a builtin's name replaces the builtin in dbt's
global namespace, so in a consuming project every incremental model's macro chain reached
`macro.dbt_virtual.is_incremental`. Against a state manifest without the package, `state:modified.macros`
selected all of them: a downstream project's Slim CI selected 393 nodes, Snowplow base models included,
and one of them tried to write a prod table under `--defer` (stopped only by IAM). Measured 2026-11-11 from
the parsed manifest: `snowplow_unified_base_sessions_lifecycle_manifest` reaches `macro.dbt_virtual.is_incremental`
through its macro chain.

Fix: drop the macro; `virtual_is_incremental()` (the documented name) returns `dbt.is_incremental()`. Release 0.2.3.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Reviewer: ktillerflipto (pre-review), via Developer: Yuri Olive | Slim CI selected the whole project after the package was installed | downstream PR pre-review, 2026-11-10 | 2026-11-11 |
| Built | Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive | drop `is_incremental`, keep `virtual_is_incremental` | branch claude/dbt-virtual-no-global-is-incremental | 2026-11-11 |
| Proven | CI | L1: `test_package_defines_no_dbt_builtin_macro_names` fails on main, passes here (local 2026-11-11) | tests/test_macros_execution.py | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending: downstream Slim CI selects only the changed nodes after repinning to v0.2.3 | |
