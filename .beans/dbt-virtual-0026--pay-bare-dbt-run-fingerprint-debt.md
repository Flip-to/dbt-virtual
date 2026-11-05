---
# dbt-virtual-0026
title: 'Pay bare dbt run fingerprint debt (require_cli_fingerprints by default)'
status: todo
type: bug
created_at: 2026-11-05T00:00:00Z
updated_at: 2026-11-05T00:00:00Z
---

A bare `dbt run` still names virtual tables without upstream fingerprints, because dbt renders
`generate_alias_name` at parse time (`execute` False). dbt-virtual-0022 shipped the mitigations
(`guard_shared_tables` on by default, `dbt-virtual dbt` passthrough, `DBT_VIRTUAL_FPS`). Paying the
debt means making `require_cli_fingerprints` the default; see
docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Remaining part of the bare-run debt after dbt-virtual-0022 | docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md | 2026-11-05 |
| Built | pending | | pending | |
| Proven | pending | | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
