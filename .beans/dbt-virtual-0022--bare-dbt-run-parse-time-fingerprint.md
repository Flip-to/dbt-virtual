---
# dbt-virtual-0022
title: 'Guard bare dbt run against rebuilding shared physical tables'
status: todo
type: bug
created_at: 2026-11-04T00:00:00Z
updated_at: 2026-11-04T00:00:00Z
---

Without CLI `_dbt_virtual_fps`, physical names are generated at parse time without upstream
fingerprints, so a bare `dbt run` after an upstream change rebuilds a downstream physical table
in place, including one another environment points at. See
[bare_dbt_run_parse_time_fingerprint](../docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md).

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Found in the 2026-11-04 review; environments must not overwrite each other's tables | dbt-virtual-0021 review, repro in docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md | 2026-11-04 |
| Built | pending | | pending | |
| Proven | pending | | pending | |
| Reviewed | pending | | pending | |
| Shipped | pending | | pending | |
| Verified | pending | | pending | |
