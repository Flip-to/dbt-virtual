---
# dbt-virtual-0002
title: 'Upgrade pure-Jinja fingerprint fallback to include upstream dependency hashes'
status: todo
type: task
created_at: 2026-10-27T20:30:00Z
updated_at: 2026-10-27T20:30:00Z
---

Track tech debt `jinja_fingerprint_no_merkle_parents` (`docs/tech_debt/jinja_fingerprint_no_merkle_parents.md`). When running bare `dbt run` without `dbt-virtual build`, `dbt_virtual.get_fingerprint(node)` hashes only the node's own `raw_code` and config, not its upstream virtualized parents.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Document and track known v1 limitation in pure-Jinja fallback fingerprinting | `docs/tech_debt/jinja_fingerprint_no_merkle_parents.md` | 2026-10-27 |
| Built | pending | | | |
| Proven | pending | | | |
| Reviewed | pending | | | |
| Shipped | pending | | | |
| Verified | pending | | | |

## Todo

- [ ] Walk `node.depends_on.nodes` inside `graph.nodes` in `macros/fingerprint/get_fingerprint.sql`
- [ ] Add unit/integration test proving bare `dbt run` propagates upstream fingerprint changes
- [ ] Remove `TECH_DEBT(jinja_fingerprint_no_merkle_parents)` marker and mark debt `paid`
