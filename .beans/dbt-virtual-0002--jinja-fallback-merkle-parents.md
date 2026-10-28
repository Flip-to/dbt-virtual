---
# dbt-virtual-0002
title: 'Upgrade pure-Jinja fingerprint fallback to include upstream dependency hashes'
status: completed
type: task
created_at: 2026-10-27T20:30:00Z
updated_at: 2026-10-27T22:48:00Z
---

Track tech debt `jinja_fingerprint_no_merkle_parents` (`docs/tech_debt/jinja_fingerprint_no_merkle_parents.md`). When running bare `dbt run` without `dbt-virtual build`, `dbt_virtual.get_fingerprint(node)` hashes both the node's own `raw_code` and config and recursively walks `node.depends_on.nodes` in `graph.nodes` to include upstream virtualized parent hashes.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Document and resolve known limitation in pure-Jinja fallback fingerprinting | `docs/tech_debt/jinja_fingerprint_no_merkle_parents.md` | 2026-10-27 |
| Built | Agent: Claude (session 8546528b-092b-4b9b-b7e2-853b31780db0), approved by Yuri Olive | Added `compute_node_self_payload` and recursive `depends_on.nodes` walk in `macros/fingerprint/get_fingerprint.sql` | commit 5390909 | 2026-10-27 |
| Proven | CI | L2: `test_jinja_merkle_dag_fallback_walks_parent_nodes` in `tests/test_macros_execution.py` + `test_real_dbt_duckdb_project_e2e` | `pytest` | 2026-10-27 |
| Reviewed | Developer: Yuri Olive | Reviewed and approved | direct push to main | 2026-10-27 |
| Shipped | CI | Pushed to `Flip-to/dbt-virtual` `main` | commit 5390909 / 4165f15 | 2026-10-27 |
| Verified | CI | All 5 CI jobs green on GitHub Actions | GitHub Actions run 40616950085 | 2026-10-27 |

## Todo

- [x] Walk `node.depends_on.nodes` inside `graph.nodes` in `macros/fingerprint/get_fingerprint.sql`
- [x] Add unit/integration test proving bare `dbt run` propagates upstream fingerprint changes
- [x] Remove `TECH_DEBT(jinja_fingerprint_no_merkle_parents)` marker and mark debt `paid`
