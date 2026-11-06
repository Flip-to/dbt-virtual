---
status: accepted
kind: developer
who: "Agent: Claude (session dbt-virtual-pilot-round2), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# On BigQuery, seed with CLONE until the chain would exceed 3, then with COPY

`DECISION(bigquery_clone_chain_copy)`

## Context

A real-BigQuery pilot (2026-11-05) hit "Cannot have more than 3 chained clones or snapshots". `seed_incremental_clones`
clones the base environment's current table, and after a promote that table may itself be a clone. Deploy 1
clones a regular table (depth 1), deploy 2 clones that (depth 2), deploy 3 depth 3, and deploy 4 fails.

SQLMesh avoids the problem by construction: it clones only into throwaway dev tables (`_can_clone` /
`_clone_snapshot_in_dev` in its evaluator), and prod is never a clone, so chains stay at depth 1. dbt-virtual
promotes the seeded table itself (the view swap points prod at it), so prod tables are clones and chains grow.

## Decision

On BigQuery (not with `use_ctas_for_clone`, not on the emulator), `clone_or_copy_table` reads the clone depth of the
source table (`INFORMATION_SCHEMA.TABLES`: `table_type = 'CLONE'`, following `base_table_*`, at most
`bigquery_max_clone_chain()` = 3 hops; depth 0 is a regular table):

- depth + 1 <= 3: `CREATE TABLE ... CLONE` as before, and the depth is logged.
- depth + 1 > 3: `CREATE TABLE ... COPY` (a copy job: no bytes scanned, partitioning, clustering and options kept),
  which is independent (depth 0). Logged with the depth.
- depth unknown: CLONE, and rely on the backstop below.

Backstop: `bigquery__clone_table_sql` wraps the CLONE in a script, `EXCEPTION WHEN ERROR`, and on a message containing
"chained clones" runs the COPY; any other error is raised again. `vars.dbt_virtual.clone_chain_fallback: false`
turns both off. Other adapters are unchanged (Snowflake and Databricks have no such limit that we know of: not verified).

## Why

A CLONE bills only the bytes that change; a COPY duplicates storage for as long as the previous release is
retained, and takes time that grows with table size. Copying only when the chain is full gives zero-copy deploys
most of the time and one COPY about every third deploy of a model. The storage cost of that COPY ends when the
previous release is garbage collected.

## Alternatives

- Always COPY a clone source: depth never above 1, but every seeded deploy of a clone pays the duplicate storage and
  copy time. Rejected on cost.
- Only the error backstop: works, but the failed CLONE is a wasted job and the log cannot say which path ran.
  Kept as the backstop for an unreadable depth.
- CTAS: scans the table (bytes billed) and loses partitioning and options unless repeated. Rejected.
- Never promote a clone (SQLMesh's way): needs a rebuild or copy at promote, which defeats the zero-rebuild promote.

## Impact

One metadata query per seeded table on BigQuery (up to 3 hops). `base_table_catalog/schema/name` and `table_type =
'CLONE'` are taken from BigQuery's documented `INFORMATION_SCHEMA.TABLES` columns: not verified against a real
warehouse in this change, and the emulator reports clones as `BASE TABLE`, has no `base_table_*` columns and rejects
`CREATE TABLE ... COPY`, so the depth and COPY paths are covered by macro harness tests only. If the columns differ,
the depth query fails and `clone_chain_fallback: false` is the workaround; revisit then.

## Who

Developer decision: Yuri Olive (2026-11-05), after reading SQLMesh's clone code. Implemented by Agent: Claude
(session dbt-virtual-pilot-round2), approved by Yuri Olive.
