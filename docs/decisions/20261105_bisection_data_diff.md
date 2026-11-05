---
status: accepted
kind: developer
who: "Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# Row-level data diff is checksum bisection, run in the warehouse

## Context

`virtual_audit` compares only row counts, so a release that changes values (or swaps rows) passes the gate. Promotion is zero-copy, so the candidate and the target are both warehouse tables that can be compared in place. A full `FULL OUTER JOIN` of two large tables reads every column of both on every diff.

## Decision

`virtual_data_diff` (macro `macros/lifecycle/data_diff.sql`, CLI `dbt-virtual diff-data`, `promote --data-diff`) compares candidate and target by checksum bisection: per-segment `COUNT(*)` plus an order-independent row-hash aggregate (partition, else whole table), split differing segments into key ranges until they hold at most `threshold` rows, then key-join only those ranges for only-in-candidate / only-in-target / changed rows, per-column mismatch counts and a sample. The row hash is dispatched per adapter with a default (`bit_xor(hash(...))`). The orchestration (levels, queue of segments) lives in the macro, in Jinja with a bounded `max_depth`; the CLI only wraps it and parses the logged `DBT_VIRTUAL_DIFF_JSON:` line. Gating is `max_mismatch_pct` per model.

## Why

Identical data costs one grouped query per side and never a join; differing data reads the full columns only for the small ranges that differ. It is the Datafold data-diff / reladiff technique, and it works with no data leaving the warehouse. Keeping it in a macro keeps the hybrid-package rule (every operation works through `run-operation` and the CLI).

## Alternatives

- Full outer join of both tables: simple, but reads every column of both tables each time. Kept only for the leaf ranges.
- Orchestrate bisection in Python: fewer Jinja limits, but not usable through `dbt run-operation`.
- Hash per row to compare in Python: moves data out of the warehouse.
- Count-only audit (existing): cannot see changed values.

## Impact

New macros, CLI subcommand and promote flags; model configs `diff_key`, `diff_exclude_columns`, `diff_where`, `diff_window_days`, `diff_partition_by`. Limits: a model needs a unique key to be drilled and range bisection needs a numeric key; `BIT_XOR` cancels pairs of identical rows; checked on DuckDB (real dbt) and as BigQuery SQL text only (mock harness), not run on BigQuery, Snowflake, Databricks or PostgreSQL. Revisit if BigQuery bytes billed for the checksum level is high or non-numeric keys are needed (hash-bucket splitting).

## Who

Developer decision. Agent: Claude (subagent of session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive.
