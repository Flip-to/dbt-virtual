---
status: open
area: fingerprinting
impact: medium
effort: s
bean: dbt-virtual-0002
---

# Pure-Jinja fingerprint fallback does not include upstream Merkle parent hashes

## What

In `macros/fingerprint/get_fingerprint.sql`, when `var('_dbt_virtual_fps')` is not supplied by the `dbt-virtual` CLI, the pure-Jinja fallback computes `local_md5(...)[:12]` over the node's own `raw_code` and `config`, without walking `node.depends_on.nodes` to include upstream virtualized parent fingerprints (`TECH_DEBT(jinja_fingerprint_no_merkle_parents)`).

## Why it exists

During `generate_alias_name(custom_alias_name, node)` execution at dbt parse time, `graph.nodes` is not yet fully populated for all upstream nodes in every dbt version, whereas the Python CLI pre-pass (`src/dbt_virtual/fingerprint.py`) reads the complete `target/manifest.json` after parse/compile and computes the full topological Merkle DAG.

## Consequence

Not measured in production (v1 initial scaffold). When a user runs bare `dbt run` without `dbt-virtual build`, changing an upstream virtualized model `A` will change `A`'s fingerprint, and in default `env_view` mode downstream model `B` will read `A`'s updated view on its next incremental run, but `B` will not automatically receive a fresh `<B>__<fp>` table name unless `dbt-virtual build` (or `virtual_env_epoch`) is used.

## Fix

In `macros/fingerprint/get_fingerprint.sql`, when `execute` is true and `graph.nodes` is available, recursively collect `depends_on.nodes` for virtualized parents in topological order and append their hashes to the Jinja fallback payload.

## Workaround meanwhile

Use `dbt-virtual plan` / `dbt-virtual build` (which passes the full Merkle-DAG `_dbt_virtual_fps` map to dbt), or increment `config(virtual_env_epoch=2)` on the downstream model when running bare `dbt run`.
