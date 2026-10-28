# CLI & Lifecycle Operations Reference

`dbt-virtual` can be operated either through the **`dbt-virtual` Python CLI** (recommended) or directly via **`dbt run-operation`** macros.

---

## 1. CLI Subcommands (`dbt-virtual`)

### `dbt-virtual plan`
Computes Merkle-DAG fingerprints from `target/manifest.json`, writes `target/virtual_fingerprints.json`, and classifies each virtualized node (`REUSE`, `SEED_INCREMENTAL`, `FULL_BUILD`, `ROUTINE_DEPLOY`).

```bash
dbt-virtual plan \
  --env staging \
  --base-env prod \
  --manifest target/manifest.json \
  --output target/virtual_fingerprints.json
```

| Flag | Default | Description |
|---|---|---|
| `--env` | `"staging"` | Target virtual environment name. |
| `--base-env` | `"prod"` | Reference base environment for `SEED_INCREMENTAL` detection. |
| `--manifest` | `target/manifest.json` | Path to compiled dbt manifest. |
| `--state-file` | `None` | Optional JSON file from `dbt-virtual status --output state.json` with live warehouse pointers. |
| `--schema` | `["analytics_staging", "analytics"]` | Repeatable flag for known environment schemas to strip during SQL normalization. |
| `--format` | `"text"` | Output format: `"text"` (table) or `"json"`. |

---

### `dbt-virtual build`
Computes fingerprints, syncs views for `REUSE` models in **zero compute**, and invokes `dbt build` only for models that require `FULL_BUILD`, `SEED_INCREMENTAL`, or `ROUTINE_DEPLOY`.

```bash
dbt-virtual build \
  --env staging \
  --release v2026.10.27 \
  --start 2026-10-01 \
  --end 2026-10-27
```

| Flag | Default | Description |
|---|---|---|
| `--env` | `"staging"` | Target virtual environment to build. |
| `--base-env` | `"prod"` | Base environment for incremental clone seeding. |
| `--release` | `""` | Release identifier recorded in `_releases` and `_environments`. |
| `--start` | `""` | Optional interval start timestamp/date recorded in `_intervals`. |
| `--end` | `""` | Optional interval end timestamp/date recorded in `_intervals`. |
| `--select` | `""` | Optional dbt node selector override. |
| `--dry-run` | `False` | Print the `dbt` commands that would run without executing them. |

---

### `dbt-virtual diff`
Performs AST-level and manifest-level column schema diffing between a base `manifest.json` (e.g. `prod`) and candidate `manifest.json` (`staging`), classifying every change as:
- **`NON_BREAKING`**: `COLUMN_ADDED`
- **`TYPE_WIDENED`** *(Non-breaking)*: Compatible type widening (`INT64 -> NUMERIC / FLOAT64 / BIGNUMERIC`, `DATE -> DATETIME / TIMESTAMP`, `STRING(n) -> STRING`, or synonyms like `INT == INT64`)
- **`BREAKING`**: removed models, `COLUMN_REMOVED`, or incompatible/narrowing `TYPE_CHANGED` (`FLOAT64 -> INT64`, `STRING -> INT64`)

```bash
dbt-virtual diff \
  --base-manifest prod_manifest.json \
  --curr-manifest target/manifest.json \
  --fail-on-breaking
```

---

### `dbt-virtual audit`
Runs the pre-promotion warehouse quality gate (`virtual_audit` macro), verifying that:
1. Every candidate physical table in the release is non-empty (unless `--allow-empty` is passed).
2. No model's row count dropped by more than `--max-row-drop-pct` compared to `to_env` (`prod`).

```bash
dbt-virtual audit \
  --from staging \
  --to prod \
  --release v2026.10.27 \
  --max-row-drop-pct 15.0
```

---

### `dbt-virtual promote`
Executes zero-rebuild promotion from `--from` (`staging`) to `--to` (`prod`):
1. Optionally runs `virtual_audit` when `--audit` is passed.
2. Verifies all physical tables exist in `physical_schema`.
3. Skips re-executing DDL for models/routines whose fingerprint is already active in `--to` (partial promotion).
4. Deploys updated objects in strict tier order (`Tier 1 UDFs -> Tier 2 Views -> Tier 3 TVFs -> Tier 4 Procedures`).
5. Automatically **demotes** (`DROP VIEW IF EXISTS` / `DROP FUNCTION IF EXISTS`) any object previously in `--to` that was removed from the release.

```bash
dbt-virtual promote \
  --from staging \
  --to prod \
  --release v2026.10.27 \
  --audit \
  --max-row-drop-pct 15.0
```

---

### `dbt-virtual rollback`
Instantly repoints all views and redeploys all routines in `--env` (`prod`) to the target release (or the most recent prior promoted release if `--to-release` is omitted):

```bash
# Roll back prod to the immediately preceding release
dbt-virtual rollback --env prod

# Or roll back to a specific release ID
dbt-virtual rollback --env prod --to-release v2026.10.26
```

---

### `dbt-virtual status`
Queries `_environments` and `INFORMATION_SCHEMA.TABLES` in the warehouse via `virtual_export_state` and outputs the live warehouse state as JSON or text. Can also save to `--output state.json` to feed into `dbt-virtual plan --state-file state.json`.

```bash
dbt-virtual status --env staging --base-env prod --output state.json
```

---

### `dbt-virtual init-env` & `dbt-virtual drop-env`
Bootstraps an ephemeral PR/developer environment in **0 compute** by copying view pointers from `prod`, and tears down the schema when the PR closes:

```bash
# Create analytics_pr_42 views pointing to prod's physical tables in 0 compute
dbt-virtual init-env --env pr_42 --from prod

# Drop analytics_pr_42 schema and clean up _environments rows
dbt-virtual drop-env --env pr_42
```

---

### `dbt-virtual gc`
Janitor garbage collector that finds and drops orphaned `<model>__<fp>` physical tables in `physical_schema` that are not referenced by any active environment in `_environments`, not within the last `retention_releases` releases in `_releases`, and older than `retention_days`:

```bash
# Preview orphaned physical tables
dbt-virtual gc --dry-run --retention-releases 5 --retention-days 7

# Drop orphaned physical tables
dbt-virtual gc --retention-releases 5 --retention-days 7
```

---

## 2. Direct `dbt run-operation` Macros

Every lifecycle operation can also be called directly via `dbt run-operation` without the Python CLI:

```bash
# Promote
dbt run-operation virtual_promote --args '{from_env: staging, to_env: prod, release_id: v1, audit: true}'

# Audit
dbt run-operation virtual_audit --args '{from_env: staging, to_env: prod, release_id: v1, max_row_drop_pct: 10.0}'

# Rollback
dbt run-operation virtual_rollback --args '{env: prod, to_release: v0}'

# Acquire & Release Concurrency Lock
dbt run-operation virtual_acquire_lock --args '{env: prod, owner: ci_run_99, ttl_minutes: 30}'
dbt run-operation virtual_release_lock --args '{env: prod, owner: ci_run_99}'

# Init & Drop Ephemeral Environment
dbt run-operation virtual_init_env --args '{env: pr_42, from_env: prod}'
dbt run-operation virtual_drop_env --args '{env: pr_42}'

# Garbage Collection
dbt run-operation virtual_gc --args '{dry_run: true, retention_releases: 5, retention_days: 7}'
```
