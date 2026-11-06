# CLI & Lifecycle Operations Reference

`dbt-virtual` can be operated either through the **`dbt-virtual` Python CLI** (recommended) or directly via **`dbt run-operation`** macros.

Every subcommand accepts the global flag `--project-dir <path>` (default: current directory) before the subcommand name, e.g. `dbt-virtual --project-dir ./analytics plan --env staging`.

!!! note "`--env` is also the dbt target for `plan` and `build`"
    `plan` and `build` pass `--target <env>` to every dbt command they run, so `profiles.yml` needs an output with the environment's name (`staging`, `prod`, `pr_42`, ...). The other subcommands run `dbt run-operation` with your profile's default target. For a PR environment, write an output named after it in the CI job's `profiles.yml`.

---

## 1. CLI Subcommands (`dbt-virtual`)

### `dbt-virtual plan`
Runs `dbt compile --target <env>` (unless `--skip-compile`), computes Merkle-DAG fingerprints from `target/manifest.json`, writes `.dbt_virtual/fingerprints.json`, and classifies each virtualized node (`REUSE`, `SEED_INCREMENTAL`, `FULL_BUILD`, `ROUTINE_DEPLOY`).

```bash
dbt-virtual status --env staging --base-env prod --output state.json
dbt-virtual plan --env staging --state-file state.json
```

| Flag | Default | Description |
|---|---|---|
| `--env` | `"staging"` | Target virtual environment name (also used as the dbt `--target`). |
| `--state-file` | `None` | JSON file from `dbt-virtual status --output state.json` with live warehouse pointers. Without it, nothing is classified `REUSE` or `SEED_INCREMENTAL`. A physical table is only `REUSE` when it exists **and** the registry points at it (`registered_physical_tables`), so a table left by a failed build is rebuilt. |
| `--skip-compile` | `False` | Reuse the existing `target/manifest.json`. |
| `--known-schema` | none | Repeatable. Extra schema names to strip from SQL before hashing. Rarely needed now that ref locations are stripped automatically; see [How It Works](how_it_works.md) for when it still is. |
| `--format` | `"text"` | Output format: `"text"` or `"json"`. |

The base environment for `SEED_INCREMENTAL` detection is the `base_env_state` of the state file (`status --base-env`); there is no `--base-env` flag on `plan`.

---

### `dbt-virtual build`
Computes fingerprints, syncs views for `REUSE` models in **zero compute**, seeds incremental clones, invokes `dbt build` only for models that require `FULL_BUILD`, `SEED_INCREMENTAL`, or `ROUTINE_DEPLOY`, and completes the release.

```bash
dbt-virtual build \
  --env staging \
  --state-file state.json \
  --release v2026.10.27 \
  --start 2026-10-01 \
  --end 2026-10-27
```

What it runs, in order. Every dbt call gets `--target <env>` and the same `--vars` (`_dbt_virtual_fps`, `_dbt_virtual_env`, `_dbt_virtual_release`, and `_dbt_virtual_start` / `_dbt_virtual_end` when set), so node schemas and aliases match what dbt builds:

1. `dbt compile` (unless `--skip-compile`), fingerprints, plan.
2. `dbt run-operation virtual_sync_reused_views` for `REUSE` tables: re-points their environment views and pointers.
3. `dbt run-operation virtual_seed_incremental_clones` for `SEED_INCREMENTAL` models: clones the base environment's table before dbt starts.
4. `dbt build --select <models to build>` (or your `--select`).
5. `dbt run-operation virtual_snapshot_release` when `--release` is set: adds every other virtual node's current pointer to the release, so the release describes the whole environment.

If nothing needs building and no `--select` is given, step 4 is skipped (no unscoped `dbt build`).

| Flag | Default | Description |
|---|---|---|
| `--env` | `"staging"` | Target virtual environment to build (also used as the dbt `--target`). |
| `--release` | `""` | Release identifier recorded in `_releases` and `_environments`. |
| `--state-file` | `None` | Warehouse state from `dbt-virtual status --output`, used to skip `REUSE` models. |
| `--select` | `None` | dbt `--select` expression that overrides the planned selection. |
| `--full-refresh` | `False` | Plan every table/incremental model as `FULL_BUILD` and pass `--full-refresh` to dbt. |
| `--start` | `None` | Optional interval start timestamp/date recorded in `_intervals`. |
| `--end` | `None` | Optional interval end timestamp/date recorded in `_intervals`. |
| `--skip-compile` | `False` | Reuse the existing `target/manifest.json`. |
| `--known-schema` | none | Same as on `plan`. |

The clone source is `vars.dbt_virtual.base_environment`; `build` has no `--base-env` or `--dry-run` flag. Use `dbt-virtual plan` to preview.

---

### `dbt-virtual dbt <dbt args...>` (passthrough)

Runs any dbt command with the Merkle fingerprints computed in Python, so a plain `build` / `run` / `test` / `ls` / `seed` / `snapshot` / `compile` / `run-operation` names physical tables exactly like `dbt-virtual build` does. dbt generates aliases while parsing (`execute` is False), where Jinja cannot see parents, so only fingerprints passed in from outside can put them in a name ([tech debt](../tech_debt/bare_dbt_run_parse_time_fingerprint.md)).

```bash
dbt-virtual dbt build --target staging --select state:modified+ --vars '{start_date: 2026-01-01}'
dbt-virtual dbt --env pr_42 run --target ci          # --env sets _dbt_virtual_env
```

- It compiles once with the same `--target` / `--vars` / `--profiles-dir` (never your selection), computes the fingerprints, and runs dbt with `_dbt_virtual_fps` merged into your `--vars` (your vars are kept; dbt-virtual's win on a clash). `--env NAME` and `--no-cache` go right after `dbt`; everything after is passed to dbt untouched.
- `.dbt_virtual/fingerprints.json` is reused (no compile) while the project files, target, vars, profiles, `DBT_*` environment and the manifest checksum are unchanged. `--no-cache` forces a compile. Status lines go to stderr, so `dbt ls --output json` stays parseable on stdout.
- `deps`, `clean`, `init`, `debug` and `parse` run as-is. dbt's exit code is returned unchanged.
- Docker / CI entrypoint: `ENTRYPOINT ["dbt-virtual", "dbt"]`, then `docker run image build --target prod`. Without the CLI, use the environment variable below.

### Plain `dbt run` through an environment variable

```bash
export DBT_VIRTUAL_FPS=$(dbt-virtual plan --env staging --print-fps)   # compact JSON {model: fingerprint}
dbt run --target staging
```

`--print-fps` prints only the JSON on stdout (dbt's compile output stays off it). `get_fingerprint` reads `DBT_VIRTUAL_FPS` when `_dbt_virtual_fps` is not set, and `env_var` is allowed while dbt parses, so aliases include parent fingerprints. `require_cli_fingerprints` accepts this source. Recompute it whenever a model changes: a stale value names tables from the old fingerprints.

### `dbt-virtual diff`
Performs AST-level and manifest-level column schema diffing between a base `manifest.json` (e.g. `prod`) and a candidate `manifest.json` (`staging`). Each column change is `ADDED`, `DROPPED`, `TYPE_WIDENED` (compatible widening such as `INT64 -> NUMERIC / FLOAT64 / BIGNUMERIC`, `DATE -> DATETIME / TIMESTAMP`, `STRING(n) -> STRING`; synonyms like `INT == INT64` are not changes) or `TYPE_CHANGED`. A model is **`BREAKING`** when it was removed or has a `DROPPED` / `TYPE_CHANGED` column, otherwise **`NON_BREAKING`**.

```bash
dbt-virtual diff \
  --base-manifest prod_manifest.json \
  --target-manifest target/manifest.json \
  --fail-on-breaking
```

`--target-manifest` defaults to `target/manifest.json`; `--fail-on-breaking` exits with code `2` on breaking changes; `--format json` prints JSON.

---

### `dbt-virtual audit`
Runs the pre-promotion warehouse quality gate (`virtual_audit` macro), verifying that:
1. Every candidate physical table in the release is non-empty (unless `--allow-empty` is passed).
2. No model's row count dropped by more than `--max-row-drop-pct` compared to `--to` (`prod`).

On BigQuery the row counts come from the dataset's `__TABLES__` metadata, because `COUNT(*)` fails on tables created with `require_partition_filter = true`; other adapters use `COUNT(*)`.

```bash
dbt-virtual audit \
  --from staging \
  --to prod \
  --release v2026.10.27 \
  --max-row-drop-pct 15.0
```

---

### `dbt-virtual diff-data`
Row-level diff of each model's candidate table (`--from`, or `--release`) against the table `--to` points at (`virtual_data_diff` macro). It reports rows only in the candidate, rows only in the target and changed rows, with per-column mismatch counts, the segments that were drilled and a sample of keys.

```bash
dbt-virtual diff-data --from staging --to prod --release v2026.11.05 \
  --select orders+ --max-mismatch-pct 2.0 --format json
```

| Flag | Default | Description |
|---|---|---|
| `--from` / `--to` / `--release` | `staging` / `prod` / none | Candidate and baseline; `--release` diffs that release instead of `--from`. |
| `--select` | all | dbt selector limiting the models. |
| `--max-mismatch-pct` | none (report only) | Exit non-zero when a model's mismatch percentage is higher. |
| `--threshold` | `10000` | A range with at most this many rows is joined instead of split. |
| `--branching` / `--max-depth` | `16` / `8` | Sub-ranges per split and the maximum number of splits down one path. |
| `--sample-limit` | `10` | Sample rows per model. |
| `--exclude-columns` | none | Comma separated columns to ignore (load timestamps). Adds to each model's `diff_exclude_columns`. |
| `--where` / `--window-days` | none | Bound both sides (`--window-days N` keeps the last N days of the partition column). |
| `--format` | `text` | `json` prints the summary that the macro logs as `DBT_VIRTUAL_DIFF_JSON:{...}`. |

Per-model config (in `config()` or `meta`): `unique_key` or `diff_key` (a string or a list: every entry is joined on, so composite keys work; range bisection uses the first numeric entry, e.g. `id` for `[event_date, id]`; with no numeric entry the differing segments are joined whole, which is slower on big partitions; the summary reports `bisect_key`), `diff_exclude_columns`, `diff_where`, `diff_window_days`, and `diff_partition_by` (`{field, data_type}`) when `partition_by` is not set (BigQuery's `partition_by` is used otherwise). A model without a key is checksummed per segment only: it is reported as differing with an upper bound, without row detail.

`promote --data-diff [--max-mismatch-pct X]` runs the same diff against `--to` after the audit and before repointing anything; above the threshold the promotion stops.

Cost: see "Data diff" in [How it works](how_it_works.md).

---

### `dbt-virtual promote`
Executes zero-rebuild promotion from `--from` (`staging`) to `--to` (`prod`):
1. Compiles the project for `--to` and passes the expected fingerprints to `virtual_promote` (see the guards below).
2. Takes the lease lock on `--to` in `_locks` (skip with `--no-lock`); it is released when the promote finishes, is refused or fails (the CLI releases it after a failed run-operation, so a retry needs no `--no-lock`).
3. **Refuses, before changing anything**, a release that is partial, unsealed or built from other code (guards below).
4. Runs `virtual_audit` (row counts and, on BigQuery, partition coverage) when `--audit` is on; it is on by default when `--to` is the base environment.
5. Verifies all physical tables of the release exist in `physical_schema`.
6. Skips objects whose fingerprint is already active in `--to` (partial promotion).
7. Deploys **environment views first**, then **routines in DAG order** (each after the views and routines it reads; at equal depth UDFs, then table functions, then procedures), each into the target environment's schema for its custom schema.
8. **Demotes** (`DROP VIEW` / `DROP FUNCTION` / ...) objects active in `--to` that are not in the release **and** are no longer virtual nodes in the project.

**Promotion guards** (an incomplete table never reaches `--to`):

- **Fingerprint**: every release row must have the fingerprint of the code compiled for `--to` (a windowed build renders other SQL, so it and its descendants differ). Macro argument `expected_fps_json`, always set by the CLI.
- **Partial**: `sync_env_view` marks rows `build_quality = 'partial'` when dbt runs with `--empty`, when a var listed in `vars.dbt_virtual.partial_build_vars` is set, or when `_dbt_virtual_partial: true`. Partial rows are refused, and `plan` / `build` never reuse or clone a partial table (`status --output` exports `partial_physical_tables`).
- **Sealed**: `dbt-virtual build --release` seals the release (`_releases.sealed_at`) only after `dbt build` succeeded. An unsealed release is refused unless `--allow-unsealed`. A release created before sealing existed has no seal; it is refused while `--require-sealed` is on (default). After checking its tables, seal it with `dbt run-operation virtual_seal_release --args '{release_id: v1}'` or pass `--no-require-sealed`.
- **Coverage** (BigQuery, part of `--audit`): the candidate must have every partition the current target table has, except `__NULL__`, `__UNPARTITIONED__` and partitions older than `--coverage-ignore-before` (or `vars.dbt_virtual.coverage_ignore_before`). Missing ranges are reported. Other adapters skip it; `--no-coverage-check` disables it.

```bash
dbt-virtual promote \
  --from staging \
  --to prod \
  --release v2026.10.27 \
  --audit \
  --max-row-drop-pct 15.0
```

| Flag | Default | Description |
|---|---|---|
| `--from` / `--to` | `staging` / `prod` | Source and target environments. |
| `--release` | required | Release to promote. |
| `--audit` / `--no-audit` | on when `--to` is the base environment | Run the `audit` gate first. |
| `--max-row-drop-pct` | `100.0` | Row-drop threshold for `--audit`. |
| `--no-coverage-check` / `--coverage-ignore-before` | off / unset | Coverage part of the audit (BigQuery). |
| `--skip-fingerprint-check` | `False` | **Dangerous**: do not compare fingerprints with the code of `--to`. A windowed, `--empty` or other-branch build can then be promoted. |
| `--compile-target` / `--skip-compile` / `--known-schema` | `--to` / `False` / none | How the expected fingerprints are computed. |
| `--require-sealed` / `--no-require-sealed` | on | Also refuse releases that predate sealing. |
| `--allow-unsealed` | `False` | **Dangerous**: promote a release whose build did not finish. |
| `--data-diff` | `False` | Run the row-level `diff-data` against `--to` first. |
| `--max-mismatch-pct` | none | With `--data-diff`: stop when a model's mismatch percentage is higher. |
| `--no-lock` | `False` | Do not take the lease lock. |
| `--lock-owner` | `dbt-virtual-cli:<user>@<host>` | Lease owner. The default is stable per user and machine, so a promote that failed while holding the lock can be retried right away; in CI pass the job's own lock owner if it already holds the lock. |

---

### `dbt-virtual rollback`
Instantly repoints all views and redeploys all routines in `--env` (`prod`) to the target release (or the previous promoted release if `--to-release` is omitted). It takes the same lease lock as `promote` (`--no-lock`, `--lock-owner`) and first checks that every physical table of the target release still exists, failing before any change if `gc` dropped one. A legacy table left at the environment path by `adopt --keep-existing-table` is replaced by the view; the lock is released if the rollback fails. Undone releases are marked `rolled_back`, so running it again goes one more release back:

```bash
# Roll back prod to the immediately preceding release
dbt-virtual rollback --env prod

# Or roll back to a specific release ID
dbt-virtual rollback --env prod --to-release v2026.10.26
```

---

### `dbt-virtual status`
Queries `_environments` and the physical schema's table list via `virtual_export_state` and prints the live warehouse state. `--output state.json` saves it for `plan --state-file` / `build --state-file`.

```bash
dbt-virtual status --env staging --base-env prod --output state.json
```

---

### `dbt-virtual init-env` & `dbt-virtual drop-env`
Bootstraps an ephemeral PR/developer environment in **0 compute** by promoting `--from`'s active pointers (views and routines) into it, and tears it down when the PR closes:

```bash
# Create analytics_pr_42 views pointing to prod's physical tables in 0 compute
dbt-virtual init-env --env pr_42 --from prod

# Drop the schema(s) of pr_42 and its _environments rows
dbt-virtual drop-env --env pr_42
```

`drop-env` drops every schema the environment used (one per custom schema for a mapped environment) with `CASCADE`, and refuses to run when the environment is `base_environment` or when one of those schemas is `physical_schema` or belongs to another environment listed in `env_schemas`. Physical tables are never dropped; `gc` handles them.

---

### `dbt-virtual gc`
Janitor garbage collector that finds and drops orphaned `<model>__<fp>` physical tables in `physical_schema` that are not referenced by any active environment in `_environments`, not within the last `retention_releases` releases **of any environment** in `_releases`, and older than `retention_days`:

```bash
# Preview orphaned physical tables
dbt-virtual gc --dry-run --retention-releases 5 --retention-days 7

# Drop orphaned physical tables
dbt-virtual gc --retention-releases 5 --retention-days 7
```

---

### `dbt-virtual adopt`
Zero-compute migration of an existing deployment: runs `dbt compile`, computes fingerprints, and for every virtual `table` / `incremental` model clones `<env_schema>.<model>` into `<physical_schema>.<model>__<fp>`, replaces the table with a view (unless `--keep-existing-table`), and registers it. Routines are not adopted; the first `build` + `promote` registers them. See [Migrating an Existing Project](migrating_existing_project.md).

```bash
dbt-virtual adopt --env prod --release v_initial_adopt [--keep-existing-table] [--skip-compile] [--known-schema <schema>]
```

---

## 2. Direct `dbt run-operation` Macros

Every lifecycle operation can also be called directly via `dbt run-operation` without the Python CLI:

```bash
# Promote (the macro takes the lock only when asked: lock: true, lock_owner: <name>)
dbt run-operation virtual_promote --args '{from_env: staging, to_env: prod, release_id: v1, audit: true, lock: true}'

# Audit
dbt run-operation virtual_audit --args '{from_env: staging, to_env: prod, release_id: v1, max_row_drop_pct: 10.0}'

# Row-level data diff (logs DBT_VIRTUAL_DIFF_JSON:{...}; fails above max_mismatch_pct)
dbt run-operation virtual_data_diff --args '{from_env: staging, to_env: prod, models: [orders], threshold: 10000, max_mismatch_pct: 2.0}'

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

# Export state (prints a DBT_VIRTUAL_STATE_JSON:{...} log line)
dbt run-operation virtual_export_state --args '{env: staging, base_env: prod}'
```

The steps `dbt-virtual build` runs around `dbt build` are macros too. When you call them yourself, pass the same `--target` and `--vars` as the `dbt build` they belong to:

```bash
# Before dbt build: clone base tables of changed incremental models
dbt run-operation virtual_seed_incremental_clones --target staging \
  --args '{model_names: [orders_metrics]}' --vars "$VARS"

# After dbt build: complete the release with the environment's unchanged objects
dbt run-operation virtual_snapshot_release --target staging \
  --args '{env: staging, release_id: v1, models_json: {orders_metrics: {materialized: incremental}}}' --vars "$VARS"
```

## Upgrading from 0.1.0

Switch `packages.yml` to the new version, run `dbt deps`, and use the CLI as before. Verified by
`tests/test_dbt_duckdb_upgrade_v010.py` (real dbt on DuckDB) and `scripts/run_emulator_concurrency_upgrade_e2e.py`
(BigQuery emulator), which build and promote with the `v0.1.0` tag and then continue with the current code.

- **The registry migrates itself.** A 0.1.0 registry has no `schema_key`, `build_quality` or `sealed_at`.
  `status`, `build`, `promote`, `rollback` and `drop-env` add them with `ADD COLUMN IF NOT EXISTS` the
  first time they touch the registry (`migrate_registry_if_old`). Releases written by 0.1.0 stay readable
  and `rollback --to-release <old release>` works. They carry no seal, so `--require-sealed` refuses them
  until you run `virtual_seal_release` for the release.
- **Fingerprints changed since 0.1.0** (they no longer depend on the target's datasets), so the first
  `dbt-virtual build` after the upgrade names new physical tables (the test rebuilt both `table` models).
  To keep a 0.1.0 table instead, set `physical_version` on the model to its current fingerprint
  (`dbt-virtual status` shows it); the build then reuses it with no compute and creates no table.
  For the same reason `dbt-virtual promote` refuses to re-promote a release built by 0.1.0 (its
  fingerprints differ from the current code's); not measured: `incremental` models across the upgrade.
