---
name: dbt-virtual-ops
description: Operate day-to-day dbt-virtual virtual environment workflows — plan, build, schema diff, pre-promotion audit, zero-rebuild promote, instant rollback, concurrency locks, ephemeral PR environments (`init-env`/`drop-env`), and janitor garbage collection (`gc`).
user-invocable: true
allowed-tools: Bash, Read, Grep, Glob, Edit
---

# Operate `dbt-virtual` Lifecycle & Release Workflows

Canonical references (maintain DRY — consult these guides for full CLI flags and macro arguments):
- CLI & `dbt run-operation` Reference: `docs/guides/cli_and_operations.md`
- CI/CD & Production Workflows: `docs/guides/ci_cd_workflows.md`
- Routines (`table_function`, `virtual_function`, `procedure`) & `tvf_test_ref`: `docs/guides/routines_and_tvf_testing.md`
- How fingerprints, releases and promotion work: `docs/guides/how_it_works.md`

## Rules of thumb

- Build virtual models with `dbt-virtual build`, not bare `dbt run` / `dbt build`: a bare run has no CLI fingerprints, so physical names miss upstream changes (`docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md`). `vars.dbt_virtual.require_cli_fingerprints: true` makes such runs fail in `on-run-start`.
- `plan` and `build` pass `--env` to dbt as `--target`: `profiles.yml` needs an output with the environment's name. Other subcommands use the profile's default target.
- `plan` / `build` have no `--base-env`; the clone source is `vars.dbt_virtual.base_environment`, and `status --base-env` fills the state file.
- Never select an environment with `--vars "{dbt_virtual: {environment: ...}}"`: it replaces the whole `dbt_virtual` mapping.

## Standard Workflows

1. **Inspect Live State & Plan (`status` + `plan`)**:
   ```bash
   dbt-virtual status --env staging --base-env prod --output state.json
   dbt-virtual plan --env staging --state-file state.json
   ```
   - `plan` compiles the project itself (`--skip-compile` to reuse `target/manifest.json`) and writes `.dbt_virtual/fingerprints.json`.
   - Without `--state-file` nothing is `REUSE`; on a brand-new project the registry tables `status` reads do not exist until the first build, so skip `status` for the first build.
   - `REUSE`: Physical table `<model>__<fp>` already exists, or the routine's fingerprint is already active (0 compute; only view pointer synced).
   - `SEED_INCREMENTAL`: Cloned from `prod` via zero-copy `CLONE` (`seed_incremental_clones`, before `dbt build`), then incremental delta merged.
   - `FULL_BUILD`: New or changed table, or full refresh.
   - `ROUTINE_DEPLOY`: `table_function`, `virtual_function`, `function`, or `procedure`.
   - Fingerprints are the same in dev, PR and prod. An unchanged model that shows `FULL_BUILD` usually reads a per-target schema outside `ref()`: add `--known-schema <schema>`.

2. **Build a Candidate Release in Staging (`build`)**:
   ```bash
   dbt-virtual build --env staging --state-file state.json --release <release_id>
   ```
   - Runs, with the same `--target` / `--vars`: REUSE view sync, incremental clone seeding, `dbt build --select <changed>`, then `virtual_snapshot_release` so the release covers the whole environment.
   - If adding columns to an incremental model, configure `virtual_add_columns: ['col_name TYPE']` so `seed_incremental_clones` alters the cloned table before incremental insertion.
   - For forward-only changes where you want to pin an existing physical table version, set `physical_version: '<fp>'` and `forward_only: true`.

3. **Schema Diff & Pre-Promotion Quality Gate (`diff` + `audit`)**:
   ```bash
   dbt-virtual diff --base-manifest prod_manifest.json --fail-on-breaking
   dbt-virtual audit --from staging --to prod --release <release_id> --max-row-drop-pct 15.0
   ```
   - On BigQuery, audit row counts come from `__TABLES__` (works with `require_partition_filter`).

4. **Zero-Rebuild Promotion to Production (`promote`)**:
   ```bash
   dbt-virtual promote --from staging --to prod --release <release_id> --audit --max-row-drop-pct 15.0
   ```
   - Takes the target environment's lease lock (`--no-lock` to skip, `--lock-owner` to reuse a lock the job already holds).
   - Skips unchanged fingerprints (partial promotion), deploys environment views first, then routines in DAG order (at equal depth UDFs, then table functions, then procedures), each into the target environment's schema for its custom schema (`env_schemas`), with routine references re-pointed.
   - Demotes (drops) only objects that are not in the release and are no longer virtual nodes in the project.

5. **Instant Rollback (`rollback`)**:
   ```bash
   dbt-virtual rollback --env prod
   # Or to a specific release:
   dbt-virtual rollback --env prod --to-release <prior_release_id>
   ```
   - Fails before changing anything if a physical table of the target release is gone (e.g. dropped by `gc`).
   - Undone releases are marked `rolled_back`; running it again goes one more release back.

6. **Ephemeral Pull Request Environments (`init-env` / `drop-env`) & Janitor GC (`gc`)**:
   ```bash
   dbt-virtual init-env --env pr_123 --from prod
   dbt-virtual drop-env --env pr_123
   dbt-virtual gc --dry-run
   ```
   - `pr_123` is not in `env_schemas`, so it gets one schema, `<prod default schema>_pr_123`.
   - `drop-env` refuses `base_environment`, `physical_schema`, and schemas of other listed environments.
