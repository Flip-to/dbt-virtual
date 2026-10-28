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
- Routines (`table_function`, `function`, `procedure`) & `tvf_test_ref`: `docs/guides/routines_and_tvf_testing.md`

## Standard Workflows

1. **Inspect Live State & Plan (`status` + `plan`)**:
   ```bash
   dbt compile --target staging
   dbt-virtual status --env staging --base-env prod --output state.json
   dbt-virtual plan --env staging --base-env prod --state-file state.json
   ```
   - `REUSE`: Physical table `<model>__<fp>` already exists (0 compute; only view pointer synced).
   - `SEED_INCREMENTAL`: Cloned from `prod` via zero-copy `CLONE` (`clone_if_new_fp`), then incremental delta merged.
   - `FULL_BUILD`: New table or full refresh.
   - `ROUTINE_DEPLOY`: `table_function`, `function`, or `procedure`.

2. **Build a Candidate Release in Staging (`build`)**:
   ```bash
   dbt-virtual build --env staging --base-env prod --state-file state.json --release <release_id>
   ```
   - If adding columns to an incremental model, configure `virtual_add_columns: ['col_name TYPE']` so `clone_if_new_fp` alters the cloned table before incremental insertion.
   - For forward-only changes where you want to pin an existing physical table version, set `physical_version: '<fp>'` and `forward_only: true`.

3. **Schema Diff & Pre-Promotion Quality Gate (`diff` + `audit`)**:
   ```bash
   dbt-virtual diff --base-manifest prod_manifest.json --fail-on-breaking
   dbt-virtual audit --from staging --to prod --release <release_id> --max-row-drop-pct 15.0
   ```

4. **Zero-Rebuild Promotion to Production (`promote`)**:
   ```bash
   dbt-virtual promote --from staging --to prod --release <release_id> --audit --max-row-drop-pct 15.0
   ```
   - Skips unchanged fingerprints (partial promotion), deploys in strict dependency tier order (`1=function -> 2=model view -> 3=table_function -> 4=procedure`), and automatically demotes views/routines removed from the release.

5. **Instant Rollback (`rollback`)**:
   ```bash
   dbt-virtual rollback --env prod
   # Or to a specific release:
   dbt-virtual rollback --env prod --to-release <prior_release_id>
   ```

6. **Ephemeral Pull Request Environments (`init-env` / `drop-env`) & Janitor GC (`gc`)**:
   ```bash
   dbt-virtual init-env --env pr_123 --from prod
   dbt-virtual drop-env --env pr_123
   dbt-virtual gc --dry-run
   ```
