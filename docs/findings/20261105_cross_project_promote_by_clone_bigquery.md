# Cross-project promote by clone on real BigQuery

Dated `docs/findings/20261105_cross_project_promote_by_clone_bigquery.md`.

## Question

Does `physical_locations` with two different GCP projects work on real BigQuery: CI builds in one project, prod promotes by `CREATE TABLE ... CLONE` across projects, routine templates are replayed in the prod project, CI can be dropped afterwards, CI can seed an incremental model from prod, and the audit and `diff-data` gates compare across projects? Which IAM permissions did that need? (`docs/decisions/20261105_cross_project_promote_by_clone.md` listed all of this as not verified.)

## Method

`scripts/run_bigquery_sandbox_e2e.py --xproject --project <ci-project> --prod-project <prod-project>` (opt-in, not in CI), run on 2026-11-05 with dbt-core 1.12.6 + dbt-bigquery 1.12.2 and Application Default Credentials of one human user, location US in both projects. Two runs: v1 (found a bug in step 10) and v2 (after the fix, below). Every dataset was named `dbt_virtual_sbx_<run>_x3_*`; the script dropped them at the end and printed the remaining datasets with the run prefix per project: an empty list for both projects in both runs, and an independent listing afterwards showed no `dbt_virtual_*` dataset in either project.

Layout: `physical_locations: {prod: {database: <prod-project>, schema: ..._phys_prod}, default: {database: <ci-project>, schema: ..._phys_ci}}`; dbt targets `staging` (CI project) and `prod` (prod project). Models: `events` (incremental), `orders` (table), `big_orders` (`table_function` reading `orders`), `amt_plus` (`virtual_function` with a scalar subquery on `orders`). Promote used `promote --target prod --source-location <ci-project>.<ci-physical-schema>`.

## Results

| # | Item | Result (v2) | Evidence (2026-11-05) |
|---|---|---|---|
| 1 | `CREATE TABLE ... CLONE` across projects, views, registry | PASS | The log line `promote: cloning <ci>.<phys_ci>.events__<fp> -> <prod>.<phys_prod>.events__<fp>`; `INFORMATION_SCHEMA.TABLES` of the prod physical dataset had `table_type = CLONE` for `events` and `orders`; prod views (in the prod project) pointed at `<prod>.<phys_prod>.<model>__<fp>` and not at the CI project; the prod registry held 4 pointers and the release rows (`status promoted`, `source_environment staging`); the CI registry had no `prod` pointer; promoting a second time left 4 release rows. No error from BigQuery for a cross-project clone with both datasets in US. |
| 2 | Routine templates rewritten to the prod project | PASS | The deployed `big_orders` and `amt_plus` bodies in prod read `` `<prod>`.`<prod env dataset>`.`orders` `` and contained no CI project id; `big_orders(20)` returned ids `[2, 3]` and `amt_plus(1)` returned 41. |
| 3 | Drop the CI datasets | PASS | After `delete_dataset` of the CI physical and staging datasets, prod views, the TVF and the UDF still answered the same rows. |
| 4 | CI seeds an incremental model from prod (clone prod to CI) | PASS | `build` of release b2 in the CI project: `events__<fp2>` held `(1,a,10) (2,b,25) (3,c,40) (4,D,55)`: the three old rows kept their lower-case `v` (a full rebuild would upper-case them), only the new row was added. |
| 5 | `audit` and `diff-data` gates across projects | PASS | `diff-data` (CI candidate vs prod in the other project): `only_candidate 1, only_target 0, changed 0` for `events` and `orders`; `audit`: `candidate=4 rows, prod=3 rows`; `promote --data-diff --max-mismatch-pct 0` refused (`data diff failed`, rc 1) and prod still pointed at fp1. |
| - | Rollback in prod after two cross-project promotes | FAIL in v1, PASS in v2 | See Bugs. v2: `rollback` repointed `events` and `orders` to the b1 clones and redeployed `amt_plus` and `big_orders`. |

## Bugs found and fixed

`virtual_rollback` in prod failed after a cross-project promote (v1, step 10): `Not found: Dataset <ci-project>:<prod env dataset> was not found in location US`. `virtual_promote` rewrote the source project id in a routine template only for the DDL it deployed, but copied the original template (naming the CI project) into the prod `_releases`. Rollback redeploys routines from that registry and has no source project at hand, so it ran DDL that read the CI project.

Fix: `retarget_project_in_ddl` (macros/lifecycle/operations.sql) rewrites the project id once, and is used both for the deployed DDL and for the template stored in the target registry (`promoted_release_values_sql`). Regression tests, run 2026-11-05: `tests/test_cross_project.py::test_cross_location_promote_stores_the_routine_template_with_the_target_project` and `::test_retarget_project_in_ddl_only_replaces_whole_project_ids` (macro harness, BigQuery dialect; both fail without the macro change), emulator step 21 in `scripts/run_emulator_e2e.py` (a second emulator project; fails without the fix on `flipto/main` of the emulator, passes with it), and `tests/test_dbt_duckdb_cross_project.py::test_rollback_in_prod_after_cross_location_promotes_redeploys_routines` (real dbt-duckdb; DuckDB has no project boundary, so this guards the rollback path and was not shown to fail without the fix). Releases promoted by an older version still carry the CI project in the prod registry; rolling back to one of them needs a re-promote (not measured).

## IAM

The run used one human identity, not the two service accounts of a real setup, so least privilege was **not** tested: no principal with only the minimal roles was available.

- CI project: the project policy (read with `gcloud projects get-iam-policy`) gave the identity `roles/editor`. `projects.testIamPermissions` returned the BigQuery permissions `jobs.create`, `jobs.list`, `datasets.create`, `datasets.get`, `routines.create`, `routines.get`, `routines.list` and **not** `tables.create`, `tables.get`, `tables.getData`, `tables.list`, `tables.updateData`, `tables.delete`, `datasets.update`, `datasets.delete`. Reads and writes of tables still worked, so the identity got them some other way (most likely as the creator and owner of the datasets it made); not investigated.
- Prod project: the policy could not be read (`resourcemanager.projects.getIamPolicy` denied), so the roles are unknown. `testIamPermissions` returned all probed permissions except `bigquery.jobs.list`, `datasets.update`, `datasets.delete`. The missing `bigquery.jobs.list` made `INFORMATION_SCHEMA.JOBS_BY_USER` fail there (`Access Denied ... bigquery.jobs.list ... at project level`); the script falls back to `jobs.list` / `jobs.get` of the user's own jobs.
- Needed for the flow that passed (observed, not minimal): in the CI project the permissions above; in the prod project dataset create, table create and read, routine create, `jobs.create` (the clone job, the registry statements and the views ran as jobs of the prod project).
- Expected minimal roles, from BigQuery's documentation of table clones, **not verified here**: CI identity `roles/bigquery.dataEditor` + `roles/bigquery.jobUser` on the CI project and `roles/bigquery.dataViewer` on prod's physical dataset (seeding clones prod's table into CI); prod identity `dataEditor` + `jobUser` on the prod project and `dataViewer` on the CI physical dataset (the clone reads it). To verify: run the script with two service accounts holding exactly these roles, and check that removing the `dataViewer` grant makes the promote fail before anything changes (the pre-flight reads the source table).

## Bytes billed per step

From `INFORMATION_SCHEMA.JOBS_BY_USER` (CI project) and `jobs.list` / `jobs.get` (prod project), run v2. Each query is billed at the 10 MB per-table minimum, so billed bytes mostly count queries on this tiny data.

| Step | Project | Jobs | Bytes processed | Bytes billed |
|---|---|---|---|---|
| build b1 (CI) | CI | 49 | 2,514 | 62,914,560 |
| promote b1 (with the 2 clones) | prod | 29 | 31,458,711 | 115,343,360 |
| promote b1 again | prod | 23 | 2,411 | 136,314,880 |
| build b2 (seed from prod) | CI | 52 | 2,843 | 94,371,840 |
| diff-data b2 vs prod | CI | 10 | 1,301 | 146,800,640 |
| audit b2 vs prod | CI | 8 | 41,943,606 | 62,914,560 |
| promote b2 `--data-diff` (refused) | prod | 22 | 41,944,958 | 220,200,960 |
| promote b2 | prod | 31 | 73,403,413 | 188,743,680 |
| rollback | prod | 19 | 10,490,745 | 125,829,120 |

Which of the 31 to 73 MB processed in the audit and promote steps come from the gates' `INFORMATION_SCHEMA` and `__TABLES__` reads rather than the data was not broken down. The `CREATE TABLE ... CLONE` jobs themselves, read by `jobs.get` after the run: 4 in the prod project (2 for b1, 2 for b2) and 1 in the CI project (the seeding clone of prod's `events`), each with 0 bytes processed and 0 bytes billed.

## Conclusion

Items 1 to 5 are verified on real BigQuery for two projects in the same location (US), including the routine-template rewrite. One real bug (rollback after a cross-project promote) was found and fixed. Not verified: least-privilege service accounts, datasets in different locations (BigQuery requires the same location for a clone; not tried), a reader of the prod views without access to the physical dataset, tables large enough for the billing minimum not to dominate.
