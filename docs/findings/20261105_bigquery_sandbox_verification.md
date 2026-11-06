# Real BigQuery verification of the newer features

Dated `docs/findings/20261105_bigquery_sandbox_verification.md`.

## Question

Do `authorize_env_views`, env view metadata, promote-by-clone across locations, `diff-data`, `gc --expire-env-days` and the partition coverage audit work on real BigQuery, not only on DuckDB, the macro harness and the emulator?

## Method

`scripts/run_bigquery_sandbox_e2e.py --extended` (opt-in, not in CI), run on 2026-11-05 against a throwaway sandbox in one GCP project (placeholder: `<sandbox-project>`, location US), with dbt-core + dbt-bigquery 1.12.2 and Application Default Credentials. Every dataset was named `dbt_virtual_sbx_<run>_...` and dropped at the end of each run; the script lists the remaining datasets with that prefix and printed an empty list every time (runs v1 to v4).

Two extra dbt projects were built per run:

- `proj_x1` (one location): `authorize_env_views: true`; `facts` (3000 rows, daily partitions over 30 days, `persist_docs`, labels, column docs) and `facts_rpf` (same data, `require_partition_filter=true`).
- `proj_x2` (two "locations" that differ by schema in the same project): `events` (incremental), `orders`, `big_orders` (table function).

Row counts were checked against an independent `FULL OUTER JOIN` run in BigQuery. Bytes per query come from `INFORMATION_SCHEMA.JOBS_BY_USER` (one job per query the diff reported).

## Results

| # | Item | Result | Evidence (2026-11-05) |
|---|---|---|---|
| 1 | `authorize_env_views` | PASS | The physical dataset's `access` list held exactly one `dataset` entry with `targetTypes: [VIEWS]` per env dataset (staging after build, prod after promote, none for prod before it had views); still one each after re-running build and promote. Not verified: that a reader without access to the physical dataset can query the view (no second principal). |
| 2 | Env view metadata | PASS | `CREATE OR REPLACE VIEW ... OPTIONS(description, labels)` accepted for a description containing `;` and `'`; labels, description and column descriptions (`update_columns`) read back equal on staging and prod views; a model without `persist_docs`/labels got none. |
| 3 | Promote by clone across locations | PASS, same project only | Tables cloned into the target schema had `table_type = CLONE`; prod views and the table function pointed at the clones; prod registry held the pointers and the release (`source_environment = staging`); promoting twice did not duplicate rows; after dropping the CI dataset and the staging dataset, prod views and the TVF still answered; CI then seeded an incremental model by cloning prod's table (old values kept, one new row); rollback to the older release worked. Cross-PROJECT `CLONE` is unverified: no second sandbox project was available, so IAM roles and the project-id rewrite of routine templates were not exercised. |
| 3b | Gates across locations | FAIL, fixed | See Bugs. |
| 4 | `diff-data` | PASS | Injected differences: 24 changed, 12 removed, 5 added. Reported and oracle counts both `(only_candidate 5, only_target 12, changed 24)`, `mismatch_pct` 1.3667, changed column `amount` only. Bytes (processed / billed, run v2): full table 6 queries, 162,768 / 104,857,600; `--window-days 3` 4 queries, 28,872 / 62,914,560; bisection with `--threshold 20 --branching 4` 67 queries (28 checksum, 34 join, 5 sample), 246,408 / 1,090,519,040. Every query is billed at the 10 MB per-table minimum, so on a 60 KB table billed bytes measure the number of queries, not data scanned; bytes processed shows the partition pruning. `require_partition_filter` table: a plain diff fails with BigQuery's "Cannot query over table ... without a filter over column(s) 'day'" (job error `invalidQuery`, 0 bytes); `--window-days 7` worked and matched the windowed oracle (4 queries, 48,072 / 62,914,560). `--max-mismatch-pct 1` failed the run (1.37% > 1). |
| 5 | `gc --expire-env-days` | PASS | `pr_old`, `staging` and `prod` were aged 40 days by DML on `_environments.updated_at`. `--expire-env-days 60` expired nothing; 30 listed only `pr_old` in the dry run (mapped environments protected); the real run dropped its dataset and pointers and left prod and staging intact. |
| 6 | Partition coverage audit | PASS | 30 partitions present: pass on both tables, including the `require_partition_filter` one (row counts come from `__TABLES__`). A windowed build lacking 20 partitions failed with `lacks 20 partition(s)` and the range `YYYYMMDD..YYYYMMDD (20)`; `--coverage-ignore-before` and `--no-coverage-check` let it pass. |
| - | Base lifecycle (existing steps) | PASS | Run v1: every pre-existing check passed (build, promote, rollback, audit, locks, init/drop-env, gc). |

## Bugs found and fixed

`virtual_audit` and `virtual_data_diff` read the `to_env` registry from the running target's location. With `physical_locations`, a CI run (target staging) has no prod pointers there, so:

- `diff-data --to prod` reported every model as `new`;
- `audit --to prod` logged `OK (new model ...)` and compared nothing;
- `promote --data-diff --max-mismatch-pct 0` run with the prod target read the not yet cloned release from the prod registry, found no candidates and passed, promoting a release that differs (observed on real BigQuery, run v2: promote succeeded, prod moved to the new fingerprint).

Fix: `registry_location_for` (macros/lifecycle/data_diff.sql) resolves each side's location; `virtual_audit` reads `to_env` from its location; `virtual_data_diff` takes `source_database` / `source_schema` (passed by `virtual_promote` for a cross-location release) and logs when it finds no candidate models. Regression tests, each failing before the fix (run 2026-11-05): `tests/test_cross_project.py::test_bigquery_gates_read_prod_from_its_own_location` (BigQuery dialect, macro harness), `tests/test_dbt_duckdb_cross_project.py::test_gates_compare_with_prod_in_its_own_location` (real dbt-duckdb), emulator step 20 in `scripts/run_emulator_e2e.py` (not run: no Docker daemon on the verification machine; it runs in CI), and the sandbox checks in `extended_cross_location`.

## Conclusion

Items 1, 2, 4, 5 and 6 are verified on real BigQuery. Item 3 is verified for two locations in one project; the cross-project case was verified afterwards, see `docs/findings/20261105_cross_project_promote_by_clone_bigquery.md` (it also found a rollback bug that this run could not see). IAM roles stay unverified. One real bug (gates across locations) was found and fixed. Not measured: cost on tables large enough for the 10 MB billing minimum not to dominate.
