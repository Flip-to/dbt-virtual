---
status: accepted
kind: developer
who: "Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66), approved by Yuri Olive"
ticket: None
spec: None
prs: []
---

# An incomplete table can never be promoted: four guards in `virtual_promote`

## Context

Slim CI and a planned staging build tables over a short data window (vars such as `backfill_start`, 2 to 8 days) or with `dbt build --empty`. `virtual_promote` copied whatever a release listed, so a windowed, empty or half-failed table could reach prod. Evidence of the gap: before this change `tests/test_promote_guards.py` fails 23 of 23 on the parent commit `dc8465a` (run 2026-11-04).

## Decision

1. **Fingerprint guard.** `dbt-virtual promote` compiles the project for the target (`dbt compile --target <to>`, override `--compile-target`), computes the fingerprints and passes `{name: fingerprint}` as `expected_fps_json`. `virtual_promote` refuses, before changing anything, any release row whose fingerprint differs or whose name is not a virtual node of the project, listing them. The macro argument is optional (manual use); the CLI always sets it unless `--skip-fingerprint-check` (documented as dangerous).
2. **Partial marker.** `sync_env_view` stamps `build_quality` on the pointer and release rows it writes: `partial` when `flags.EMPTY`, when a var listed in `vars.dbt_virtual.partial_build_vars` is set, or when `_dbt_virtual_partial` is true; `full` otherwise (always written, so a full rebuild clears an old marker). `NULL` (rows from before) is read as full. `virtual_promote` refuses partial rows. `virtual_export_state` exports `partial_physical_tables` (a table is partial when its latest registration is partial) and `classify_plan_actions(partial_physical_tables=...)` neither reuses nor clones them.
3. **Sealed releases.** `virtual_snapshot_release`, run by `dbt-virtual build --release` only after a successful `dbt build`, sets `_releases.sealed_at`. `virtual_promote` with a `release_id` refuses a release that has no seal when its rows carry `build_quality` (i.e. written by this version: the build did not finish) unless `allow_unsealed=true`.
4. **Coverage audit.** `virtual_audit` (and `promote --audit`) on BigQuery compares `INFORMATION_SCHEMA.PARTITIONS` of the candidate with the current target table and fails when the candidate lacks partitions the target has (`__NULL__`, `__UNPARTITIONED__` and partitions older than `coverage_ignore_before` ignored), reporting the missing ranges. Other adapters skip it with a log. The CLI turns the audit on by default when `--to` is the base environment.

Backward compatibility of seals: a release created before this change has neither a seal nor `build_quality`, so it cannot be told apart from a finished release. The macro default is permissive (`require_sealed=false`, so existing manual calls and tests keep working), the CLI default is strict (`--require-sealed`): such a release is refused until someone verifies its tables and runs `dbt run-operation virtual_seal_release --args '{release_id: <id>}'`, or passes `--no-require-sealed`. The registry gains `build_quality` (both tables) and `sealed_at` (`_releases`) through `registry_migrations_sql`, triggered by `ensure_registry` when a column is missing.

## Why

- The fingerprint is the only signal that already covers code, rendered variables and parents (Merkle DAG), so a windowed build is caught even without the marker (`test_windowed_build_cannot_be_promoted`); the marker catches `--empty`, which renders the same SQL and therefore the same fingerprint (`test_empty_build_is_marked_partial`).
- A seal written only after the whole `dbt build` succeeded is the only way to know a release is complete: hooks already register each model as it finishes (`test_failed_build_leaves_the_release_unsealed_and_unpromotable`).
- Strict CLI default, permissive macro default: the CLI is the supported path to prod; the legacy-release escape hatch is explicit and logged.

## Alternatives

- Put the seal in `_releases.status = 'sealed'`: `status` is already `candidate` / `promoted` / `rolled_back` and used by rollback, gc and the registry cluster key; a separate column avoids touching those.
- Compare fingerprints in the CLI only: the macro is also run through `dbt run-operation`; the check belongs where the data is read, with the CLI computing the expectation.
- Count partitions with `COUNT(*)` per day: costs a scan per table; metadata is free.

## Impact

- New file `macros/lifecycle/guards.sql`; small edits in `sync_env_view`, `ensure_registry`, `virtual_promote`, `virtual_audit`, `virtual_export_state`, `virtual_snapshot_release` and the registry DDL.
- `promote` now needs a compiled manifest for `--to` (one `dbt compile`) unless `--skip-compile` / `--skip-fingerprint-check`.
- Raw `dbt build` with `_dbt_virtual_release` followed by a direct `virtual_promote` now needs `allow_unsealed=true`.
- Not verified: the BigQuery SQL of the coverage check against a real BigQuery (only the macro rendering and the harness); `INFORMATION_SCHEMA.PARTITIONS` column names are from BigQuery's documentation. Revisit if a project partitions by integer ranges (such ids are never ignored by `coverage_ignore_before`).

## Who

Developer decision, drafted by Agent: Claude (session 9d590ac4-0b78-4fbf-91ee-2f18573cbc66); approver Yuri Olive.
