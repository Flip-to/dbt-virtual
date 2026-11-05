# Companion state: manifests that describe a virtual model's table

Some incremental models keep their progress outside the model, for example Snowplow's
`snowplow_unified_incremental_manifest` (one row per model name with `last_success`). The row and
the physical table `<model>__<fp>` describe the same data, so they must move together. Without
that, a changed model's new table is built in full next to a row that says "consumed up to X", and
reads only what comes after X.

Decision and reasoning: `docs/decisions/20261105_companion_state_for_manifest_tracked_models.md`.
Known gaps: `docs/tech_debt/companion_state_gaps.md`.

## Configuration

```yaml
vars:
  dbt_virtual:
    companion_on_full_build: error        # or `reset`
    companion_state:
      - table: snowplow_unified_incremental_manifest   # model name, `schema.table` or `db.schema.table`
        key_column: model                              # default `model`
        tag: snowplow_unified_incremental              # optional: only models with this tag
        env_tables:                                    # optional: where each environment keeps it
          prod: rt_pipeline_prod1_snowplow_manifest.snowplow_unified_incremental_manifest
          staging: analytics_staging.snowplow_unified_incremental_manifest
```

Only virtual incremental models are tracked. A bare model name resolves to that model's relation in
the current target; set `env_tables` when promote or rollback run from a different target than the
environment they act on, or when environments keep separate manifests. Add
`{{ dbt_virtual.seed_incremental_clones() }}` (or `dbt_virtual.on_run_start()`) to `on-run-start`;
`dbt-virtual build` runs it for you.

## What happens

| Event | Effect on the environment's state rows |
|---|---|
| Seed by clone from the base environment | rows of the base table are copied in |
| Table built in full (no base table, or `--full-refresh`) with state present | run fails (`error`), or rows are deleted so the package backfills (`reset`) |
| Table another environment built is used | rows of that table are copied in |
| Promote | rows of the promoted fingerprint are written to the target environment |
| Rollback | rows of the restored fingerprint are written back |
| Reuse sync | rows are copied in when the environment moves to the table |
| Promote from another location (`physical_locations`, `--source-location`) | rows of the promoted fingerprint are written from the source environment's live rows, else from the rows saved in the source registry |
| Build that fails after the seed | `flush_registry` (on-run-end) puts the rows back to the table the environment still points at |
| `gc` drops a table | the rows saved for that table in `_companion_state` are deleted |

When an environment's pointer leaves a fingerprint, its rows are saved in
`<physical_schema>._companion_state`. While an environment points at a fingerprint, its live rows
are used as the source.

`reset` makes the package treat the model as new: Snowplow replays from its start date for every
model in that run, not only the rebuilt one.
