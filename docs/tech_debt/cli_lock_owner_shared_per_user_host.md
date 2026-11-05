---
status: open
area: lifecycle
impact: medium
effort: s
bean: dbt-virtual-0030
---

# The CLI lease owner is the same for every process of one user on one host

## What

`dbt-virtual promote` and `rollback` take the `_locks` lease with `--lock-owner`, which defaults to
`dbt-virtual-cli:<user>@<host>` (`_default_lock_owner` in `src/dbt_virtual/cli.py`,
`TECH_DEBT(cli_lock_owner_shared_per_user_host)`). `virtual_acquire_lock` treats a second acquire
by the same owner as a refresh: it deletes that owner's lease row and inserts a new one.

## Why it exists

The default was chosen "stable for one user on one machine, so a failed promote can be retried"
(docstring of `_default_lock_owner`): a retry after a crash must not wait for the lease TTL.

## Consequence

Two promotes started by the same user on the same machine (two terminals, or two CI jobs on one
self-hosted runner) both acquire the lock and run at once. Evidence: `scenario_same_owner_reacquires_the_lease`
in `tests/registry_concurrency.py` (DuckDB, `tests/test_registry_concurrency_duckdb.py`): two
simultaneous acquires with one owner both succeed, and the lease table briefly holds two rows for it.
Different owners exclude each other (`scenario_lock_race_has_exactly_one_winner`). Not measured: how
often CI runners share a host name.

## Fix

Make the default owner unique per process (add the pid or a random id) and let a retry release or
take over its own earlier lease explicitly (for example `--lock-owner` set by the caller, or a
`release-lock` command). Then flip the scenario to expect a lock conflict.

## Workaround meanwhile

Pass a distinct `--lock-owner` per CI job or terminal, for example the CI run id.
