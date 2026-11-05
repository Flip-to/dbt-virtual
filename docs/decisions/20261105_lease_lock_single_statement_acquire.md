---
status: accepted
kind: developer
who: "Developer: Yuri Olive"
ticket: None
spec: None
prs: [25]
---

# The lease lock is acquired by one conditional statement on a keyed `_locks`, then verified by reading back

## Context

`virtual_acquire_lock` deleted expired leases, deleted its own, inserted its own lease with
`acquired_at = CURRENT_TIMESTAMP` (the statement's start time), read every live lease and won when
its row came first by `ORDER BY acquired_at, owner`. Under MVCC a reader cannot order by the start
time of a row it cannot see yet: runner w3 read before w0's insert was visible, saw only itself and
won; w0, whose `acquired_at` was earlier, then read both rows and won as well. Evidence: CI run of
PR #25 (Python 3.11), `tests/test_registry_concurrency_duckdb.py::test_lock_race_has_exactly_one_winner`,
"round 2: 2 runners hold the lock: ['w0', 'w3']". Two runners holding the lock defeats its purpose
(`virtual_promote`, `virtual_rollback`).

## Decision

Acquire is two steps, each committed on its own (`macros/lifecycle/operations.sql`,
`DECISION(lease_lock_single_statement_acquire)`):

1. One conditional statement, `lock_upsert_sql` (`macros/adapters/dispatch.sql`): write this
   owner's lease unless another owner's lease on the environment is live. The same statement
   refreshes the owner's own lease and takes over an expired one.
   - DuckDB and PostgreSQL (`default__`): `INSERT ... ON CONFLICT (environment) DO UPDATE ... WHERE
     l.owner = EXCLUDED.owner OR l.expires_at <= EXCLUDED.acquired_at`, on a UNIQUE index
     `_locks_environment_uq` over `_locks(environment)`.
   - BigQuery, Snowflake, Databricks: `MERGE` keyed on `environment` with the same condition.
2. Read the live leases back. The runner holds the lock only if no live lease of another owner is
   visible and its own is. Otherwise it deletes its own row (if any) and fails with
   "dbt-virtual lock conflict". The old start-time ordering is gone.

Existing `_locks` tables (v0.1.0 and v0.2.0 have no key): `ensure_locks_unique_key` runs at the start
of an acquire on DuckDB and PostgreSQL while the index is missing; it keeps the latest lease per
environment and creates the index. Fresh registries get the index in `ensure_registry_tables_sql`.

## Why

Correctness argument per adapter:

- **PostgreSQL**: the unique index allows one row per environment. `ON CONFLICT DO UPDATE` waits
  for a concurrent inserter of the key and then evaluates its `WHERE` against the committed row; a
  false `WHERE` changes nothing. Exactly one runner ends with its owner in the row. This is
  enforced by the database. Exercised by the PostgreSQL scenarios of
  `scripts/run_snowflake_emulator_e2e.py` (see the evidence in PR #25).
- **DuckDB**: same unique index; the second concurrent insert of a key fails with a unique
  violation instead of waiting ("Duplicate key ... violates unique constraint", or "Failed to
  commit: PRIMARY KEY or UNIQUE constraint violation"). That runner fails without holding a lease;
  its error is the DuckDB error, not the "lock conflict" text. Measured on 2026-11-05: 30 rounds of
  6 threads in a probe script gave exactly one winner in each round.
- **BigQuery**: no enforced constraints, and plain `INSERT` statements on one table run
  concurrently without conflicting, so `INSERT ... WHERE NOT EXISTS` would not exclude anyone.
  `MERGE` is mutating DML: BigQuery serializes concurrent mutating DML on a table and retries a
  conflicting one, so the second `MERGE` sees the first one's row and matches it (condition false,
  no change). Source: BigQuery documentation on DML concurrency, not re-measured here: the emulator
  does not serialize or conflict DML, so `scripts/run_emulator_concurrency_upgrade_e2e.py` proves the
  statement shape and the single-threaded semantics, not the serialization. A conflict that
  exhausts BigQuery's retries fails the statement, which is a loser too.
- **Snowflake**: `MERGE` takes the table's DML lock; the second waits and then sees the committed
  row. No constraint enforces it (Snowflake does not enforce keys), so this rests on documented
  locking behavior and is not measured here: `TECH_DEBT(snowflake_lock_not_constraint_enforced)`.
- **Databricks**: `MERGE` into a Delta table; Delta's conflict detection fails the later of two
  concurrent writers. Not measured here (no Databricks run in CI); same tech debt item.
- **Backstop on every adapter (step 2)**: each runner reads after its own commit. If two runners
  both wrote (a warehouse that did not serialize step 1), the one that reads second sees the other's
  committed row, so at most one runner can see "only me". Both can see each other and both yield; two
  can never win. Cost: a spurious double failure (the caller retries). The tests expect exactly one
  winner only where step 1 serializes.

## Alternatives

- **Verify-after-commit alone** (insert, read back, yield if any other live lease): safe by the
  argument above, but under a real race it can fail every contender (nobody wins). Kept as the
  backstop, not as the primary.
- **Keep ordering by `acquired_at`** and read after a delay: a sleep is not a correctness argument.
- **`INSERT ... WHERE NOT EXISTS`** alone: not serialized on BigQuery, and under MVCC on
  DuckDB/PostgreSQL both inserts see "none" (it needs the key anyway).
- **A PRIMARY KEY on `_locks`**: DuckDB cannot add a primary key to an existing table; a unique
  index can be added to an existing table and works with `ON CONFLICT` (checked on DuckDB).
- **Advisory locks (`pg_advisory_lock`)**: PostgreSQL only.

## Impact

- `_locks` holds at most one row per environment on DuckDB/PostgreSQL; BigQuery/Snowflake/Databricks
  keep one by the `MERGE` condition (legacy duplicate rows expire by TTL).
- Acquire sends one DML statement instead of three (BigQuery: fewer jobs, less DML contention).
- The DuckDB loser error text differs from the PostgreSQL/BigQuery one; `tests/registry_concurrency.py`
  accepts it through `Backend.lost_race_errors`.
- A live lease of another owner blocks regardless of start time (the previous rule let an earlier
  `acquired_at` win).
- The takeover condition compares the old `expires_at` with the new writer's clock; runner clock skew
  larger than the gap matters (not measured). Revisit when Snowflake or Databricks get a concurrent
  acquire test (see the tech debt item).

## Who

Developer decision: Yuri Olive. Implemented by a subagent of session
9d590ac4-0b78-4fbf-91ee-2f18573cbc66, approved by Yuri Olive.
