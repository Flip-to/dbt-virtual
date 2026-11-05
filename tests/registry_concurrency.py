"""Concurrency scenarios for the registry and the lease lock, shared by every warehouse.

Each scenario renders the real dbt-virtual macros through `MacroTestHarness` and executes the SQL
on a live warehouse, one connection per worker thread. A `Backend` supplies the connections:

- `DuckDBBackend` (in-process, used by `tests/test_registry_concurrency_duckdb.py`),
- `BigQueryBackend` (the BigQuery emulator, `scripts/run_emulator_concurrency_upgrade_e2e.py`),
- `PostgresBackend` (a PostgreSQL 16 container, `scripts/run_snowflake_emulator_e2e.py`).

A scenario raises `AssertionError` with the evidence when the invariant it states does not hold.
"""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from tests.test_macros_execution import FakeRelation, MacroTestHarness

LOCK_CONFLICT = "dbt-virtual lock conflict"


def lost_the_lock(backend: Backend, message: str) -> bool:
    """A runner that did not get the lease: the lock-conflict error, or (DuckDB) the unique violation
    the warehouse raises for the second concurrent insert of `_locks(environment)`."""
    return LOCK_CONFLICT in message or any(m in message for m in backend.lost_race_errors)


def split_statements(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


# --------------------------------------------------------------------------------------------
# Backends
# --------------------------------------------------------------------------------------------


class Conn:
    """One warehouse session. `run(sql)` executes `;`-separated statements, returns the last rows."""

    def run(self, sql: str) -> tuple[list[tuple], list[str]]:  # pragma: no cover - interface
        raise NotImplementedError

    def lookup(self, database: str, schema: str, identifier: str) -> FakeRelation | None:
        raise NotImplementedError

    def close(self) -> None:
        pass

    def rows(self, sql: str) -> list[tuple]:
        return [tuple(r) for r in self.run(sql)[0]]


class Backend:
    name = ""
    adapter_type = "duckdb"
    database = ""
    quote_char = ""
    lost_race_errors: tuple[str, ...] = ()

    def new_conn(self) -> Conn:  # pragma: no cover - interface
        raise NotImplementedError

    def create_schema(self, schema: str) -> None:
        conn = self.new_conn()
        try:
            conn.run(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        finally:
            conn.close()

    def adapter_extras(self, conn: Conn) -> dict[str, Any]:
        return {}

    def ts(self, offset_minutes: int) -> str:
        """SQL timestamp expression `offset_minutes` from now (negative: in the past)."""
        return f"CURRENT_TIMESTAMP + INTERVAL '{offset_minutes} minutes'"

    def qualified(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"


class _DuckConn(Conn):
    def __init__(self, cursor) -> None:
        self.cursor = cursor

    def run(self, sql):
        rows: list[tuple] = []
        cols: list[str] = []
        for stmt in split_statements(sql):
            self.cursor.execute(stmt)
            if self.cursor.description:
                rows = self.cursor.fetchall()
                cols = [d[0] for d in self.cursor.description]
        return rows, cols

    def lookup(self, database, schema, identifier):
        rows = self.cursor.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            [schema, identifier],
        ).fetchall()
        if not rows:
            return None
        return FakeRelation(database, schema, identifier, "view" if rows[0][0] == "VIEW" else "table", "")

    def close(self):
        self.cursor.close()


class DuckDBBackend(Backend):
    name = "duckdb"
    lost_race_errors = ("violates unique constraint", "PRIMARY KEY or UNIQUE constraint violation")

    def __init__(self) -> None:
        import duckdb

        self.root = duckdb.connect(":memory:")

    def new_conn(self) -> Conn:
        return _DuckConn(self.root.cursor())


class _PgConn(Conn):
    def __init__(self, dsn: str) -> None:
        import psycopg

        self.conn = psycopg.connect(dsn, autocommit=True)

    def run(self, sql):
        rows: list[tuple] = []
        cols: list[str] = []
        for stmt in split_statements(sql):
            with self.conn.cursor() as cur:
                cur.execute(stmt)
                if cur.description:
                    rows = cur.fetchall()
                    cols = [d.name for d in cur.description]
        return rows, cols

    def lookup(self, database, schema, identifier):
        rows = self.run(
            "SELECT table_type FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_name = '{identifier}'"
        )[0]
        if not rows:
            return None
        return FakeRelation(database, schema, identifier, "view" if rows[0][0] == "VIEW" else "table", "")

    def close(self):
        self.conn.close()


class PostgresBackend(Backend):
    name = "postgres"
    adapter_type = "postgres"

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def new_conn(self) -> Conn:
        return _PgConn(self.dsn)


class _BqConn(Conn):
    def __init__(self, client, project: str) -> None:
        self.client, self.project = client, project

    def run(self, sql):
        rows: list[tuple] = []
        cols: list[str] = []
        for stmt in split_statements(sql):
            result = self.client.query(stmt).result()
            if result.schema:
                cols = [f.name for f in result.schema]
                rows = [tuple(r.values()) for r in result]
        return rows, cols

    def lookup(self, database, schema, identifier):
        from google.api_core.exceptions import NotFound

        try:
            table = self.client.get_table(f"{self.project}.{schema}.{identifier}")
        except NotFound:
            return None
        return FakeRelation(
            database, schema, identifier, "view" if table.table_type == "VIEW" else "table", "`"
        )


class BigQueryBackend(Backend):
    name = "bigquery"
    adapter_type = "bigquery"
    quote_char = "`"

    def __init__(self, client, project: str, endpoint: str) -> None:
        self.client, self.project, self.endpoint = client, project, endpoint
        self.database = project

    def new_conn(self) -> Conn:
        return _BqConn(self.client, self.project)

    def create_schema(self, schema: str) -> None:
        import requests

        requests.post(
            f"{self.endpoint}/bigquery/v2/projects/{self.project}/datasets",
            json={"datasetReference": {"projectId": self.project, "datasetId": schema}},
            timeout=10,
        )

    def adapter_extras(self, conn: Conn) -> dict[str, Any]:
        class Col:
            def __init__(self, name, data_type):
                self.name, self.data_type = name, data_type

        def columns_in_relation(rel):
            table = self.client.get_table(f"{rel.database}.{rel.schema}.{rel.identifier}")
            return [Col(f.name, f.field_type) for f in table.schema]

        return {"get_columns_in_relation": columns_in_relation, "quote": lambda name: f"`{name}`"}

    def ts(self, offset_minutes: int) -> str:
        return f"TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {offset_minutes} MINUTE)"

    def qualified(self, schema: str, table: str) -> str:
        return f"`{self.project}.{schema}.{table}`"


# --------------------------------------------------------------------------------------------
# Environment and harness
# --------------------------------------------------------------------------------------------


@dataclass
class Names:
    physical: str
    stg: str
    prod: str


def new_world(backend: Backend, tag: str) -> Names:
    """Fresh schemas with the registry tables (dbt's `ensure_registry` needs a statement block)."""
    suffix = uuid.uuid4().hex[:6]
    names = Names(f"cc_{tag}_{suffix}_phys", f"cc_{tag}_{suffix}_stg", f"cc_{tag}_{suffix}_prod")
    for schema in (names.physical, names.stg, names.prod):
        backend.create_schema(schema)
    conn = backend.new_conn()
    try:
        harness = make_harness(backend, conn, names)
        conn.run(harness.call_macro("ensure_registry_tables_sql", names.physical))
    finally:
        conn.close()
    return names


def make_harness(
    backend: Backend,
    conn: Conn,
    names: Names,
    *,
    env: str = "staging",
    release: str = "",
    fps: dict[str, str] | None = None,
    nodes: dict[str, Any] | None = None,
) -> MacroTestHarness:
    cfg = {
        "physical_schema": names.physical,
        "base_environment": "prod",
        "env_schemas": {"staging": names.stg, "prod": names.prod},
    }
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": cfg,
            "_dbt_virtual_fps": fps or {},
            "_dbt_virtual_release": release,
            "_dbt_virtual_env": env,
        },
        target_name=env,
        target_schema=names.stg,
        target_database=backend.database,
        adapter_type=backend.adapter_type,
    )
    harness.graph = {"nodes": nodes or {}}
    harness.query_executor = conn.run
    harness.relation_lookup = conn.lookup
    harness.adapter_extras = backend.adapter_extras(conn)
    return harness


@dataclass
class Outcome:
    ok: bool
    value: Any = None
    error: BaseException | None = None

    @property
    def message(self) -> str:
        return str(self.error) if self.error else ""


def run_parallel(backend: Backend, jobs: list[Callable[[Conn], Any]], timeout: float = 300) -> list[Outcome]:
    """Run `jobs` on separate connections, released together by a barrier; never raises."""
    barrier = threading.Barrier(len(jobs))

    def worker(job: Callable[[Conn], Any]) -> Outcome:
        conn = backend.new_conn()
        try:
            barrier.wait(timeout=60)
            try:
                return Outcome(True, job(conn))
            except Exception as exc:  # noqa: BLE001 - the scenario inspects every failure
                return Outcome(False, error=exc)
        finally:
            conn.close()

    with ThreadPoolExecutor(len(jobs)) as pool:
        futures = [pool.submit(worker, job) for job in jobs]
        return [f.result(timeout=timeout) for f in futures]


def read(backend: Backend, sql: str) -> list[tuple]:
    conn = backend.new_conn()
    try:
        return conn.rows(sql)
    finally:
        conn.close()


# --------------------------------------------------------------------------------------------
# Registry writes
# --------------------------------------------------------------------------------------------

MODELS = ("m0", "m1", "m2", "m3")


def _writer_nodes(names: Names, writer: str) -> tuple[dict[str, str], dict[str, Any], list[dict]]:
    """Per-writer fingerprints, graph and dbt `results` for the models in `MODELS`."""
    fps = {m: f"{writer}{m}".ljust(12, "0")[:12] for m in MODELS}
    nodes = {}
    for m in MODELS:
        uid = f"model.cc.{m}"
        nodes[uid] = {
            "unique_id": uid, "name": m, "resource_type": "model", "alias": f"{m}__{fps[m]}",
            "schema": names.physical, "database": "",
            "config": {"materialized": "table", "virtual_env": True},
            "compiled_code": f"SELECT '{writer}' AS w", "depends_on": {"nodes": []},
        }
    results = [{"node": n, "status": "success"} for n in nodes.values()]
    return fps, nodes, results


def flush_job(backend: Backend, names: Names, env: str, release: str, writer: str):
    """A worker that runs the real `flush_registry` for `MODELS` as `env` / `release`."""
    fps, nodes, results = _writer_nodes(names, writer)

    def job(conn: Conn):
        harness = make_harness(backend, conn, names, env=env, release=release, fps=fps, nodes=nodes)
        harness.call_macro("flush_registry", results)
        return fps

    return job


def assert_unique_pointers(backend: Backend, names: Names, expected_envs: int) -> list[tuple]:
    """`_environments` holds exactly one row per (environment, model_name)."""
    rows = read(
        backend,
        f"SELECT environment, model_name, fingerprint, release_id "
        f"FROM {backend.qualified(names.physical, '_environments')} ORDER BY environment, model_name",
    )
    keys = [(r[0], r[1]) for r in rows]
    dups = sorted({k for k in keys if keys.count(k) > 1})
    assert not dups, f"duplicate _environments rows for (environment, model): {dups}"
    assert len(keys) == expected_envs * len(MODELS), f"lost pointers: {len(keys)} rows, {rows}"
    return rows


def scenario_flushes_of_different_environments(backend: Backend, n: int = 5) -> None:
    """n builds flush at once into one registry, each for its own environment and release.

    Catches: a flush that clobbers another environment's pointers (a DELETE that is not scoped to
    the environment), lost pointers, duplicate `_environments` rows, or a lost `_releases` row.
    """
    names = new_world(backend, "envs")
    jobs = [flush_job(backend, names, f"pr_{i}", f"rel_{i}", f"w{i}") for i in range(n)]
    outcomes = run_parallel(backend, jobs)
    failed = [o.message for o in outcomes if not o.ok]
    assert not failed, f"flushes failed (conflicting DML must be retried or fail loudly): {failed}"
    rows = assert_unique_pointers(backend, names, n)
    for env, model, fingerprint, release in rows:
        i = int(env.split("_")[1])
        assert release == f"rel_{i}" and fingerprint.startswith(f"w{i}"), (env, model, fingerprint, release)
    releases = read(
        backend,
        f"SELECT release_id, model_name, COUNT(*) FROM {backend.qualified(names.physical, '_releases')} "
        "GROUP BY release_id, model_name",
    )
    assert len(releases) == n * len(MODELS) and all(r[2] == 1 for r in releases), releases


def scenario_flushes_of_the_same_environment(backend: Backend, n: int = 4, rounds: int = 3) -> None:
    """n builds flush the SAME environment at once (e.g. two CI runs of one PR).

    Catches: duplicate `_environments` rows for (environment, model_name) and a torn pointer
    (fingerprint of one writer, release of another). Last writer wins; which one is not specified.
    Probabilistic for a non-atomic upsert: the deterministic variant is
    `scenario_interleaved_pointer_upserts`.
    """
    names = new_world(backend, "same")
    for r in range(rounds):
        jobs = [flush_job(backend, names, "staging", f"rel_{r}_{i}", f"w{i}") for i in range(n)]
        outcomes = run_parallel(backend, jobs)
        failed = [o.message for o in outcomes if not o.ok]
        assert not failed, f"round {r}: flushes failed: {failed}"
        rows = assert_unique_pointers(backend, names, 1)
        for _env, model, fingerprint, release in rows:
            writer = fingerprint[:2]
            assert release.endswith(f"_{writer[1:]}"), f"torn pointer {model}: {fingerprint} / {release}"


def scenario_interleaved_pointer_upserts(backend: Backend) -> None:
    """Two writers upsert the SAME (environment, model) pointer, statements interleaved in order.

    The upsert macro's statements are executed A1, B1, A2, B2, ...: the schedule that a single
    atomic statement (MERGE) survives and a DELETE-then-INSERT pair does not. Deterministic, so it
    does not depend on timing. Catches: two `_environments` rows for one (environment, model).
    """
    names = new_world(backend, "ilv")
    conns = [backend.new_conn(), backend.new_conn()]
    try:
        statements = []
        for i, conn in enumerate(conns):
            harness = make_harness(backend, conn, names)
            sql = harness.call_macro(
                "upsert_environment_pointer_sql", names.physical, "prod", "m0", "model",
                f"fp{i}".ljust(12, "0"), f"{names.physical}.m0__fp{i}", f"rel_{i}", None,
            )
            statements.append(split_statements(sql))
        for step in range(max(len(s) for s in statements)):
            for conn, stmts in zip(conns, statements):
                if step < len(stmts):
                    conn.run(stmts[step])
    finally:
        for conn in conns:
            conn.close()
    rows = read(
        backend,
        f"SELECT environment, model_name, fingerprint FROM {backend.qualified(names.physical, '_environments')}",
    )
    assert len(rows) == 1, f"two writers left {len(rows)} rows for one (environment, model): {rows}"


# --------------------------------------------------------------------------------------------
# Lease locks
# --------------------------------------------------------------------------------------------


def _acquire(backend: Backend, names: Names, env: str, owner: str, ttl: int = 15):
    def job(conn: Conn):
        harness = make_harness(backend, conn, names)
        harness.call_macro("virtual_acquire_lock", env=env, owner=owner, ttl_minutes=ttl)
        return owner

    return job


def _locks(backend: Backend, names: Names, env: str) -> list[str]:
    return [
        r[0]
        for r in read(
            backend,
            f"SELECT owner FROM {backend.qualified(names.physical, '_locks')} "
            f"WHERE environment = '{env}' ORDER BY owner",
        )
    ]


def scenario_lock_race_has_exactly_one_winner(backend: Backend, n: int = 5, rounds: int = 3) -> None:
    """n runners acquire the lease on one environment at the same moment.

    Catches: an acquire that lets two runners both hold the lock (the old start-time ordering did,
    under MVCC); a loser that fails with something other than the lock-conflict message (or, on
    DuckDB, the unique violation of the second concurrent insert); a loser that leaves its lease row
    behind (which would block the winner's successors until the TTL).
    """
    names = new_world(backend, "lock")
    for r in range(rounds):
        env = f"prod_r{r}"
        outcomes = run_parallel(backend, [_acquire(backend, names, env, f"w{i}") for i in range(n)])
        winners = [o.value for o in outcomes if o.ok]
        assert len(winners) == 1, f"round {r}: {len(winners)} runners hold the lock: {winners}; {[o.message for o in outcomes if not o.ok]}"
        for o in outcomes:
            if not o.ok:
                assert lost_the_lock(backend, o.message), f"round {r}: loser failed with {o.message!r}"
        assert _locks(backend, names, env) == winners, (_locks(backend, names, env), winners)


def scenario_legacy_locks_table_is_upgraded(backend: Backend) -> None:
    """A `_locks` table from v0.1.0/v0.2.0 (no unique key, maybe two leases for one environment) is
    deduplicated and keyed by the first acquire; the lease then excludes a second owner.

    Catches: `ON CONFLICT (environment)` failing on a registry created before the unique index.
    Runs on DuckDB and PostgreSQL only (the other warehouses have no index to add).
    """
    names = new_world(backend, "legacy")
    locks = backend.qualified(names.physical, "_locks")
    conn = backend.new_conn()
    try:
        conn.run(f"DROP INDEX {backend.qualified(names.physical, '_locks_environment_uq')}")
        conn.run(
            f"INSERT INTO {locks} VALUES ('prod', 'old1', {backend.ts(-3)}, {backend.ts(10)}); "
            f"INSERT INTO {locks} VALUES ('prod', 'old2', {backend.ts(-2)}, {backend.ts(10)})"
        )
    finally:
        conn.close()
    outcome = run_parallel(backend, [_acquire(backend, names, "prod", "old2")])[0]
    assert outcome.ok, outcome.message  # the latest lease (old2) was kept, so its owner refreshes it
    assert _locks(backend, names, "prod") == ["old2"], _locks(backend, names, "prod")
    other = run_parallel(backend, [_acquire(backend, names, "prod", "new")])[0]
    assert not other.ok and LOCK_CONFLICT in other.message, other.message


def scenario_lease_lifecycle(backend: Backend) -> None:
    """Live lease blocks, non-owner release is a no-op, owner release frees, expired lease is taken over.

    Catches: a release that frees someone else's lease, an expired lease that still blocks (the
    environment would stay locked until someone edits `_locks` by hand), a takeover that leaves the
    dead lease row behind.
    """
    names = new_world(backend, "lease")
    conn = backend.new_conn()
    try:
        def call(macro: str, **kwargs):
            return make_harness(backend, conn, names).call_macro(macro, **kwargs)

        call("virtual_acquire_lock", env="prod", owner="a", ttl_minutes=15)
        try:
            call("virtual_acquire_lock", env="prod", owner="b", ttl_minutes=15)
        except RuntimeError as exc:
            assert LOCK_CONFLICT in str(exc) and "'a'" in str(exc), str(exc)
        else:
            raise AssertionError("a live lease did not block a second owner")
        assert _locks(backend, names, "prod") == ["a"], "the loser left a lease row behind"

        call("virtual_release_lock", env="prod", owner="b")
        assert _locks(backend, names, "prod") == ["a"], "release by a non-owner freed the lease"
        call("virtual_release_lock", env="prod", owner="a")
        assert _locks(backend, names, "prod") == []

        # An expired lease (its runner died) is taken over, and its row is removed.
        conn.run(
            f"INSERT INTO {backend.qualified(names.physical, '_locks')} "
            f"(environment, owner, acquired_at, expires_at) VALUES "
            f"('prod', 'dead', {backend.ts(-60)}, {backend.ts(-30)})"
        )
        call("virtual_acquire_lock", env="prod", owner="c", ttl_minutes=15)
        assert _locks(backend, names, "prod") == ["c"], _locks(backend, names, "prod")

        # An expired lease on another environment is not touched by an acquire on `prod`.
        conn.run(
            f"INSERT INTO {backend.qualified(names.physical, '_locks')} "
            f"(environment, owner, acquired_at, expires_at) VALUES "
            f"('qa', 'dead', {backend.ts(-60)}, {backend.ts(-30)})"
        )
        call("virtual_release_lock", env="prod", owner="c")
        call("virtual_acquire_lock", env="prod", owner="d", ttl_minutes=15)
        assert _locks(backend, names, "qa") == ["dead"]
    finally:
        conn.close()


# --------------------------------------------------------------------------------------------
# Promotion under the lock
# --------------------------------------------------------------------------------------------

PROMOTE_MODELS = ("a", "b")


def seed_releases(backend: Backend, names: Names, releases: tuple[str, ...]) -> None:
    """Staging releases `releases`: one physical table per model and release, sealed registry rows."""
    conn = backend.new_conn()
    try:
        for idx, release in enumerate(releases):
            for model in PROMOTE_MODELS:
                fp = f"{release}{model}".ljust(12, "0")[:12]
                table = f"{model}__{fp}"
                conn.run(
                    f"CREATE TABLE {backend.qualified(names.physical, table)} AS SELECT {idx + 1} AS v"
                )
                conn.run(
                    f"INSERT INTO {backend.qualified(names.physical, '_releases')} "
                    "(release_id, source_environment, target_environment, model_name, resource_type, "
                    "dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status, "
                    "created_at, promoted_at, rolled_back_at, schema_key, build_quality, sealed_at) VALUES "
                    f"('{release}', 'staging', NULL, '{model}', 'model', 2, '{fp}', "
                    f"'{names.physical}.{table}', NULL, 'candidate', CURRENT_TIMESTAMP, NULL, NULL, NULL, "
                    "'full', CURRENT_TIMESTAMP)"
                )
    finally:
        conn.close()


def _promote(backend: Backend, names: Names, release: str, owner: str):
    def job(conn: Conn):
        harness = make_harness(backend, conn, names)
        harness.call_macro(
            "virtual_promote", from_env="staging", to_env="prod", release_id=release,
            lock=True, lock_owner=owner,
        )
        return release

    return job


def _prod_state(backend: Backend, names: Names) -> list[tuple]:
    return read(
        backend,
        f"SELECT model_name, fingerprint, release_id FROM {backend.qualified(names.physical, '_environments')} "
        "WHERE environment = 'prod' ORDER BY model_name",
    )


def _release_statuses(backend: Backend, names: Names) -> list[tuple]:
    return read(
        backend,
        f"SELECT release_id, model_name, status, target_environment "
        f"FROM {backend.qualified(names.physical, '_releases')} ORDER BY release_id, model_name",
    )


def scenario_promote_is_refused_while_the_lease_is_held(backend: Backend) -> None:
    """A promote that meets a live lease fails cleanly and changes nothing; it works once released.

    Catches: a promote that starts changing views/pointers before it holds the lock, or a refusal
    that leaves partial state (the prod pointers and release statuses are compared before/after).
    """
    names = new_world(backend, "prom")
    seed_releases(backend, names, ("r1", "r2"))
    conn = backend.new_conn()
    try:
        _promote(backend, names, "r1", "runner-1")(conn)
        before = (_prod_state(backend, names), _release_statuses(backend, names))
        assert [row[2] for row in before[0]] == ["r1", "r1"], before

        make_harness(backend, conn, names).call_macro("virtual_acquire_lock", env="prod", owner="in-flight")
        try:
            _promote(backend, names, "r2", "runner-2")(conn)
        except RuntimeError as exc:
            assert LOCK_CONFLICT in str(exc), str(exc)
        else:
            raise AssertionError("promote ran while another runner held the lease")
        after = (_prod_state(backend, names), _release_statuses(backend, names))
        assert after == before, f"a refused promote changed state:\nbefore={before}\nafter={after}"
        assert _locks(backend, names, "prod") == ["in-flight"]

        make_harness(backend, conn, names).call_macro("virtual_release_lock", env="prod", owner="in-flight")
        _promote(backend, names, "r2", "runner-2")(conn)
        assert [row[2] for row in _prod_state(backend, names)] == ["r2", "r2"]
        assert _locks(backend, names, "prod") == [], "a successful promote left its lease behind"
    finally:
        conn.close()


def scenario_simultaneous_promotes(backend: Backend, rounds: int = 2) -> None:
    """Two promotes of different releases start at the same moment.

    Either order is acceptable (the second may run after the first finished), but never a mix:
    prod must end on ONE complete release, every failure must be the lock conflict, nobody leaves
    a lease behind, and at least one promote succeeds.
    """
    names = new_world(backend, "race")
    seed_releases(backend, names, ("r1", "r2", "r3"))
    conn = backend.new_conn()
    try:
        _promote(backend, names, "r1", "baseline")(conn)
    finally:
        conn.close()
    for r in range(rounds):
        outcomes = run_parallel(
            backend,
            [_promote(backend, names, "r2", f"runner-a{r}"), _promote(backend, names, "r3", f"runner-b{r}")],
        )
        ok = [o.value for o in outcomes if o.ok]
        assert ok, f"round {r}: no promote succeeded: {[o.message for o in outcomes]}"
        for o in outcomes:
            if not o.ok:
                assert lost_the_lock(backend, o.message), f"round {r}: failed with a non-lock error: {o.message}"
        state = _prod_state(backend, names)
        assert len(state) == 2 and state[0][2] == state[1][2], f"round {r}: prod mixes releases: {state}"
        assert state[0][2] == ok[-1] or state[0][2] in ok, (state, ok)
        assert _locks(backend, names, "prod") == [], f"round {r}: lease left behind"
        view = read(backend, f"SELECT v FROM {backend.qualified(names.prod, 'a')}")
        assert view == [({"r1": 1, "r2": 2, "r3": 3}[state[0][2]],)], (view, state)


REGISTRY_MERGE_XFAIL = (
    "the default (DuckDB/PostgreSQL) `_environments` upsert is DELETE then INSERT, so two writers of one "
    "(environment, model) can leave two rows; fixed by branch claude/dbt-virtual-registry-merge (MERGE). "
    "Remove this xfail when that lands: a pass is reported as a failure (strict)."
)


def run_checks(checks: list[tuple], label: str = "") -> list[str]:
    """Run `(name, fn[, xfail_reason])` checks, print one line each, return the unexpected outcomes.

    A check with an `xfail_reason` must fail; passing counts as unexpected (like pytest's strict xfail).
    """
    import traceback

    unexpected: list[str] = []
    prefix = f"[{label}] " if label else ""
    for name, fn, *rest in checks:
        xfail = rest[0] if rest else None
        try:
            fn()
        except Exception:  # noqa: BLE001 - every failing check is reported
            if xfail:
                print(f"{prefix}XFAIL {name}: {xfail}")
            else:
                unexpected.append(name)
                print(f"{prefix}FAIL  {name}")
                traceback.print_exc()
        else:
            if xfail:
                unexpected.append(name)
                print(f"{prefix}XPASS {name}: expected to fail ({xfail})")
            else:
                print(f"{prefix}PASS  {name}")
    return unexpected


def scenario_same_owner_reacquires_the_lease(backend: Backend) -> None:
    """Pins TECH_DEBT(cli_lock_owner_shared_per_user_host): the same owner string is re-entrant.

    `dbt-virtual promote` defaults `--lock-owner` to `dbt-virtual-cli:<user>@<host>`, the same for
    every process of one user on one machine. A second acquire with that owner refreshes the lease
    and succeeds, so two promotes started from one machine do not exclude each other. When the
    owner becomes unique per process, flip this scenario to expect a lock conflict.
    """
    names = new_world(backend, "owner")
    # One after the other: truly simultaneous inserts of one owner can lose the race on a warehouse
    # with a unique key (DuckDB), which is the safe outcome; the limitation is the re-entrant refresh.
    outcomes = [
        run_parallel(backend, [_acquire(backend, names, "prod", "dbt-virtual-cli:me@host")])[0]
        for _ in range(2)
    ]
    assert [o.ok for o in outcomes] == [True, True], [o.message for o in outcomes]
    assert set(_locks(backend, names, "prod")) == {"dbt-virtual-cli:me@host"}
