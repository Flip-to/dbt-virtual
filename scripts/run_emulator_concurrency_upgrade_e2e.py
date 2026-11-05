"""Concurrency and upgrade checks against the BigQuery emulator.

Runs the real rendered macros through `MacroTestHarness` with every SQL statement sent to a live
BigQuery emulator (`BIGQUERY_EMULATOR_HOST`, default `http://127.0.0.1:9050`, project `test-project`).
Each check creates its own datasets, so the script can be re-run on one emulator instance.

A. Concurrency (scenarios shared with DuckDB and PostgreSQL, `tests/registry_concurrency.py`):
   - concurrent `flush_registry` calls for different environments and for the same one,
   - interleaved upserts of one pointer,
   - the `_locks` lease: simultaneous acquire, lifecycle, expired lease takeover,
   - `virtual_promote(lock=True)`: refused while the lease is held, simultaneous promotes.
B. Upgrade from v0.1.0: a registry written by the v0.1.0 macros (git tag `v0.1.0`) is read, migrated,
   promoted and rolled back by the current macros.

Limits of the emulator: it does not serialize or conflict DML the way BigQuery does ("Could not
serialize access to table ... due to concurrent update"), so conflict retries are not exercised here.
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tests import registry_concurrency as rc
from tests.legacy_release import export_release, legacy_harness, tag_available
from tests.test_macros_execution import FakeRelation


def run_upgrade(backend: rc.BigQueryBackend, project: str, release_dir: Path) -> None:
    """Registry written by the v0.1.0 macros, then read/migrated/promoted/rolled back by this checkout."""
    sfx = uuid.uuid4().hex[:6]
    names = rc.Names(f"up_{sfx}_phys", f"up_{sfx}_stg", f"up_{sfx}_prod")
    for schema in (names.physical, names.stg, names.prod):
        backend.create_schema(schema)
    conn = backend.new_conn()

    def cols(table: str) -> list[str]:
        return [f.name for f in backend.client.get_table(f"{project}.{names.physical}.{table}").schema]

    def scalar(sql: str):
        return conn.rows(sql)[0][0]

    def orders(schema: str) -> int:
        return scalar(f"SELECT n FROM `{project}.{schema}.orders`")

    fps = {"v1": "aaaaaaaaaaa1", "v2": "aaaaaaaaaaa2", "v3": "aaaaaaaaaaa3"}
    node = {
        "unique_id": "model.up.orders", "name": "orders", "resource_type": "model",
        "config": {"materialized": "table", "virtual_env": True},
        "compiled_code": "select 1", "raw_code": "select 1",
    }

    def legacy(release: str):
        h = legacy_harness(
            release_dir,
            vars_dict={
                "dbt_virtual": {
                    "physical_schema": names.physical, "base_environment": "prod",
                    "env_schemas": {"staging": names.stg, "prod": names.prod},
                },
                "_dbt_virtual_fps": {"orders": fps[release]},
                "_dbt_virtual_release": release,
            },
            target_name="staging", target_schema=names.stg, target_database=project,
            adapter_type="bigquery",
        )
        h.query_executor, h.relation_lookup = conn.run, conn.lookup
        h.adapter_extras = backend.adapter_extras(conn)
        h.model = node
        h.this = FakeRelation(project, names.physical, f"orders__{fps[release]}", "table", "`")
        return h

    # --- v0.1.0 builds and promotes v1 and v2 ---
    conn.run(legacy("v1").call_macro("ensure_registry_tables_sql", names.physical))
    for n, release in ((1, "v1"), (2, "v2")):
        conn.run(f"CREATE TABLE `{project}.{names.physical}.orders__{fps[release]}` AS SELECT {n} AS n")
        h = legacy(release)
        conn.run(h.call_macro("sync_env_view"))
        h.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id=release)
        assert orders(names.prod) == n, f"v0.1.0 promote of {release} did not repoint prod"
    assert "sealed_at" not in cols("_releases") and "schema_key" not in cols("_releases"), cols("_releases")
    assert "build_quality" not in cols("_environments"), cols("_environments")
    print("[upgrade] v0.1.0 registry written: releases v1, v2 promoted; no schema_key/build_quality/sealed_at.")

    # --- current macros ---
    def current(release: str = "", fp_map: dict | None = None, nodes: dict | None = None):
        h = rc.make_harness(backend, conn, names, release=release, fps=fp_map, nodes=nodes)
        h.run_statement_blocks = True
        return h

    # status first: used to fail with "Unrecognized name: build_quality".
    h = current()
    h.call_macro("virtual_export_state", env="prod", base_env="prod")
    state_logs = [m for m in h.logs if m.startswith("DBT_VIRTUAL_STATE_JSON:")]
    assert state_logs and "orders" in state_logs[0] and fps["v2"] in state_logs[0], state_logs
    assert {"schema_key", "build_quality", "sealed_at"} <= set(cols("_releases")), cols("_releases")
    assert {"schema_key", "build_quality"} <= set(cols("_environments")), cols("_environments")
    print("[upgrade] status on the v0.1.0 registry works and migrates it (idempotent).")
    current().call_macro("migrate_registry_if_old", phys_schema=names.physical)  # second run: no-op

    legacy_rows = conn.rows(
        f"SELECT release_id, model_name, sealed_at, build_quality FROM `{project}.{names.physical}._releases` "
        "ORDER BY release_id"
    )
    assert [(r[0], r[1], r[2], r[3]) for r in legacy_rows] == [
        ("v1", "orders", None, None), ("v2", "orders", None, None),
    ], legacy_rows

    # v3 built by the current macros (batched registry flush), sealed, promoted.
    conn.run(f"CREATE TABLE `{project}.{names.physical}.orders__{fps['v3']}` AS SELECT 3 AS n")
    conn.run(
        f"CREATE OR REPLACE VIEW `{project}.{names.stg}.orders` AS "
        f"SELECT * FROM `{project}.{names.physical}.orders__{fps['v3']}`"
    )
    v3_node = {
        "unique_id": "model.up.orders", "name": "orders", "resource_type": "model",
        "alias": f"orders__{fps['v3']}", "schema": names.physical, "database": project,
        "config": {"materialized": "table", "virtual_env": True}, "compiled_code": "select 3",
        "depends_on": {"nodes": []},
    }
    h = current("v3", {"orders": fps["v3"]}, {"model.up.orders": v3_node})
    h.call_macro("flush_registry", [{"node": v3_node, "status": "success"}])
    h.call_macro("virtual_seal_release", release_id="v3")
    h.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v3", require_sealed=True)
    assert orders(names.prod) == 3
    print("[upgrade] v3 built, sealed and promoted by the current macros on the migrated registry.")

    # An unsealed v0.1.0 release: refused only under require_sealed, with the actionable message.
    try:
        current().call_macro(
            "virtual_promote", from_env="staging", to_env="prod", release_id="v2", require_sealed=True
        )
    except RuntimeError as exc:
        assert "has no seal" in str(exc), str(exc)
    else:
        raise AssertionError("a release without a seal was promoted under require_sealed")
    assert orders(names.prod) == 3, "the refused promote changed prod"

    # Rollback to a release written by v0.1.0.
    current().call_macro("virtual_rollback", env="prod", to_release="v1")
    assert orders(names.prod) == 1, "rollback to a v0.1.0 release did not repoint prod"
    statuses = dict(
        conn.rows(
            f"SELECT release_id, MAX(status) FROM `{project}.{names.physical}._releases` "
            "WHERE target_environment = 'prod' GROUP BY release_id"
        )
    )
    assert statuses.get("v3") == "rolled_back", statuses
    current().call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v2")
    assert orders(names.prod) == 2
    print("[upgrade] rollback to the v0.1.0 release v1 and re-promote of v2 verified.")


def main() -> int:
    try:
        import requests  # noqa: F401
        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials
        from google.cloud import bigquery
    except ImportError as exc:
        print(f"Missing emulator dependencies ({exc}). Install with: uv sync --extra emulator", file=sys.stderr)
        return 1

    endpoint = os.environ.get("BIGQUERY_EMULATOR_HOST", "http://127.0.0.1:9050").rstrip("/")
    project = os.environ.get("BIGQUERY_EMULATOR_PROJECT", "test-project")
    client = bigquery.Client(
        project=project, credentials=AnonymousCredentials(), client_options=ClientOptions(api_endpoint=endpoint)
    )
    backend = rc.BigQueryBackend(client, project, endpoint)
    print(f"Connected to the BigQuery emulator at {endpoint} (project={project})")

    checks = [
        ("flushes of different environments", lambda: rc.scenario_flushes_of_different_environments(backend, n=4)),
        ("flushes of the same environment", lambda: rc.scenario_flushes_of_the_same_environment(backend, n=4, rounds=2)),
        ("interleaved pointer upserts", lambda: rc.scenario_interleaved_pointer_upserts(backend)),
        ("lock race: exactly one winner", lambda: rc.scenario_lock_race_has_exactly_one_winner(backend, n=4, rounds=2)),
        ("lease lifecycle and expired takeover", lambda: rc.scenario_lease_lifecycle(backend)),
        ("promote refused while lease held", lambda: rc.scenario_promote_is_refused_while_the_lease_is_held(backend)),
        ("simultaneous promotes", lambda: rc.scenario_simultaneous_promotes(backend, rounds=2)),
    ]
    if tag_available("v0.1.0"):
        import tempfile

        release_dir = export_release("v0.1.0", Path(tempfile.mkdtemp(prefix="dbt_virtual_v010_")))
        checks.append(("upgrade v0.1.0 -> current", lambda: run_upgrade(backend, project, release_dir)))
    else:
        print("git tag v0.1.0 not available: run `git fetch --tags` (upgrade check not run)", file=sys.stderr)
        return 1

    failures = rc.run_checks(checks)
    if failures:
        print(f"{len(failures)} check(s) FAILED: {', '.join(failures)}", file=sys.stderr)
        return 1
    print("All BigQuery emulator concurrency and upgrade checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
