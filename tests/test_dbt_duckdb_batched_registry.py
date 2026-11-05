"""Batched registry writes and hook-driven `dbt-virtual build`, run through real dbt-core + DuckDB.

See docs/decisions/20261104_batched_registry_writes.md and
docs/decisions/20261104_single_dbt_build_invocation.md.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests.test_dbt_duckdb_regressions import (
    INCREMENTAL_V1,
    INCREMENTAL_V2,
    PROJECT_YML,
    Project,
    _table_model,
)

N_TABLES = 6


def _models() -> dict[str, str]:
    models = {"t0": _table_model("select 0 as v")}
    for i in range(1, N_TABLES):
        models[f"t{i}"] = _table_model(f"select v + {i} as v from {{{{ ref('t{i - 1}') }}}}")
    models["add_one"] = (
        "{{ config(materialized='virtual_function', parameters='x') }}\n(x + 1)\n"
    )
    return models


def _build(proj: Project, env: str, release: str, *, start: str = "", end: str = "") -> object:
    fps = proj.fingerprints(env)
    dbt_vars = {"_dbt_virtual_fps": fps, "_dbt_virtual_env": env, "_dbt_virtual_release": release}
    if start:
        dbt_vars.update({"_dbt_virtual_start": start, "_dbt_virtual_end": end})
    return proj.dbt("build", "--target", env, "--vars", json.dumps(dbt_vars), expect_ok=False)


def test_parallel_build_registers_each_object_once_in_batch(tmp_path: Path) -> None:
    proj = Project(tmp_path, _models(), threads=4)
    res = _build(proj, "staging", "r1", start="2026-10-01", end="2026-10-27")
    assert res.returncode == 0, res.stdout + res.stderr

    # Before the change every model ran its own MERGE/INSERTs from its post-hook.
    assert "registering 7 object(s)" in res.stdout
    assert "no on-run-end hook" not in res.stdout

    env_rows = proj.query(
        "SELECT model_name, resource_type, physical_identifier FROM analytics_physical._environments "
        "WHERE environment = 'staging' ORDER BY 1"
    )
    assert [r[0] for r in env_rows] == sorted([f"t{i}" for i in range(N_TABLES)] + ["add_one"])
    assert {r[0]: r[1] for r in env_rows}["add_one"] == "function"
    for name, rtype, physical in env_rows:
        if rtype == "model":
            assert physical.startswith(f"analytics_physical.{name}__")

    releases = proj.query(
        "SELECT model_name, resource_type, routine_ddl_template FROM analytics_physical._releases "
        "WHERE release_id = 'r1' ORDER BY 1"
    )
    assert [r[0] for r in releases] == [r[0] for r in env_rows]  # exactly one row per object
    templates = {name: tpl for name, _rtype, tpl in releases}
    assert "add_one" in templates["add_one"] and "x + 1" in templates["add_one"]
    assert templates["t0"] is None

    intervals = proj.query(
        "SELECT model_name, start_ts, end_ts FROM analytics_physical._intervals ORDER BY 1"
    )
    assert [r[0] for r in intervals] == [f"t{i}" for i in range(N_TABLES)]
    assert {(r[1], r[2]) for r in intervals} == {("2026-10-01", "2026-10-27")}

    assert proj.query(f"SELECT v FROM analytics_staging.t{N_TABLES - 1}") == [(15,)]
    assert proj.query("SELECT analytics_staging.add_one(41)") == [(42,)]


def test_failed_model_is_not_registered_but_the_others_are(tmp_path: Path) -> None:
    models = _models()
    models["broken"] = _table_model("select * from table_that_does_not_exist")
    proj = Project(tmp_path, models, threads=4)
    res = _build(proj, "staging", "r1")
    assert res.returncode != 0

    registered = {
        r[0]
        for r in proj.query(
            "SELECT model_name FROM analytics_physical._environments WHERE environment = 'staging'"
        )
    }
    assert registered == {f"t{i}" for i in range(N_TABLES)} | {"add_one"}
    released = {
        r[0]
        for r in proj.query("SELECT model_name FROM analytics_physical._releases WHERE release_id = 'r1'")
    }
    assert released == registered
    assert "broken" not in {name for name, _ in proj.relations("analytics_staging")}


def test_per_model_mode_still_writes_from_the_post_hooks(tmp_path: Path) -> None:
    yml = PROJECT_YML.replace(
        '    base_environment: "prod"\n',
        '    base_environment: "prod"\n    registry_write_mode: per_model\n',
    )
    assert yml != PROJECT_YML
    # threads=1: per-model mode creates the registry in the first post-hooks, which races on 4 threads
    # ("Catalog write-write conflict" on DuckDB); batch mode creates it once in on-run-end.
    proj = Project(tmp_path, _models(), project_yml=yml)
    res = _build(proj, "staging", "r1")
    assert res.returncode == 0, res.stdout + res.stderr
    assert "registering" not in res.stdout  # flush_registry did nothing
    count = proj.query(
        "SELECT count(*) FROM analytics_physical._releases WHERE release_id = 'r1'"
    )
    assert count == [(N_TABLES + 1,)]


def test_run_start_hook_seeds_the_clones_the_cli_passes(tmp_path: Path) -> None:
    # No project-level `seed_incremental_clones()`: only the package hook reads `_dbt_virtual_seed`.
    yml = PROJECT_YML.replace(
        'on-run-start:\n  - "{{ dbt_virtual.seed_incremental_clones() }}"\n', ""
    )
    assert yml != PROJECT_YML
    proj = Project(tmp_path, {"events": INCREMENTAL_V1}, project_yml=yml)
    assert _build(proj, "prod", "r1").returncode == 0
    proj.write_models({"events": INCREMENTAL_V2})

    fps = proj.fingerprints("staging")
    dbt_vars = {
        "_dbt_virtual_fps": fps,
        "_dbt_virtual_env": "staging",
        "_dbt_virtual_release": "r2",
        "_dbt_virtual_seed": ["events"],
    }
    proj.dbt("build", "--target", "staging", "--vars", json.dumps(dbt_vars))
    # History from the prod clone (lower case); only the new row ran through v2.
    assert proj.query("SELECT id, v FROM analytics_staging.events ORDER BY id") == [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "D"),
    ]


def test_cli_build_with_nothing_to_build_still_syncs_views_and_completes_the_release(
    tmp_path: Path,
) -> None:
    proj = Project(tmp_path, {"a": _table_model("select 1 as a"), "b": _table_model("select 2 as b")})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    proj.dbt("run-operation", "virtual_promote", "--target", "staging", "--args",
             json.dumps({"from_env": "staging", "to_env": "prod", "release_id": "r1"}))
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {"env_state": {n: f for n, f in proj.fingerprints("staging").items()}},
        ),
        encoding="utf-8",
    )
    # Every model is REUSE: one run-operation (no dbt build) must record release r2 completely.
    assert proj.cli("build", "--env", "staging", "--release", "r2", "--state-file", str(state)) == 0
    rows = proj.query(
        "SELECT model_name FROM analytics_physical._releases WHERE release_id = 'r2' ORDER BY 1"
    )
    assert rows == [("a",), ("b",)]
    assert proj.relations("analytics_staging") == [("a", "VIEW"), ("b", "VIEW")]
