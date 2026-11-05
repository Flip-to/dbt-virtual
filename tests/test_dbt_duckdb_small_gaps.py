"""Small gaps closed on top of the production-readiness branch, through real dbt-core + dbt-duckdb.

- `ref_resolution: explicit` used to behave exactly like `physical`; it is now rejected;
- `virtual_export_state` (`dbt-virtual status`) failed on a project whose registry did not exist yet.
"""

from __future__ import annotations

import json

import duckdb
import pytest

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project, _table_model


@pytest.fixture
def make_project(tmp_path):
    def _make(models: dict[str, str]) -> Project:
        return Project(tmp_path, models)

    return _make


def test_ref_resolution_explicit_is_rejected_with_a_clear_message(tmp_path) -> None:
    yml = PROJECT_YML.replace(
        'base_environment: "prod"', 'base_environment: "prod"\n    ref_resolution: "explicit"'
    )
    proj = Project(
        tmp_path,
        {"up": _table_model("select 1 as x"), "down": _table_model("select x from {{ ref('up') }}")},
        project_yml=yml,
    )
    # Before the change `explicit` compiled to the physical relation, like `physical`.
    for command in ("parse", "compile"):
        res = proj.dbt(command, "--target", "staging", expect_ok=False)
        assert res.returncode != 0, command
        assert "ref_resolution 'explicit' is not supported" in res.stdout + res.stderr


def test_ref_resolution_unknown_value_is_rejected(tmp_path) -> None:
    yml = PROJECT_YML.replace(
        'base_environment: "prod"', 'base_environment: "prod"\n    ref_resolution: "enviroment"'
    )
    proj = Project(
        tmp_path,
        {"up": _table_model("select 1 as x"), "down": _table_model("select x from {{ ref('up') }}")},
        project_yml=yml,
    )
    res = proj.dbt("parse", "--target", "staging", expect_ok=False)
    assert res.returncode != 0
    assert "unknown ref_resolution 'enviroment'" in res.stdout + res.stderr


def test_ref_resolution_physical_still_compiles_to_the_physical_table(tmp_path) -> None:
    yml = PROJECT_YML.replace(
        'base_environment: "prod"', 'base_environment: "prod"\n    ref_resolution: "physical"'
    )
    proj = Project(
        tmp_path,
        {"up": _table_model("select 1 as x"), "down": _table_model("select x from {{ ref('up') }}")},
        project_yml=yml,
    )
    fps = proj.fingerprints("staging")
    proj.dbt("compile", "--target", "staging", "--vars", json.dumps({"_dbt_virtual_fps": fps}))
    compiled = (proj.dir / "target" / "compiled" / "regress" / "models" / "down.sql").read_text("utf-8")
    assert f"analytics_physical.up__{fps['up']}" in compiled.replace('"', "")


def _state(stdout: str) -> dict:
    marker = "DBT_VIRTUAL_STATE_JSON:"
    line = next(line for line in stdout.splitlines() if marker in line)
    return json.loads(line.split(marker, 1)[1])


EMPTY_STATE = {
    "registered_physical_tables": [],
    "env_state": {},
    "base_env_state": {},
    "existing_physical_tables": [],
}


def test_status_on_a_project_without_a_registry_returns_empty_state(make_project, tmp_path) -> None:
    proj = make_project({"a": _table_model("select 1 as a")})
    # The physical schema exists but nothing was ever built: no registry tables.
    assert [n for n, _ in proj.relations("analytics_physical")] == []
    res = proj.operation("virtual_export_state", {"env": "staging", "base_env": "prod"})
    state = _state(res.stdout)
    assert {k: state[k] for k in EMPTY_STATE} == EMPTY_STATE
    # Nothing is created by a read-only status.
    assert [n for n, _ in proj.relations("analytics_physical")] == []

    out = tmp_path / "state.json"
    assert proj.cli("status", "--env", "staging", "--output", str(out)) == 0
    assert {k: json.loads(out.read_text("utf-8"))[k] for k in EMPTY_STATE} == EMPTY_STATE


def test_status_when_the_physical_schema_does_not_exist_returns_empty_state(make_project) -> None:
    proj = make_project({"a": _table_model("select 1 as a")})
    con = duckdb.connect(proj.db_path)
    con.execute("DROP SCHEMA analytics_physical CASCADE")
    con.close()
    res = proj.operation("virtual_export_state", {"env": "staging", "base_env": "prod"})
    state = _state(res.stdout)
    assert {k: state[k] for k in EMPTY_STATE} == EMPTY_STATE


def test_status_after_a_build_still_reports_registered_state(make_project) -> None:
    proj = make_project({"a": _table_model("select 1 as a")})
    fps = proj.build("staging", "r1")
    res = proj.operation("virtual_export_state", {"env": "staging", "base_env": "prod"})
    state = _state(res.stdout)
    assert state["env_state"] == {"a": fps["a"]}
    assert state["registered_physical_tables"] == [f"a__{fps['a']}"]
