"""Findings of a real BigQuery pilot, reproduced through real dbt-core + dbt-duckdb.

- `build --select` overrode the REUSE exclusion: a selector such as `tag:x` made dbt build (and
  overwrite) tables the plan had decided to only point at;
- `promote` decided "audit by default" from the raw `base_environment`, so a Jinja value never matched;
- `--empty` / `DBT_EMPTY=true` changed the compiled SQL and therefore every fingerprint.
"""

from __future__ import annotations

import json

import duckdb
import pytest

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project

TAGGED = "{{ config(materialized='table', tags=['dbt_virtual']) }}\n"


def _model(sql: str) -> str:
    return TAGGED + sql + "\n"


def _physical(proj: Project, name: str) -> str:
    rows = proj.query(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = 'analytics_physical' AND table_name LIKE '{name}__%'"
    )
    assert len(rows) == 1, rows
    return rows[0][0]


def _empty_physical_table(proj: Project, table: str) -> None:
    """Delete the rows of a physical table: a rebuild of it would bring them back."""
    con = duckdb.connect(proj.db_path)
    try:
        con.execute(f"DELETE FROM analytics_physical.{table}")
    finally:
        con.close()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch) -> None:
    monkeypatch.delenv("DBT_EMPTY", raising=False)


def test_build_select_does_not_rebuild_reuse_models(tmp_path) -> None:
    proj = Project(tmp_path, {"a": _model("select 1 as a"), "b": _model("select 10 as b")})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    a_table = _physical(proj, "a")
    _empty_physical_table(proj, a_table)

    state = tmp_path / "state.json"
    assert proj.cli("status", "--env", "staging", "--output", str(state)) == 0
    proj.write_models({"b": _model("select 20 as b")})
    args = ["build", "--env", "staging", "--state-file", str(state), "--select", "tag:dbt_virtual"]
    assert proj.cli(*args) == 0

    # Before the fix: `a` (REUSE) was rebuilt by the tag selector and had 1 row again.
    assert proj.query(f"SELECT count(*) FROM analytics_physical.{a_table}") == [(0,)]
    assert proj.query("SELECT b FROM analytics_staging.b") == [(20,)]


def test_build_select_with_only_reuse_models_builds_nothing(tmp_path) -> None:
    proj = Project(tmp_path, {"a": _model("select 1 as a")})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    a_table = _physical(proj, "a")
    _empty_physical_table(proj, a_table)
    state = tmp_path / "state.json"
    assert proj.cli("status", "--env", "staging", "--output", str(state)) == 0
    args = ["build", "--env", "staging", "--state-file", str(state), "--select", "tag:dbt_virtual"]
    assert proj.cli(*args) == 0
    assert proj.query(f"SELECT count(*) FROM analytics_physical.{a_table}") == [(0,)]


JINJA_BASE_YML = PROJECT_YML.replace(
    'base_environment: "prod"',
    "base_environment: \"{{ 'shared_with_prod' if target.name == 'staging' else 'other' }}\"",
)
assert JINJA_BASE_YML != PROJECT_YML

_TABLE_M = "{{{{ config(materialized='table') }}}}" + chr(10) + "{sql}" + chr(10)
PROMOTE = ["promote", "--from", "staging", "--to", "shared_with_prod", "--compile-target", "staging"]


def test_promote_audits_by_default_when_base_environment_is_jinja(tmp_path) -> None:
    proj = Project(
        tmp_path,
        {"m": _TABLE_M.format(sql="select 1 as id")},
        project_yml=JINJA_BASE_YML,
    )
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    assert proj.cli(*PROMOTE, "--release", "r1") == 0

    # r2 produces 0 rows while the target has 1: only the audit refuses that.
    proj.write_models({"m": _TABLE_M.format(sql="select 1 as id where false")})
    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0
    # Before the fix the Jinja value was read as the default `prod`, never equal to `--to`, so the
    # audit was off and r2 was promoted.
    assert proj.cli(*PROMOTE, "--release", "r2") != 0
    assert proj.query("SELECT count(*) FROM analytics.m") == [(1,)]


SOURCES = """version: 2
sources:
  - name: raw
    schema: analytics_staging
    tables:
      - name: t
"""


def test_fingerprints_do_not_depend_on_dbt_empty(tmp_path, monkeypatch) -> None:
    proj = Project(
        tmp_path,
        {"c": _model("select * from {{ source('raw', 't') }}"), "d": _model("select 1 as d")},
    )
    (proj.dir / "models" / "sources.yml").write_text(SOURCES, encoding="utf-8")

    def plan_fps() -> dict[str, str]:
        assert proj.cli("plan", "--env", "staging") == 0
        data = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))
        return data["by_name"]

    normal = plan_fps()
    monkeypatch.setenv("DBT_EMPTY", "true")
    # Before the fix `c` compiled to `(select * from ... where false limit 0)` and changed.
    assert plan_fps() == normal
