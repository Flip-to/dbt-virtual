"""Real dbt-core + dbt-duckdb: registry upserts replace a pointer in one statement and
`exec_atomic` commits several statements together or not at all (DECISION(atomic_registry_upserts))."""

from __future__ import annotations

import pytest

from tests.test_dbt_duckdb_regressions import Project, _table_model

PROBE = """{% macro probe_atomic_failure() %}
  {% do dbt_virtual.exec_atomic([
    "INSERT INTO analytics_physical._locks VALUES ('probe', 'owner', now(), now())",
    "INSERT INTO analytics_physical.no_such_table VALUES (1)"
  ]) %}
{% endmacro %}

{% macro probe_atomic_success() %}
  {% do dbt_virtual.exec_atomic([
    "INSERT INTO analytics_physical._locks VALUES ('probe_ok', 'owner', now(), now())",
    "DELETE FROM analytics_physical._locks WHERE environment = 'probe_ok'",
    "INSERT INTO analytics_physical._locks VALUES ('probe_ok', 'owner2', now(), now())"
  ]) %}
{% endmacro %}
"""


@pytest.fixture
def project(tmp_path) -> Project:
    proj = Project(tmp_path, {"a": _table_model("select 1 as a"), "b": _table_model("select 2 as b")})
    (proj.dir / "macros" / "atomic_probe.sql").write_text(PROBE, encoding="utf-8")
    return proj


def test_rebuilds_keep_one_pointer_row_per_model(project: Project) -> None:
    assert project.cli("build", "--env", "staging", "--release", "r1") == 0
    project.write_models({"a": _table_model("select 10 as a")})
    assert project.cli("build", "--env", "staging", "--release", "r2", "--select", "a") == 0
    rows = project.query(
        "SELECT model_name, release_id, build_quality FROM analytics_physical._environments "
        "WHERE environment = 'staging' ORDER BY 1"
    )
    assert [r[0] for r in rows] == ["a", "b"], "MERGE updated `a` in place; no duplicate row"
    assert {r[0]: r[1] for r in rows} == {"a": "r2", "b": "r1"}


def test_exec_atomic_rolls_back_every_statement_when_one_fails(project: Project) -> None:
    assert project.cli("build", "--env", "staging", "--release", "r1") == 0
    res = project.operation("probe_atomic_failure", {}, expect_ok=False)
    assert res.returncode != 0
    assert project.query("SELECT count(*) FROM analytics_physical._locks WHERE environment = 'probe'") == [(0,)]


def test_exec_atomic_commits_all_statements(project: Project) -> None:
    assert project.cli("build", "--env", "staging", "--release", "r1") == 0
    project.operation("probe_atomic_success", {})
    assert project.query(
        "SELECT owner FROM analytics_physical._locks WHERE environment = 'probe_ok'"
    ) == [("owner2",)]
