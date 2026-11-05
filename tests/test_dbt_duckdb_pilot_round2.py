"""Findings of the second real-BigQuery pilot, reproduced through real dbt-core + dbt-duckdb.

- rollback to an adopt release failed on a legacy TABLE left at the environment path;
- a failed promote / rollback kept its lease lock;
- a new incremental was cloned twice per build (CLI seeding, then the on-run-start hook);
- `diff-data` bisected on the first `diff_key` entry even when it is not numeric.
"""

from __future__ import annotations

import json

import duckdb
import pytest

from tests.test_dbt_duckdb_regressions import Project

TABLE = "{{ config(materialized='table') }}\n"
MARKER = "DBT_VIRTUAL_DIFF_JSON:"


def _legacy_tables(proj: Project) -> None:
    """Tables an existing deployment has at the prod path (what `adopt` migrates)."""
    con = duckdb.connect(proj.db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        con.execute("CREATE TABLE analytics.a AS SELECT 1 AS x")
        con.execute("CREATE TABLE analytics.b AS SELECT 1 AS x")
    finally:
        con.close()


def _kind(proj: Project, name: str) -> str:
    rows = proj.query(
        f"SELECT table_type FROM information_schema.tables WHERE table_schema = 'analytics' AND table_name = '{name}'"
    )
    return rows[0][0]


def test_rollback_to_an_adopt_release_replaces_legacy_tables(tmp_path) -> None:
    proj = Project(tmp_path, {"a": TABLE + "select 1 as x\n", "b": TABLE + "select 1 as x\n"})
    _legacy_tables(proj)
    assert proj.cli("adopt", "--env", "prod", "--release", "adopt1", "--keep-existing-table") == 0
    assert _kind(proj, "a") == "BASE TABLE" and _kind(proj, "b") == "BASE TABLE"  # kept in place

    # Before the fix: "Existing object is of type Table, trying to replace with type View".
    assert proj.cli("rollback", "--env", "prod", "--to-release", "adopt1") == 0
    assert _kind(proj, "a") == "VIEW" and _kind(proj, "b") == "VIEW"
    assert proj.query("SELECT x FROM analytics.a") == [(1,)]


def test_pilot_sequence_adopt_change_one_build_promote_rollback(tmp_path) -> None:
    proj = Project(tmp_path, {"a": TABLE + "select 1 as x\n", "b": TABLE + "select 1 as x\n"})
    _legacy_tables(proj)
    assert proj.cli("adopt", "--env", "prod", "--release", "adopt1", "--keep-existing-table") == 0
    proj.write_models({"b": TABLE + "select 2 as x\n"})
    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0
    assert proj.cli("promote", "--release", "r2", "--audit") == 0
    # The unchanged model's legacy table is replaced by the view too (it used to stay a table).
    assert _kind(proj, "a") == "VIEW" and _kind(proj, "b") == "VIEW"
    assert proj.query("SELECT x FROM analytics.b") == [(2,)]

    assert proj.cli("rollback", "--env", "prod", "--to-release", "adopt1") == 0
    assert proj.query("SELECT x FROM analytics.a") == [(1,)]
    assert proj.query("SELECT x FROM analytics.b") == [(1,)]


def _locks(proj: Project) -> list[tuple]:
    return proj.query("SELECT environment, owner FROM analytics_physical._locks")


def test_failed_promote_and_rollback_release_the_lock(tmp_path) -> None:
    proj = Project(tmp_path, {"a": TABLE + "select 1 as x\n"})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    # Neither release exists: both operations fail after taking the lease.
    assert proj.cli("promote", "--release", "nope", "--skip-fingerprint-check", "--no-audit") != 0
    assert _locks(proj) == []
    assert proj.cli("rollback", "--env", "prod", "--to-release", "nope") != 0
    assert _locks(proj) == []
    # The retry needs no `--no-lock`.
    assert proj.cli("promote", "--release", "r1", "--no-audit") == 0
    assert _locks(proj) == []


INCREMENTAL_V1 = """{{ config(materialized='incremental', unique_key='id') }}
select i as id from range(1, 6) t(i)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""
INCREMENTAL_V2 = INCREMENTAL_V1.replace("select i as id", "select i as id, 'v2' as tag")


def test_new_incremental_is_cloned_once_per_build(tmp_path, capfd) -> None:
    proj = Project(tmp_path, {"inc": INCREMENTAL_V1})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    assert proj.cli("promote", "--release", "r1", "--no-audit") == 0
    proj.write_models({"inc": INCREMENTAL_V2})
    capfd.readouterr()

    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0
    out = capfd.readouterr().out
    assert out.count("Seeding") == 1, out
    # Before the fix the on-run-start hook dropped the CLI's clone and cloned again.
    assert "never registered" not in out
    assert proj.query("SELECT count(*) FROM analytics_staging.inc") == [(5,)]


def _diff(proj: Project, **args) -> dict:
    op = {"from_env": "staging", "to_env": "prod", "threshold": 10, "branching": 4, **args}
    res = proj.operation("virtual_data_diff", op)
    for line in res.stdout.splitlines():
        if MARKER in line:
            return json.loads(line.split(MARKER, 1)[1])["models"][0]
    raise AssertionError(res.stdout + res.stderr)


def _events(changed: str, extra_where: str = "") -> str:
    return (
        "{{ config(materialized='table', diff_key=['dt', 'id'], "
        "meta={'diff_partition_by': {'field': 'dt', 'data_type': 'date'}}) }}\n"
        "select date '2024-01-01' + cast(i % 2 as integer) as dt, i as id,\n"
        f"  case when i in ({changed}) then 'changed' else 'v' || i end as v\n"
        f"from range(1, 201) t(i) {extra_where}\n"
    )


@pytest.fixture
def events(tmp_path):
    proj = Project(tmp_path, {"events": _events("0")})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    return proj


def test_diff_with_a_composite_key_whose_first_entry_is_a_date(events) -> None:
    events.write_models({"events": _events("4, 8, 12, 150", "where i != 7")})
    assert events.cli("build", "--env", "staging", "--release", "r2") == 0
    model = _diff(events)
    # Before the fix: bisection subtracted dates (TypeError) / ranged on `dt`.
    assert (model["only_candidate"], model["only_target"], model["changed"]) == (0, 1, 4)
    assert model["key"] == ["dt", "id"]
    assert model["bisect_key"] == "id"  # the first numeric entry
    assert model["leaf_segments"] and all("id in [" in s["segment"] for s in model["leaf_segments"])


def test_diff_with_no_numeric_key_joins_whole_segments(events) -> None:
    sql = _events("4, 8", "").replace("diff_key=['dt', 'id']", "diff_key=['dt', 'v']")
    events.write_models({"events": sql})
    assert events.cli("build", "--env", "staging", "--release", "r2") == 0
    # `v` is part of the key here, so a changed `v` shows as one row only on each side.
    model = _diff(events)
    assert model["bisect_key"] is None and model["drilled"] is True
    assert (model["only_candidate"], model["only_target"], model["changed"]) == (2, 2, 0)
