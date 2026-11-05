"""Virtual `view` models and `dbt-virtual run`, through real dbt-core + dbt-duckdb.

- A `virtual_env: true` + `materialized='view'` model used to exist only in the schema of the
  environment dbt built it in: promote, rollback and init-env never created it elsewhere.
- Nothing refreshed an unchanged model (REUSE) on a schedule: `dbt-virtual run` does.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from tests.test_dbt_duckdb_multi_schema import MULTI_SCHEMA_YML
from tests.test_dbt_duckdb_regressions import Project, _table_model

BASE = _table_model("select * from (values (1, 10), (2, 20)) as t(id, amount)")


def _view(sql: str, schema: str | None = None) -> str:
    extra = f", schema='{schema}'" if schema else ""
    return "{{ config(materialized='view'" + extra + ") }}\n" + sql + "\n"


def _promote(proj: Project, release: str, to_env: str = "prod") -> None:
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": to_env, "release_id": release})


def _view_sql(proj: Project, schema: str, name: str) -> str:
    return proj.query(
        f"SELECT sql FROM duckdb_views() WHERE schema_name = '{schema}' AND view_name = '{name}'"
    )[0][0]


def test_virtual_views_are_promoted_rolled_back_and_demoted(tmp_path: Path) -> None:
    proj = Project(
        tmp_path,
        {
            "base": BASE,
            "v": _view("select id, amount * 2 as dbl from {{ ref('base') }}"),
            # Alphabetically before the view it reads: needs DAG-depth ordering.
            "a_top": _view("select sum(dbl) as total from {{ ref('z_mid') }}"),
            "z_mid": _view("select id, dbl from {{ ref('v') }}"),
        },
    )
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    assert proj.query("SELECT total FROM analytics_staging.a_top") == [(60,)]

    _promote(proj, "r1")
    # Before the fix: prod had only `base`.
    assert proj.relations("analytics") == [
        ("a_top", "VIEW"), ("base", "VIEW"), ("v", "VIEW"), ("z_mid", "VIEW"),
    ]
    assert proj.query("SELECT total FROM analytics.a_top") == [(60,)]
    definition = _view_sql(proj, "analytics", "v")
    assert "analytics_staging" not in definition and "analytics.base" in definition.replace('"', "")
    registered = proj.query(
        "SELECT model_name, resource_type FROM analytics_physical._environments "
        "WHERE environment = 'prod' ORDER BY 1"
    )
    assert registered == [("a_top", "view"), ("base", "model"), ("v", "view"), ("z_mid", "view")]

    # An ephemeral environment gets every view, in dependency order.
    proj.operation("virtual_init_env", {"env": "pr_1", "from_env": "prod"})
    assert [n for n, _ in proj.relations("analytics_pr_1")] == ["a_top", "base", "v", "z_mid"]
    assert proj.query("SELECT total FROM analytics_pr_1.a_top") == [(60,)]

    # Changing a view's SQL builds a new release; promote updates prod's view.
    proj.write_models({"v": _view("select id, amount * 3 as dbl from {{ ref('base') }}")})
    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0
    _promote(proj, "r2")
    assert proj.query("SELECT total FROM analytics.a_top") == [(90,)]

    proj.operation("virtual_rollback", {"env": "prod"})
    assert proj.query("SELECT total FROM analytics.a_top") == [(60,)]
    assert proj.query("SELECT dbl FROM analytics.v ORDER BY id") == [(20,), (40,)]

    # A view removed from the project is demoted on the next promote.
    (proj.dir / "models" / "a_top.sql").unlink()
    assert proj.cli("build", "--env", "staging", "--release", "r3") == 0
    _promote(proj, "r3")
    assert [n for n, _ in proj.relations("analytics")] == ["base", "v", "z_mid"]
    gone = proj.query(
        "SELECT model_name FROM analytics_physical._environments "
        "WHERE environment = 'prod' AND model_name = 'a_top'"
    )
    assert gone == []


def test_virtual_view_in_custom_schema_follows_the_environment_mapping(tmp_path: Path) -> None:
    proj = Project(
        tmp_path,
        {
            "daily": _table_model("select 7 as d"),
            "gold_view": _view("select d + 1 as d1 from {{ ref('daily') }}", schema="gold"),
        },
        project_yml=MULTI_SCHEMA_YML,
    )
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    assert proj.relations("analytics_staging") == [("daily", "VIEW"), ("gold_view", "VIEW")]

    _promote(proj, "r1")
    assert proj.relations("analytics") == [("daily", "VIEW")]
    assert proj.relations("analytics_gold") == [("gold_view", "VIEW")]
    assert proj.query("SELECT d1 FROM analytics_gold.gold_view") == [(8,)]
    keys = dict(
        proj.query(
            "SELECT model_name, schema_key FROM analytics_physical._environments "
            "WHERE environment = 'prod'"
        )
    )
    assert keys == {"daily": None, "gold_view": "gold"}


def test_unchanged_registered_view_is_reused_not_rebuilt(tmp_path: Path) -> None:
    proj = Project(tmp_path, {"base": BASE, "v": _view("select id from {{ ref('base') }}")})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    state = proj.dir / "state.json"
    assert proj.cli("status", "--env", "staging", "--output", str(state)) == 0
    assert proj.cli("plan", "--env", "staging", "--format", "json", "--state-file", str(state)) == 0
    plan = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))
    assert set(plan["by_name"]) == {"base", "v"}

    # A second release with the same state must still describe the whole environment.
    assert proj.cli("build", "--env", "staging", "--release", "r2", "--state-file", str(state)) == 0
    released = proj.query(
        "SELECT model_name, resource_type FROM analytics_physical._releases "
        "WHERE release_id = 'r2' ORDER BY 1"
    )
    assert released == [("base", "model"), ("v", "view")]
    template = proj.query(
        "SELECT routine_ddl_template FROM analytics_physical._releases "
        "WHERE release_id = 'r2' AND model_name = 'v'"
    )[0][0]
    assert template and "CREATE OR REPLACE VIEW" in template.upper()


INCREMENTAL = """{{ config(materialized='incremental', unique_key='id') }}
select __COL__ from raw.events
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""


def _events_model(col: str = "id") -> str:
    return INCREMENTAL.replace("__COL__", col)


def _physical_tables(proj: Project) -> list[str]:
    return [n for n, _ in proj.relations("analytics_physical") if not n.startswith("_")]


def test_run_refreshes_incremental_model_in_place_and_refuses_changed_sql(tmp_path: Path) -> None:
    proj = Project(tmp_path, {"events": _events_model()})
    con = duckdb.connect(proj.db_path)
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE TABLE raw.events AS SELECT * FROM (VALUES (1), (2)) AS t(id)")
    con.close()

    assert proj.cli("build", "--env", "prod", "--release", "r1") == 0
    physical = _physical_tables(proj)
    assert len(physical) == 1 and physical[0].startswith("events__")
    assert proj.query("SELECT count(*) FROM analytics.events") == [(2,)]

    con = duckdb.connect(proj.db_path)
    con.execute("INSERT INTO raw.events VALUES (3), (4)")
    con.close()

    # `build` plans REUSE (same fingerprint) and never refreshes the data; `run` does.
    assert proj.cli("run", "--env", "prod", "--start", "2026-01-01", "--end", "2026-01-02") == 0
    assert _physical_tables(proj) == physical
    assert proj.query("SELECT count(*) FROM analytics.events") == [(4,)]
    assert proj.query(f"SELECT count(*) FROM analytics_physical.{physical[0]}") == [(4,)]
    intervals = proj.query("SELECT model_name, start_ts, end_ts FROM analytics_physical._intervals")
    assert ("events", "2026-01-01", "2026-01-02") in intervals

    # Changed SQL is a deploy: `run` refuses and leaves the table alone.
    proj.write_models({"events": _events_model(col="id * 1 as id")})
    con = duckdb.connect(proj.db_path)
    con.execute("INSERT INTO raw.events VALUES (5)")
    con.close()
    assert proj.cli("run", "--env", "prod") == 2
    assert _physical_tables(proj) == physical
    assert proj.query("SELECT count(*) FROM analytics.events") == [(4,)]

    # `--allow-changes` deploys the new fingerprint: seeded from prod's table, plus the new row.
    assert proj.cli("run", "--env", "prod", "--allow-changes") == 0
    assert len(_physical_tables(proj)) == 2
    assert proj.query("SELECT count(*) FROM analytics.events") == [(5,)]
