"""`virtual_adopt` registers views and routines that are already deployed (real dbt-duckdb)."""

from __future__ import annotations

import duckdb
import pytest

from tests.test_dbt_duckdb_regressions import Project

VIEW_MODEL = "{{ config(materialized='view') }}\nselect 7 as k\n"
FUNCTION_MODEL = (
    "{{ config(materialized='virtual_function', parameters='x INTEGER', returns='INTEGER') }}\nx + 1\n"
)
TABLE_FUNCTION_MODEL = (
    "{{ config(materialized='table_function', parameters='n INTEGER', returns='') }}\nselect n as v\n"
)


@pytest.fixture
def make_project(tmp_path):
    def _make(models: dict[str, str]) -> Project:
        return Project(tmp_path, models)

    return _make


def test_adopt_registers_deployed_views_and_routines_and_init_env_replays_them(make_project) -> None:
    proj = make_project(
        {
            "v": VIEW_MODEL,
            "f": FUNCTION_MODEL,
            "tf": TABLE_FUNCTION_MODEL,
            # Never deployed: adopt must not register it.
            "ghost": VIEW_MODEL,
        }
    )
    proj.build("prod", "r0", "v", "f", "tf")
    assert proj.query("SELECT k FROM analytics.v") == [(7,)]
    assert proj.query("SELECT analytics.f(1)") == [(2,)]

    # An existing deployment that dbt-virtual does not know yet: drop the registry.
    con = duckdb.connect(proj.db_path)
    con.execute("DROP SCHEMA analytics_physical CASCADE")
    con.execute("CREATE SCHEMA analytics_physical")
    con.close()

    assert proj.cli("adopt", "--env", "prod", "--release", "adopt1") == 0
    # Before the change adopt only looked at table/incremental models: nothing was registered.
    pointers = dict(
        proj.query(
            "SELECT model_name, resource_type FROM analytics_physical._environments "
            "WHERE environment = 'prod'"
        )
    )
    assert pointers == {"v": "view", "f": "function", "tf": "table_function"}
    templates = dict(
        proj.query(
            "SELECT model_name, routine_ddl_template FROM analytics_physical._releases "
            "WHERE release_id = 'adopt1'"
        )
    )
    assert "CREATE OR REPLACE VIEW" in templates["v"]
    assert "CREATE OR REPLACE MACRO" in templates["f"]

    # The adopted templates are what later deployments replay.
    proj.operation("virtual_init_env", {"env": "pr_7", "from_env": "prod"})
    assert proj.query("SELECT k FROM analytics_pr_7.v") == [(7,)]
    assert proj.query("SELECT analytics_pr_7.f(1)") == [(2,)]
    assert proj.query("SELECT v FROM analytics_pr_7.tf(5)") == [(5,)]
