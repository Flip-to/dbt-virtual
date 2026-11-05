"""Cross-project promote by clone, emulated with two physical schemas in one DuckDB file.

DuckDB (through dbt-duckdb) has no project boundary here, so the CI "project" and the prod
"project" are two physical schemas: `phys_ci` (environment `staging`, via the `default` entry of
`physical_locations`) and `phys_prod` (environment `prod`). What this proves is the bookkeeping the
BigQuery cross-project case relies on: the release is read from the CI registry, its tables are
cloned into the prod location, the prod view points at the clone, the clone is registered in the
prod registry, and the CI table can disappear afterwards. The permissions and the cross-project
`CLONE` itself are not exercised (see docs/decisions/20261105_cross_project_promote_by_clone.md).
"""

from __future__ import annotations

import pytest

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project, _table_model

CROSS_YML = PROJECT_YML.replace(
    "    base_environment: \"prod\"\n",
    "    base_environment: \"prod\"\n"
    "    physical_locations:\n"
    "      prod: {schema: phys_prod}\n"
    "      default: {schema: phys_ci}\n",
)
assert CROSS_YML != PROJECT_YML


@pytest.fixture
def project(tmp_path):
    return Project(tmp_path, {"orders": _table_model("select 1 as id, 10 as amount")}, project_yml=CROSS_YML)


def _tables(proj: Project, schema: str) -> set[str]:
    return {name for name, kind in proj.relations(schema) if kind == "BASE TABLE"}


def test_ci_builds_in_its_location_and_prod_promotes_by_clone(project: Project) -> None:
    fp = project.build("staging", "pr_1_abc")["orders"]
    table = f"orders__{fp}"

    # CI wrote only to its own location, registry included.
    assert table in _tables(project, "phys_ci")
    assert {"_environments", "_releases"} <= _tables(project, "phys_ci")
    assert "phys_prod" not in project.schemas() or table not in _tables(project, "phys_prod")
    assert project.query("SELECT physical_identifier FROM phys_ci._environments") == [(f"phys_ci.{table}",)]

    # Promote as the prod target: the release is read from the CI registry.
    project.operation(
        "virtual_promote",
        {"from_env": "staging", "to_env": "prod", "release_id": "pr_1_abc", "lock": True},
        env="prod",
    )

    # The prod table is a copy in the prod location, registered there; the view points at it.
    assert table in _tables(project, "phys_prod")
    assert project.query("SELECT physical_identifier FROM phys_prod._environments WHERE environment = 'prod'") == [
        (f"phys_prod.{table}",)
    ]
    assert project.query(
        "SELECT status, target_environment, source_environment FROM phys_prod._releases WHERE release_id = 'pr_1_abc'"
    ) == [("promoted", "prod", "staging")]
    assert project.query("SELECT id, amount FROM analytics.orders") == [(1, 10)]
    view_sql = project.query("SELECT view_definition FROM information_schema.views WHERE table_name = 'orders' AND table_schema = 'analytics'")
    assert "phys_prod" in view_sql[0][0] and "phys_ci" not in view_sql[0][0]

    # Prod no longer depends on CI: dropping the CI table (and its registry) breaks nothing.
    project.query("SELECT 1")  # read-only connection closed before the write below
    import duckdb

    con = duckdb.connect(project.db_path)
    con.execute(f"DROP TABLE phys_ci.{table}")
    con.execute("DROP SCHEMA phys_ci CASCADE")
    con.close()
    assert project.query("SELECT id, amount FROM analytics.orders") == [(1, 10)]


def test_promote_twice_is_idempotent_and_the_cli_takes_a_source_location(project: Project) -> None:
    project.build("staging", "pr_1_abc")
    args = ["promote", "--target", "prod", "--from", "staging", "--to", "prod", "--release", "pr_1_abc"]
    # A wrong source location fails the pre-flight instead of changing anything.
    assert project.cli(*args, "--skip-fingerprint-check", "--no-lock", "--source-location", "memory.no_such_schema") != 0
    assert project.cli(*args, "--skip-fingerprint-check", "--no-lock") == 0
    assert project.cli(*args, "--skip-fingerprint-check", "--no-lock") == 0
    assert project.query("SELECT count(*) FROM phys_prod._releases WHERE release_id = 'pr_1_abc'") == [(1,)]
    assert project.query("SELECT id FROM analytics.orders") == [(1,)]


INCREMENTAL_V1 = """{{ config(materialized='incremental', unique_key='id') }}
select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(id, v)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""
INCREMENTAL_V2 = """{{ config(materialized='incremental', unique_key='id') }}
select id, upper(v) as v from (values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) as t(id, v)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""


def test_ci_seeds_an_incremental_model_from_the_prod_location(tmp_path) -> None:
    proj = Project(tmp_path, {"events": INCREMENTAL_V1}, project_yml=CROSS_YML)
    fp_v1 = proj.build("prod", "r1")["events"]
    assert f"events__{fp_v1}" in _tables(proj, "phys_prod")

    proj.write_models({"events": INCREMENTAL_V2})
    fp_v2 = proj.build("staging", "pr_2_def")["events"]

    # The new version was cloned from prod's table (old values kept) and only the delta was built.
    assert f"events__{fp_v2}" in _tables(proj, "phys_ci")
    rows = proj.query("SELECT id, v FROM analytics_staging.events ORDER BY id")
    assert rows == [(1, "a"), (2, "b"), (3, "c"), (4, "D")]
    # Prod is untouched.
    assert proj.query("SELECT count(*) FROM phys_prod._environments") == [(1,)]
