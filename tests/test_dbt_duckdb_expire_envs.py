"""Real dbt-core + dbt-duckdb: `gc --expire-env-days` drops abandoned unmapped environments."""

from __future__ import annotations

import json

import duckdb
from test_dbt_duckdb_regressions import Project, _table_model, make_project  # noqa: F401


def _build_in_env(proj: Project, env: str, release: str) -> dict[str, str]:
    """Build an unmapped env (`pr_*`) the way `dbt-virtual build --env` does, on the staging target."""
    fps = proj.fingerprints("staging")
    dbt_vars = {"_dbt_virtual_fps": fps, "_dbt_virtual_env": env, "_dbt_virtual_release": release}
    proj.dbt("build", "--target", "staging", "--vars", json.dumps(dbt_vars))
    return fps


def _age(proj: Project, env: str, days: int) -> None:
    con = duckdb.connect(proj.db_path)
    con.execute(
        "UPDATE analytics_physical._environments "
        f"SET updated_at = CURRENT_TIMESTAMP - INTERVAL '{days} days' WHERE environment = '{env}'"
    )
    con.close()


def test_gc_expire_env_days_drops_idle_pr_env_and_frees_its_tables(make_project) -> None:  # noqa: F811
    proj = make_project({"a": _table_model("select 1 as a")})
    fp_v1 = proj.build("staging", "r1")["a"]
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})

    # A PR env that changes the model: its table `a__<v2>` is referenced only by `pr_9`.
    proj.write_models({"a": _table_model("select 2 as a")})
    fp_v2 = _build_in_env(proj, "pr_9", "rp9")["a"]
    assert fp_v2 != fp_v1
    assert proj.query("SELECT a FROM analytics_pr_9.a") == [(2,)]
    assert proj.query("SELECT environment FROM analytics_physical._environments ORDER BY 1") == [
        ("pr_9",),
        ("prod",),
        ("staging",),
    ]

    # Everything is old, including the mapped environments: those must survive.
    for env in ("pr_9", "prod", "staging"):
        _age(proj, env, 30)

    # Without the flag, a never-dropped env pins its table forever.
    assert proj.cli("gc", "--retention-days", "0", "--retention-releases", "1") == 0
    assert f"a__{fp_v2}" in {n for n, _ in proj.relations("analytics_physical")}

    # Dry run only reports.
    assert proj.cli("gc", "--dry-run", "--expire-env-days", "14", "--retention-days", "0") == 0
    assert "analytics_pr_9" in proj.schemas()
    assert proj.query("SELECT COUNT(*) FROM analytics_physical._environments WHERE environment = 'pr_9'") == [(1,)]

    assert proj.cli("gc", "--expire-env-days", "14", "--retention-days", "0", "--retention-releases", "1") == 0
    assert "analytics_pr_9" not in proj.schemas()
    assert proj.query("SELECT environment FROM analytics_physical._environments ORDER BY 1") == [
        ("prod",),
        ("staging",),
    ]
    tables = {n for n, _ in proj.relations("analytics_physical")}
    assert f"a__{fp_v2}" not in tables  # became a GC candidate and was dropped
    assert f"a__{fp_v1}" in tables  # still used by prod and staging
    assert proj.relations("analytics") == [("a", "VIEW")]
    assert proj.relations("analytics_staging") == [("a", "VIEW")]
    assert proj.query("SELECT a FROM analytics.a") == [(1,)]


def test_gc_expire_env_days_keeps_recent_pr_env(make_project) -> None:  # noqa: F811
    proj = make_project({"a": _table_model("select 1 as a")})
    proj.build("staging", "r1")
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    proj.operation("virtual_init_env", {"env": "pr_7", "from_env": "prod"})
    _age(proj, "pr_7", 3)
    assert proj.cli("gc", "--expire-env-days", "14") == 0
    assert proj.relations("analytics_pr_7") == [("a", "VIEW")]
