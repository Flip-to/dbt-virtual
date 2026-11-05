"""Promotion guards through real dbt-core + dbt-duckdb: an incomplete table never reaches prod.

Slim CI / staging builds use a short data window (a var such as `backfill_start`) or
`dbt build --empty`; a build can also fail half-way. None of these may be promoted.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project

WINDOW_YML = PROJECT_YML.replace(
    "    base_environment: \"prod\"\n",
    "    base_environment: \"prod\"\n    partial_build_vars: [\"backfill_start\"]\n",
)
assert WINDOW_YML != PROJECT_YML

# The window is rendered into the SQL, so a windowed build compiles different SQL.
EVENTS = """{{ config(materialized='table') }}
select * from (values (1, date '2026-11-01'), (2, date '2026-11-02'), (3, date '2026-11-03')) as t(id, day)
{% if var('backfill_start', none) %} where day >= date '{{ var("backfill_start") }}' {% endif %}
"""
# `daily` is a descendant: its fingerprint changes through the Merkle DAG.
DAILY = "{{ config(materialized='table') }}\nselect count(*) as n from {{ ref('events') }}\n"
MODELS = {"events": EVENTS, "daily": DAILY}


def _project(tmp_path: Path, **kwargs) -> Project:
    return Project(tmp_path, MODELS, project_yml=kwargs.pop("project_yml", WINDOW_YML), **kwargs)


def _with_window(proj: Project, start: str) -> None:
    text = (proj.dir / "dbt_project.yml").read_text(encoding="utf-8")
    (proj.dir / "dbt_project.yml").write_text(
        text.replace("vars:\n", f"vars:\n  backfill_start: '{start}'\n", 1), encoding="utf-8"
    )


def _prod_views(proj: Project) -> list[tuple[str, str]]:
    return proj.relations("analytics")


def test_full_build_is_sealed_and_promotes_with_the_cli_defaults(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    qualities = proj.query("SELECT DISTINCT build_quality FROM analytics_physical._releases WHERE release_id = 'r1'")
    assert qualities == [("full",)]
    assert proj.query("SELECT count(*) FROM analytics_physical._releases WHERE release_id = 'r1' AND sealed_at IS NULL") == [(0,)]

    # fingerprint check + audit + sealed check, all on (the CLI defaults for `--to prod`)
    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r1") == 0
    assert proj.query("SELECT count(*) FROM analytics.events") == [(3,)]
    assert _prod_views(proj) == [("daily", "VIEW"), ("events", "VIEW")]


def test_windowed_build_cannot_be_promoted(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    full = proj.fingerprints("prod")
    _with_window(proj, "2026-11-03")

    # (b) the window var is in `partial_build_vars`: the hook registers the tables as partial
    assert proj.cli("build", "--env", "staging", "--release", "r_window") == 0
    assert proj.query(
        "SELECT DISTINCT build_quality FROM analytics_physical._releases WHERE release_id = 'r_window'"
    ) == [("partial",)]
    assert proj.query("SELECT count(*) FROM analytics_staging.events") == [(1,)]

    # (a) the windowed SQL has another fingerprint, and so do its descendants (Merkle DAG)
    windowed = proj.fingerprints("staging")
    assert windowed["events"] != full["events"]
    assert windowed["daily"] != full["daily"]

    # The code of prod does not set the window (drop the var again): fingerprint check refuses.
    text = (proj.dir / "dbt_project.yml").read_text(encoding="utf-8")
    (proj.dir / "dbt_project.yml").write_text(text.replace("  backfill_start: '2026-11-03'\n", ""), encoding="utf-8")
    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r_window") != 0
    assert _prod_views(proj) == []

    # Only (b): without the fingerprint check the partial marker still stops it.
    res = proj.operation(
        "virtual_promote",
        {"from_env": "staging", "to_env": "prod", "release_id": "r_window"},
        expect_ok=False,
    )
    assert res.returncode != 0 and "built partially" in res.stdout
    assert _prod_views(proj) == []

    # Only (a): pretend the marker is gone (e.g. a registry row from before the marker existed).
    import duckdb

    con = duckdb.connect(proj.db_path)
    con.execute("UPDATE analytics_physical._releases SET build_quality = NULL")
    con.execute("UPDATE analytics_physical._environments SET build_quality = NULL")
    con.close()
    res = proj.operation(
        "virtual_promote",
        {
            "from_env": "staging",
            "to_env": "prod",
            "release_id": "r_window",
            "allow_unsealed": True,
            "expected_fps_json": json.dumps(full),
        },
        expect_ok=False,
    )
    assert res.returncode != 0 and "fingerprint differs from the prod code" in res.stdout
    assert _prod_views(proj) == []


def test_partial_table_is_not_reused_by_the_planner(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    _with_window(proj, "2026-11-03")
    assert proj.cli("build", "--env", "staging", "--release", "r_window") == 0
    state = proj.dir / "state.json"
    assert proj.cli("status", "--env", "staging", "--output", str(state)) == 0
    exported = json.loads(state.read_text(encoding="utf-8"))
    windowed = proj.fingerprints("staging")
    assert set(exported["partial_physical_tables"]) == {f"events__{windowed['events']}", f"daily__{windowed['daily']}"}

    # Same code again, now with the snapshot: the existing (partial) tables are rebuilt, not reused.
    assert proj.cli("plan", "--env", "staging", "--state-file", str(state), "--format", "json") == 0
    plan = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text(encoding="utf-8"))
    assert plan["by_name"] == windowed


def test_empty_build_is_marked_partial(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    fps = proj.fingerprints("staging")
    dbt_vars = {"_dbt_virtual_fps": fps, "_dbt_virtual_env": "staging", "_dbt_virtual_release": "r_empty"}
    proj.dbt("build", "--empty", "--target", "staging", "--vars", json.dumps(dbt_vars))
    assert proj.query("SELECT count(*) FROM analytics_staging.events") == [(0,)]
    assert proj.query(
        "SELECT DISTINCT build_quality FROM analytics_physical._releases WHERE release_id = 'r_empty'"
    ) == [("partial",)]
    res = proj.operation(
        "virtual_promote",
        {"from_env": "staging", "to_env": "prod", "release_id": "r_empty", "allow_unsealed": True},
        expect_ok=False,
    )
    assert res.returncode != 0 and "built partially" in res.stdout


def test_failed_build_leaves_the_release_unsealed_and_unpromotable(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    # `daily` fails after `events` was built and registered in the release.
    proj.write_models({"daily": "{{ config(materialized='table') }}\nselect no_such_column from {{ ref('events') }}\n"})
    assert proj.cli("build", "--env", "staging", "--release", "r_failed") != 0

    assert proj.query("SELECT model_name FROM analytics_physical._releases WHERE release_id = 'r_failed'") == [("events",)]
    assert proj.query("SELECT count(*) FROM analytics_physical._releases WHERE sealed_at IS NOT NULL") == [(0,)]
    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r_failed") != 0
    res = proj.operation(
        "virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r_failed"}, expect_ok=False
    )
    assert res.returncode != 0 and "not sealed" in res.stdout
    assert _prod_views(proj) == []

    # The operator may still override after checking the tables themselves.
    proj.operation(
        "virtual_promote",
        {"from_env": "staging", "to_env": "prod", "release_id": "r_failed", "allow_unsealed": True},
    )
    assert _prod_views(proj) == [("events", "VIEW")]


def test_release_from_before_sealing_needs_an_explicit_seal(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    assert proj.cli("build", "--env", "staging", "--release", "r_old") == 0
    # Make it look like a release of an older version: no seal, no build_quality.
    import duckdb

    con = duckdb.connect(proj.db_path)
    con.execute("UPDATE analytics_physical._releases SET sealed_at = NULL, build_quality = NULL")
    con.close()

    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r_old") != 0
    assert _prod_views(proj) == []
    proj.operation("virtual_seal_release", {"release_id": "r_old"})
    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r_old") == 0
    assert _prod_views(proj) == [("daily", "VIEW"), ("events", "VIEW")]


def test_registry_without_guard_columns_is_migrated(tmp_path: Path) -> None:
    proj = _project(tmp_path)
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    import duckdb

    con = duckdb.connect(proj.db_path)
    for table in ("_environments", "_releases"):
        con.execute(f"ALTER TABLE analytics_physical.{table} DROP COLUMN build_quality")
    con.execute("ALTER TABLE analytics_physical._releases DROP COLUMN sealed_at")
    con.close()

    assert proj.cli("build", "--env", "staging", "--release", "r2", "--full-refresh") == 0
    columns = {r[0] for r in proj.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = '_releases'"
    )}
    assert {"build_quality", "sealed_at"} <= columns
    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r2") == 0
