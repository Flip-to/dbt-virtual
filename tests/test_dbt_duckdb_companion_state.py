"""Companion state (e.g. the Snowplow incremental manifest) through real dbt-core + dbt-duckdb.

The fake package `fake_snowplow` stands in for snowplow_utils: a table `fake_manifest(model,
last_success)` per environment schema, an incremental model that reads only the input after its
own row, and an on-run-end hook of the package that advances the row to the newest input a
successful build consumed (like `snowplow_incremental_post_hook`).

Each test fails when the matching part of `macros/lifecycle/companion_state.sql` (or its calls in
hooks.sql / operations.sql) is removed: without it a changed model's new table is seeded, built,
promoted or rolled back next to a manifest row that describes a different table.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project

COMPANION_VARS = """    companion_on_full_build: {on_full_build}
    companion_state:
      - table: analytics.fake_manifest
        key_column: model
        tag: fake_incr
        env_tables:
          staging: analytics_staging.fake_manifest
          prod: analytics.fake_manifest
"""

FAKE_PACKAGE_YML = """name: 'fake_snowplow'
version: '1.0.0'
config-version: 2
macro-paths: ["macros"]
on-run-end:
  - "{{ fake_snowplow.advance_manifest() }}"
"""

FAKE_PACKAGE_MACRO = """{% macro advance_manifest(run_results=results) %}
  {%- if execute -%}
    {%- for res in run_results -%}
      {%- if res.status == 'success' and 'fake_incr' in res.node.tags -%}
        {%- do run_query("delete from " ~ target.schema ~ ".fake_manifest where model = '" ~ res.node.name ~ "'") -%}
        {%- do run_query("insert into " ~ target.schema ~ ".fake_manifest select '" ~ res.node.name ~ "', max(ts) from " ~ res.node.relation_name) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
"""

EVENTS_V1 = """{{ config(materialized='incremental', unique_key='id', tags=['fake_incr']) }}
select id, v, ts from raw.events
where ts > coalesce(
  (select max(last_success) from {{ target.schema }}.fake_manifest where model = '{{ model.name }}'),
  timestamp '1970-01-01')
"""
# v2 changes the transformation (new fingerprint); the window logic is the same.
EVENTS_V2 = EVENTS_V1.replace("select id, v, ts", "select id, upper(v) as v, ts")

ROWS = [(1, "a", "2024-01-01"), (2, "b", "2024-01-02"), (3, "c", "2024-01-03")]
LATE_ROW = (4, "d", "2024-01-04")


class CompanionProject(Project):
    def __init__(self, root: Path, *, on_full_build: str = "error") -> None:
        yml = PROJECT_YML.replace(
            '    base_environment: "prod"\n',
            '    base_environment: "prod"\n' + COMPANION_VARS.format(on_full_build=on_full_build),
        )
        super().__init__(root, {"events": EVENTS_V1}, project_yml=yml)
        pkg = root / "fake_snowplow"
        (pkg / "macros").mkdir(parents=True)
        (pkg / "dbt_project.yml").write_text(FAKE_PACKAGE_YML, encoding="utf-8")
        (pkg / "macros" / "advance.sql").write_text(FAKE_PACKAGE_MACRO, encoding="utf-8")
        packages = (self.dir / "packages.yml").read_text(encoding="utf-8")
        (self.dir / "packages.yml").write_text(
            packages + f'  - local: "{pkg.as_posix()}"\n', encoding="utf-8"
        )
        self.dbt("deps")
        con = duckdb.connect(self.db_path)
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.events (id INTEGER, v VARCHAR, ts TIMESTAMP)")
        for row in ROWS:
            con.execute("INSERT INTO raw.events VALUES (?, ?, ?)", row)
        for schema in ("analytics", "analytics_staging"):
            con.execute(f"CREATE TABLE {schema}.fake_manifest (model VARCHAR, last_success TIMESTAMP)")
        con.close()

    def add_late_row(self) -> None:
        con = duckdb.connect(self.db_path)
        con.execute("INSERT INTO raw.events VALUES (?, ?, ?)", LATE_ROW)
        con.close()

    def manifest(self, schema: str) -> list[tuple]:
        return self.query(f"SELECT model, CAST(last_success AS VARCHAR) FROM {schema}.fake_manifest")

    def dbt_args(self, env: str, release: str) -> list[str]:
        fps = self.fingerprints(env)
        dbt_vars = {"_dbt_virtual_fps": fps, "_dbt_virtual_env": env, "_dbt_virtual_release": release}
        return ["--target", env, "--vars", json.dumps(dbt_vars)]

    def physical_tables(self) -> set[str]:
        return {name for name, _ in self.relations("analytics_physical")}


@pytest.fixture
def make_companion(tmp_path: Path):
    def _make(**kwargs: str) -> CompanionProject:
        return CompanionProject(tmp_path, **kwargs)

    return _make


def test_seed_by_clone_copies_the_base_manifest_row(make_companion) -> None:
    proj = make_companion()
    proj.build("prod", "r1")
    assert proj.manifest("analytics") == [("events", "2024-01-03 00:00:00")]
    proj.add_late_row()
    proj.write_models({"events": EVENTS_V2})

    proj.build("staging", "r2")
    # The clone holds prod's rows 1-3 (lower case), and staging's manifest row says so: only the
    # late row runs through v2. Without the copied row staging's empty manifest replays everything
    # and the seeded history is rewritten.
    rows = proj.query("SELECT id, v FROM analytics_staging.events ORDER BY id")
    assert rows == [(1, "a"), (2, "b"), (3, "c"), (4, "D")]
    assert proj.manifest("analytics_staging") == [("events", "2024-01-04 00:00:00")]
    # Prod's own state is untouched by staging.
    assert proj.manifest("analytics") == [("events", "2024-01-03 00:00:00")]


def test_full_build_with_state_is_refused_then_reset_backfills(make_companion, tmp_path: Path) -> None:
    proj = make_companion()
    proj.build("staging", "r1")  # staging only: prod has no pointer to clone from
    assert proj.manifest("analytics_staging") == [("events", "2024-01-03 00:00:00")]
    proj.add_late_row()
    proj.write_models({"events": EVENTS_V2})
    args = proj.dbt_args("staging", "r2")

    # v2 has no base table, so dbt builds it in full: it would read only the rows after the
    # manifest row and drop 1-3 for good. Before the guard this built silently.
    res = proj.dbt("build", *args, expect_ok=False)
    assert res.returncode != 0
    assert "companion_on_full_build" in res.stdout
    assert len([t for t in proj.physical_tables() if t.startswith("events__")]) == 1

    # `--full-refresh` of a tracked model is the same hazard.
    res = proj.dbt("build", "--full-refresh", *args, expect_ok=False)
    assert res.returncode != 0
    assert "companion_on_full_build" in res.stdout

    # With `reset` the state is deleted first, so the package-style window starts from the beginning.
    yml = (proj.dir / "dbt_project.yml").read_text(encoding="utf-8")
    (proj.dir / "dbt_project.yml").write_text(
        yml.replace("companion_on_full_build: error", "companion_on_full_build: reset"), encoding="utf-8"
    )
    proj.dbt("build", *args)
    rows = proj.query("SELECT id, v FROM analytics_staging.events ORDER BY id")
    assert rows == [(1, "A"), (2, "B"), (3, "C"), (4, "D")]
    assert proj.manifest("analytics_staging") == [("events", "2024-01-04 00:00:00")]


def test_promote_and_rollback_move_the_manifest_row_with_the_table(make_companion) -> None:
    proj = make_companion()
    proj.build("staging", "r1")
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    # Promote wrote the promoted table's state into prod's manifest (it was empty before).
    assert proj.manifest("analytics") == [("events", "2024-01-03 00:00:00")]

    proj.add_late_row()
    proj.write_models({"events": EVENTS_V2})
    proj.build("staging", "r2")
    assert proj.manifest("analytics_staging") == [("events", "2024-01-04 00:00:00")]
    # Still describing the old table until r2 is promoted.
    assert proj.manifest("analytics") == [("events", "2024-01-03 00:00:00")]

    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r2"})
    assert proj.query("SELECT COUNT(*) FROM analytics.events") == [(4,)]
    assert proj.manifest("analytics") == [("events", "2024-01-04 00:00:00")]

    # Rolling back to r1 re-points prod at the table that lacks row 4, so the manifest must go
    # back too, or the next prod run skips row 4 forever.
    proj.operation("virtual_rollback", {"env": "prod"})
    assert proj.query("SELECT COUNT(*) FROM analytics.events") == [(3,)]
    assert proj.manifest("analytics") == [("events", "2024-01-03 00:00:00")]


def test_reused_table_gets_state_in_an_environment_that_never_ran_it(make_companion) -> None:
    proj = make_companion()
    proj.build("staging", "r1")
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    fp = proj.fingerprints("staging")["events"]
    # Staging forgets the model: no pointer, no manifest row (as for an environment that never ran it).
    con = duckdb.connect(proj.db_path)
    con.execute("DELETE FROM analytics_staging.fake_manifest")
    con.execute("DELETE FROM analytics_physical._environments WHERE environment = 'staging'")
    con.close()

    # `virtual_sync_reused_views` is what `dbt-virtual build` runs for a REUSE model: the table
    # exists, so nothing runs, but the environment must not look like it never consumed anything
    # (the package would backfill the whole run from its start date).
    proj.operation(
        "virtual_sync_reused_views",
        {"env": "staging", "release_id": "r2", "models_json": json.dumps({"events": fp})},
    )
    assert proj.manifest("analytics_staging") == [("events", "2024-01-03 00:00:00")]
