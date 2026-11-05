"""`require_cli_fingerprints`: refuse bare `dbt run` / `dbt build` of virtual tables.

Without the CLI var `_dbt_virtual_fps`, the physical name `<model>__<fp>` of a virtual table is
generated at parse time and omits upstream fingerprints, so a bare run after an upstream change
rebuilds a downstream physical table in place, possibly one prod's view points at
(docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md). With the flag on, the on-run-start hook
fails such a run before anything is built. Run through real dbt-core + dbt-duckdb.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project

GUARDED_YML = PROJECT_YML.replace(
    '    base_environment: "prod"\n',
    '    base_environment: "prod"\n    require_cli_fingerprints: true\n',
)
assert GUARDED_YML != PROJECT_YML

# Projects configured before the guard existed call `seed_incremental_clones()` from
# on-run-start; new projects use `on_run_start()`. Both must enforce the flag.
ON_RUN_START_HOOK = GUARDED_YML.replace(
    '"{{ dbt_virtual.seed_incremental_clones() }}"', '"{{ dbt_virtual.on_run_start() }}"'
)
assert ON_RUN_START_HOOK != GUARDED_YML

GUARD_MESSAGE = "require_cli_fingerprints is true"

MODELS = {
    "up": "{{ config(materialized='table') }}\nselect 1 as x\n",
    "down": "{{ config(materialized='table') }}\nselect x from {{ ref('up') }}\n",
    # A virtual view has no fingerprinted physical table, so the guard ignores it.
    "v": "{{ config(materialized='view') }}\nselect 2 as y\n",
}


def _physical_tables(proj: Project) -> list[str]:
    return [name for name, _ in proj.relations("analytics_physical") if not name.startswith("_")]


@pytest.mark.parametrize(
    "project_yml", [GUARDED_YML, ON_RUN_START_HOOK], ids=["seed_incremental_clones", "on_run_start"]
)
def test_bare_dbt_run_fails_before_building(tmp_path: Path, project_yml: str) -> None:
    proj = Project(tmp_path, MODELS, project_yml=project_yml)

    res = proj.dbt("run", "--target", "staging", expect_ok=False)

    assert res.returncode != 0
    assert GUARD_MESSAGE in res.stdout, res.stdout
    assert "dbt-virtual build" in res.stdout
    assert _physical_tables(proj) == []
    assert proj.relations("analytics_staging") == []


def test_bare_dbt_build_fails_too(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=GUARDED_YML)

    res = proj.dbt("build", "--target", "staging", expect_ok=False)

    assert res.returncode != 0
    assert GUARD_MESSAGE in res.stdout, res.stdout
    assert _physical_tables(proj) == []


def test_commands_that_do_not_build_are_not_guarded(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=ON_RUN_START_HOOK)

    # `dbt-virtual plan` runs `dbt compile` without vars; it must keep working.
    for args in (("compile",), ("parse",), ("ls",), ("docs", "generate")):
        proj.dbt(*args, "--target", "staging")
    assert _physical_tables(proj) == []


def test_bare_run_of_only_non_table_virtual_models_is_allowed(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=GUARDED_YML)

    proj.dbt("run", "--target", "staging", "--select", "v")

    assert proj.query("SELECT y FROM analytics_staging.v") == [(2,)]
    assert _physical_tables(proj) == []


def test_build_with_cli_fingerprints_succeeds(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=GUARDED_YML)

    # `Project.build` passes `_dbt_virtual_fps` the way `dbt-virtual build` does.
    fps = proj.build("staging", "r1")

    assert sorted(_physical_tables(proj)) == [f"down__{fps['down']}", f"up__{fps['up']}"]
    assert proj.query("SELECT x FROM analytics_staging.down") == [(1,)]


def test_flag_off_keeps_bare_dbt_run_working(tmp_path: Path) -> None:
    # `PROJECT_YML` does not set the flag; the default is false.
    proj = Project(tmp_path, MODELS)

    proj.dbt("run", "--target", "staging")

    assert proj.query("SELECT x FROM analytics_staging.down") == [(1,)]
