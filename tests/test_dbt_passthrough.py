"""`dbt-virtual dbt`, `DBT_VIRTUAL_FPS` and the shared-table write guard, through real dbt-core + dbt-duckdb.

Gap: dbt generates aliases while parsing (`execute` is False), so Jinja cannot put parent
fingerprints in a physical name; only fingerprints computed in Python and handed to dbt can
(docs/tech_debt/bare_dbt_run_parse_time_fingerprint.md).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_dbt_duckdb_regressions import Project

UP_V1 = "{{ config(materialized='table') }}\nselect 1 as x\n"
UP_V2 = "{{ config(materialized='table') }}\nselect 2 as x\n"
DOWN = "{{ config(materialized='table') }}\nselect x from {{ ref('up') }}\n"
MODELS = {"up": UP_V1, "down": DOWN}


def _physical(proj: Project) -> list[str]:
    return [n for n, _ in proj.relations("analytics_physical") if not n.startswith("_")]


def _passthrough(proj: Project, *args: str) -> int:
    """`dbt-virtual dbt <args>`, with the dbt of this environment and the project's profiles."""
    return proj.cli("dbt", *args, "--profiles-dir", str(proj.dir))


def _env_run(proj: Project, fps_json: str | None, *args: str) -> subprocess.CompletedProcess:
    env = {k: v for k, v in os.environ.items() if k != "DBT_VIRTUAL_FPS"}
    if fps_json is not None:
        env["DBT_VIRTUAL_FPS"] = fps_json
    cmd = [sys.executable, "-m", "dbt.cli.main", *args, "--profiles-dir", str(proj.dir)]
    return subprocess.run(cmd, cwd=proj.dir, capture_output=True, text=True, env=env, check=False)


def test_passthrough_builds_the_same_physical_names_as_dbt_virtual_build(tmp_path: Path) -> None:
    ref = Project(tmp_path / "a", MODELS)
    fps = ref.fingerprints("staging")
    assert ref.cli("build", "--env", "staging", "--skip-compile", "--release", "r1") == 0
    expected = _physical(ref)
    assert expected == sorted([f"down__{fps['down']}", f"up__{fps['up']}"])

    proj = Project(tmp_path / "b", MODELS)
    rc = _passthrough(proj, "build", "--target", "staging")

    assert rc == 0
    assert _physical(proj) == expected
    assert proj.query("SELECT x FROM analytics_staging.down") == [(1,)]


def test_passthrough_merges_user_vars_env_and_preserves_exit_code(tmp_path: Path) -> None:
    models = {
        "up": "{{ config(materialized='table') }}\nselect {{ var('n', 1) }} as x\n",
        "bad": "{{ config(materialized='view') }}\nselect nope from {{ ref('up') }}\n",
    }
    proj = Project(tmp_path, models)

    assert _passthrough(proj, "run", "--target", "staging", "--select", "up", "--vars", "{n: 7}") == 0
    assert proj.query("SELECT x FROM analytics_staging.up") == [(7,)]

    # A failing model: dbt's non-zero exit code comes back unchanged.
    assert _passthrough(proj, "run", "--target", "staging", "--select", "bad") != 0

    # `--env` lands in `_dbt_virtual_env`: the env view goes to the `prod` schema.
    assert _passthrough_env(proj, "prod") == 0
    assert proj.query("SELECT x FROM analytics.up") == [(1,)]


def _passthrough_env(proj: Project, env: str) -> int:
    return proj.cli("dbt", "--env", env, "run", "--target", "prod", "--select", "up", "--profiles-dir", str(proj.dir))


@pytest.mark.parametrize("command", ["compile", "ls", "test", "seed", "snapshot", "run-operation"])
def test_passthrough_accepts_other_dbt_commands(tmp_path: Path, command: str) -> None:
    proj = Project(tmp_path, MODELS)
    args = [command, "--target", "staging"]
    if command == "run-operation":
        args = [command, "virtual_status_probe_does_not_exist", "--target", "staging"]
        # Unknown macro: dbt fails, and the passthrough returns dbt's code rather than 0.
        assert _passthrough(proj, *args) != 0
        return
    assert _passthrough(proj, *args) == 0


def test_passthrough_reuses_cached_fingerprints_until_the_project_changes(
    tmp_path: Path, capfd: pytest.CaptureFixture[str]
) -> None:
    proj = Project(tmp_path, MODELS)

    assert _passthrough(proj, "ls", "--target", "staging") == 0
    first = capfd.readouterr().err
    assert _passthrough(proj, "ls", "--target", "staging") == 0
    second = capfd.readouterr().err
    proj.write_models({"up": UP_V2})
    assert _passthrough(proj, "ls", "--target", "staging") == 0
    third = capfd.readouterr().err

    assert "compiled and fingerprinted" in first
    assert "reusing cached fingerprints" in second
    assert "compiled and fingerprinted" in third


def test_print_fps_is_compact_json_on_stdout(tmp_path: Path, capfd: pytest.CaptureFixture[str]) -> None:
    proj = Project(tmp_path, MODELS)

    assert proj.cli("plan", "--env", "staging", "--print-fps") == 0

    out = capfd.readouterr().out
    fps = json.loads(out)  # nothing but the JSON may be on stdout
    assert sorted(fps) == ["down", "up"]
    assert " " not in out.strip()


def test_env_var_fingerprints_make_plain_dbt_run_use_merkle_names(tmp_path: Path, capfd) -> None:
    proj = Project(tmp_path, MODELS)
    fps = proj.fingerprints("staging")

    # Before: no fingerprints anywhere. `down` is named from its own SQL only, so it does not carry
    # the Merkle fingerprint the CLI computes (and would not change when `up` changes).
    bare = Project(tmp_path / "bare", MODELS)
    assert _env_run(bare, None, "run", "--target", "staging").returncode == 0
    bare_names = _physical(bare)
    assert f"down__{fps['down']}" not in bare_names, bare_names

    # After: the same plain `dbt run` with DBT_VIRTUAL_FPS=$(dbt-virtual plan --print-fps).
    assert proj.cli("plan", "--env", "staging", "--print-fps") == 0
    printed = capfd.readouterr().out.strip().splitlines()[-1]
    res = _env_run(proj, printed, "run", "--target", "staging")
    assert res.returncode == 0, res.stdout + res.stderr
    assert _physical(proj) == sorted([f"down__{fps['down']}", f"up__{fps['up']}"])


def test_env_var_fingerprints_satisfy_require_cli_fingerprints(tmp_path: Path) -> None:
    from tests.test_require_cli_fingerprints import GUARDED_YML

    proj = Project(tmp_path, MODELS, project_yml=GUARDED_YML)
    fps = proj.fingerprints("staging")

    refused = _env_run(proj, None, "run", "--target", "staging")
    assert refused.returncode != 0 and "require_cli_fingerprints is true" in refused.stdout

    ok = _env_run(proj, json.dumps(fps), "run", "--target", "staging")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert f"down__{fps['down']}" in _physical(proj)


def test_invalid_env_var_fingerprints_fail_clearly(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS)

    res = _env_run(proj, "[1, 2]", "run", "--target", "staging")

    assert res.returncode != 0
    assert "DBT_VIRTUAL_FPS must be a JSON object" in res.stdout


# --- guard_shared_tables ------------------------------------------------------------------------


def _prod_then_changed_up(proj: Project) -> None:
    """prod serves `up` and `down` (built by the CLI), then `up` changes in the working tree."""
    proj.build("prod", "r1")
    proj.write_models({"up": UP_V2})


def test_guard_refuses_bare_run_that_would_rewrite_a_table_prod_points_at(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS)
    # Bare run in prod registers `down__<own hash>` with its run-time Merkle fingerprint.
    assert proj.dbt("run", "--target", "prod", expect_ok=False).returncode == 0
    proj.write_models({"up": UP_V2})

    res = proj.dbt("run", "--target", "staging", expect_ok=False)

    assert res.returncode != 0
    assert "refusing to rewrite physical table" in res.stdout, res.stdout
    assert "environment prod" in res.stdout
    # Prod still reads what it read before; nothing was rebuilt in place.
    assert proj.query("SELECT x FROM analytics.down") == [(1,)]


def test_guard_allows_a_bare_run_when_parents_did_not_change(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS)
    assert proj.dbt("run", "--target", "prod", expect_ok=False).returncode == 0

    proj.dbt("run", "--target", "staging")

    assert proj.query("SELECT x FROM analytics_staging.down") == [(1,)]


def test_guard_allows_a_table_only_the_dev_environment_uses(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS)
    assert proj.dbt("run", "--target", "staging", expect_ok=False).returncode == 0
    proj.write_models({"up": UP_V2})

    # Only staging points at these tables; rebuilding them in place changes nobody else's data.
    proj.dbt("run", "--target", "staging")

    assert proj.query("SELECT x FROM analytics_staging.up") == [(2,)]


def test_guard_can_be_turned_off(tmp_path: Path) -> None:
    from tests.test_dbt_duckdb_regressions import PROJECT_YML

    yml = PROJECT_YML.replace(
        '    base_environment: "prod"\n', '    base_environment: "prod"\n    guard_shared_tables: false\n'
    )
    proj = Project(tmp_path, MODELS, project_yml=yml)
    assert proj.dbt("run", "--target", "prod", expect_ok=False).returncode == 0
    proj.write_models({"up": UP_V2})

    proj.dbt("run", "--target", "staging")


def test_guard_does_not_block_passthrough_builds_after_a_parent_change(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS)
    _prod_then_changed_up(proj)

    # With Merkle fingerprints in the names, `down` gets a NEW table; prod's stays untouched.
    assert _passthrough(proj, "build", "--target", "staging") == 0

    assert proj.query("SELECT x FROM analytics.down") == [(1,)]
    assert proj.query("SELECT x FROM analytics_staging.down") == [(2,)]
