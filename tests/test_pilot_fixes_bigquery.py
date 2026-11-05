"""Pilot findings on the BigQuery adapter: CLI logic with a mocked dbt, and the BigQuery macro harness.

The BigQuery emulator cannot run dbt, so the CLI half is exercised with a BigQuery-shaped manifest
(see `scripts/run_emulator_e2e.py` for the macro half against the emulator). The same bugs are
reproduced through real dbt in `tests/test_dbt_duckdb_pilot_fixes.py`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dbt_virtual import cli
from dbt_virtual.cli import main
from dbt_virtual.fingerprint import compute_manifest_fingerprints
from tests.test_cli_and_macros import _sample_manifest
from tests.test_macros_execution import MacroTestHarness


def _project(tmp_path: Path) -> dict[str, str]:
    """A project dir with the sample manifest; returns `{model name: fingerprint}`."""
    (tmp_path / "target").mkdir()
    manifest = _sample_manifest()
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return {fp.name: fp.fingerprint for fp in compute_manifest_fingerprints(manifest).values()}


def _build(tmp_path: Path, selected: set[str], *args: str) -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_run_dbt(cmd: list[str], project_dir: Path) -> int:
        calls.append(cmd)
        return 0

    with (
        patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt),
        patch("dbt_virtual.cli._selected_model_names", return_value=selected) as ls,
    ):
        assert main(["--project-dir", str(tmp_path), "build", "--env", "staging", "--skip-compile", *args]) == 0
    assert ls.call_count == 1
    return calls


def test_build_select_never_builds_reuse_models(tmp_path: Path) -> None:
    fps = _project(tmp_path)
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"env_state": {"orders_metrics": fps["orders_metrics"]}}), encoding="utf-8")
    # `tag:x` matches everything, the REUSE `orders_metrics` included.
    everything = {"orders_metrics", "udf_dimension_key", "tvf_orders_report"}
    calls = _build(tmp_path, everything, "--state-file", str(state), "--select", "tag:x")

    build = next(c for c in calls if c[0] == "build")
    selected = build[build.index("--select") + 1].split()
    # Before the fix the raw selector `tag:x` was passed and dbt rebuilt `orders_metrics`.
    assert "tag:x" not in selected and "orders_metrics" not in selected
    assert set(selected) == {"udf_dimension_key", "tvf_orders_report"}
    variables = json.loads(build[build.index("--vars") + 1])
    assert list(variables["_dbt_virtual_reuse"]) == ["orders_metrics"]


def test_build_select_narrows_to_the_selection(tmp_path: Path) -> None:
    _project(tmp_path)
    calls = _build(tmp_path, {"udf_dimension_key"}, "--select", "udf_dimension_key")
    build = next(c for c in calls if c[0] == "build")
    assert build[build.index("--select") + 1].split() == ["udf_dimension_key"]


def test_build_select_matching_only_reuse_models_runs_no_dbt_build(tmp_path: Path) -> None:
    fps = _project(tmp_path)
    state = tmp_path / "state.json"
    state.write_text(json.dumps({"env_state": {"orders_metrics": fps["orders_metrics"]}}), encoding="utf-8")
    calls = _build(tmp_path, set(), "--state-file", str(state), "--select", "orders_metrics")
    assert [c[0] for c in calls] == ["run-operation"] and calls[0][1] == "virtual_finalize_plan"


# -- base_environment rendered like the macros render it ----------------------------------------

JINJA = "{{ 'main' if target.name == 'bq_dev' else 'prod' }}"


def test_print_base_environment_macro_renders_jinja_for_bigquery() -> None:
    harness = MacroTestHarness(
        adapter_type="bigquery", target_name="bq_dev", vars_dict={"dbt_virtual": {"base_environment": JINJA}}
    )
    harness.call_macro("virtual_print_base_environment")
    assert "DBT_VIRTUAL_BASE_ENV:main" in harness.logs
    other = MacroTestHarness(
        adapter_type="bigquery", target_name="prod", vars_dict={"dbt_virtual": {"base_environment": JINJA}}
    )
    other.call_macro("virtual_print_base_environment")
    assert "DBT_VIRTUAL_BASE_ENV:prod" in other.logs


def _write_project_yml(tmp_path: Path, value: str) -> None:
    (tmp_path / "dbt_project.yml").write_text(
        f"name: p\nvars:\n  dbt_virtual:\n    base_environment: {json.dumps(value)}\n", encoding="utf-8"
    )


def _fake_dbt_output(rendered: str, returncode: int = 0):
    def fake_run(cmd, **kwargs):
        assert "virtual_print_base_environment" in cmd
        return SimpleNamespace(returncode=returncode, stdout=f"12:00 DBT_VIRTUAL_BASE_ENV:{rendered}\n", stderr="")

    return fake_run


def test_cli_renders_a_jinja_base_environment(tmp_path: Path) -> None:
    _write_project_yml(tmp_path, JINJA)
    with patch("dbt_virtual.cli.subprocess.run", side_effect=_fake_dbt_output("main")):
        assert cli._base_environment(tmp_path) == "main"
    # Before the fix a Jinja value was read as `prod`.
    with patch("dbt_virtual.cli.subprocess.run", side_effect=_fake_dbt_output("main", returncode=1)):
        assert cli._base_environment(tmp_path) is None


def test_cli_plain_base_environment_needs_no_dbt_call(tmp_path: Path) -> None:
    _write_project_yml(tmp_path, "main")
    with patch("dbt_virtual.cli.subprocess.run", side_effect=AssertionError("no dbt call expected")):
        assert cli._base_environment(tmp_path) == "main"


def test_promote_audits_by_default_on_the_rendered_base_environment(tmp_path: Path) -> None:
    _project(tmp_path)
    _write_project_yml(tmp_path, JINJA)
    calls: list[list[str]] = []

    def fake_run_dbt(cmd: list[str], project_dir: Path) -> int:
        calls.append(cmd)
        return 0

    def audit_flag(to_env: str) -> bool:
        calls.clear()
        with (
            patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt),
            patch("dbt_virtual.cli.subprocess.run", side_effect=_fake_dbt_output("main")),
        ):
            assert main(["--project-dir", str(tmp_path), "promote", "--release", "r1", "--skip-compile", "--to", to_env]) == 0
        op = next(c for c in calls if c[:2] == ["run-operation", "virtual_promote"])
        return json.loads(op[op.index("--args") + 1])["audit"]

    assert audit_flag("main") is True
    assert audit_flag("prod") is False


# -- fingerprints do not depend on --empty --------------------------------------------------------


def test_compile_runs_without_dbt_empty_and_other_commands_keep_it(monkeypatch) -> None:
    monkeypatch.setenv("DBT_EMPTY", "true")
    envs: list[dict | None] = []

    def fake_run(cmd, **kwargs):
        envs.append(kwargs.get("env"))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with patch("dbt_virtual.cli.subprocess.run", side_effect=fake_run):
        cli._run_dbt(["compile", "--target", "bq_dev"], Path("."))
        cli._compile_quietly(["compile"], Path("."))
        cli._run_dbt(["build", "--target", "bq_dev"], Path("."))
    # Before the fix every call inherited DBT_EMPTY, so `source()` compiled to `... where false limit 0`.
    assert "DBT_EMPTY" not in envs[0] and "DBT_EMPTY" not in envs[1]
    assert envs[2] is None  # inherits the environment: a build still honours --empty
    assert os.environ["DBT_EMPTY"] == "true"


def test_compile_env_is_unchanged_without_dbt_empty(monkeypatch) -> None:
    monkeypatch.delenv("DBT_EMPTY", raising=False)
    assert cli._dbt_env(["compile"]) is None
