"""Unit tests for `dbt-virtual run` and the planner's handling of virtual views (no warehouse)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from dbt_virtual.cli import main
from dbt_virtual.fingerprint import compute_manifest_fingerprints
from dbt_virtual.planner import classify_plan_actions, compute_dbt_select_for_build


def _node(name: str, materialized: str, sql: str, parents: list[str] | None = None) -> dict:
    return {
        "unique_id": f"model.pkg.{name}",
        "name": name,
        "alias": name,
        "resource_type": "model",
        "schema": "analytics_staging",
        "compiled_code": sql,
        "raw_code": sql,
        "config": {"materialized": materialized, "virtual_env": True},
        "depends_on": {"nodes": [f"model.pkg.{p}" for p in (parents or [])]},
    }


def _manifest(events_sql: str = "SELECT 1 AS id") -> dict:
    nodes = [
        _node("events", "incremental", events_sql),
        _node("daily", "table", "SELECT 2 AS d"),
        _node("v", "view", "SELECT id FROM analytics_staging.events", ["events"]),
        _node("f", "function", "1"),
    ]
    return {"nodes": {n["unique_id"]: n for n in nodes}}


def _project(tmp_path: Path, manifest: dict) -> Path:
    (tmp_path / "target").mkdir(parents=True)
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return tmp_path


def _state(tmp_path: Path, fps: dict, **overrides: str | None) -> Path:
    env_state = {fp.name: fp.fingerprint for fp in fps.values()}
    for name, value in overrides.items():
        if value is None:
            env_state.pop(name)
        else:
            env_state[name] = value
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"env_state": env_state}), encoding="utf-8")
    return path


def _run(tmp_path: Path, *extra: str) -> tuple[int, list[list[str]]]:
    recorded: list[list[str]] = []

    def fake_run_dbt(args: list[str], project_dir: Path) -> int:
        recorded.append(args)
        return 0

    with patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt):
        rc = main(["--project-dir", str(tmp_path), "run", "--skip-compile", *extra])
    return rc, recorded


def test_run_builds_only_virtual_table_and_incremental_models_with_the_build_vars(tmp_path: Path) -> None:
    manifest = _manifest()
    fps = compute_manifest_fingerprints(manifest)
    _project(tmp_path, manifest)
    state = _state(tmp_path, fps)

    rc, recorded = _run(
        tmp_path, "--env", "prod", "--state-file", str(state), "--start", "2026-01-01", "--end", "2026-01-02"
    )

    assert rc == 0
    assert len(recorded) == 1
    args = recorded[0]
    assert args[:3] == ["build", "--target", "prod"]
    # Views and routines are not refreshed by `run`.
    assert args[args.index("--select") + 1 :] == ["daily", "events"]
    run_vars = json.loads(args[args.index("--vars") + 1])
    assert run_vars["_dbt_virtual_env"] == "prod"
    assert run_vars["_dbt_virtual_start"] == "2026-01-01"
    assert run_vars["_dbt_virtual_end"] == "2026-01-02"
    assert "_dbt_virtual_release" not in run_vars
    assert run_vars["_dbt_virtual_fps"]["events"] == fps["model.pkg.events"].fingerprint
    assert "--full-refresh" not in args


def test_run_refuses_a_changed_fingerprint_unless_allowed(tmp_path: Path, capsys) -> None:
    manifest = _manifest(events_sql="SELECT 1 AS id, 2 AS extra")
    fps = compute_manifest_fingerprints(manifest)
    _project(tmp_path, manifest)
    # The environment still has the previous version of `events` and no `daily` at all.
    state = _state(tmp_path, fps, events="deadbeef0000", daily=None)

    rc, recorded = _run(tmp_path, "--env", "prod", "--state-file", str(state))
    assert rc == 2
    assert recorded == []
    err = capsys.readouterr().err
    assert "events: registered=deadbeef0000" in err and "daily: registered=none" in err

    rc, recorded = _run(tmp_path, "--env", "prod", "--state-file", str(state), "--allow-changes")
    assert rc == 0
    # The changed incremental is seeded from the base environment before dbt builds it.
    assert recorded[0][:2] == ["run-operation", "virtual_seed_incremental_clones"]
    assert json.loads(recorded[0][recorded[0].index("--args") + 1]) == {"model_names": ["events"]}
    assert recorded[1][0] == "build"


def test_run_without_state_file_asks_the_warehouse_and_honours_select(tmp_path: Path) -> None:
    manifest = _manifest()
    fps = compute_manifest_fingerprints(manifest)
    _project(tmp_path, manifest)
    exported = {"env_state": {fp.name: fp.fingerprint for fp in fps.values()}}

    with (
        patch("dbt_virtual.cli._capture_state", return_value=(0, exported)) as capture,
        patch("dbt_virtual.cli._selected_model_names", return_value={"events", "v"}) as selected,
    ):
        rc, recorded = _run(tmp_path, "--env", "prod", "--select", "tag:hourly", "--full-refresh")

    assert rc == 0
    assert capture.call_args.kwargs == {"target": "prod"}
    assert capture.call_args.args[1] == {"env": "prod", "base_env": "prod"}
    assert selected.call_args.args[2] == "tag:hourly"
    assert recorded[0][recorded[0].index("--select") + 1 :][:1] == ["events"]
    assert "--full-refresh" in recorded[0]
    assert len(recorded) == 1


def test_run_with_nothing_to_run_does_not_call_dbt(tmp_path: Path, capsys) -> None:
    manifest = {"nodes": {"model.pkg.v": _node("v", "view", "SELECT 1")}}
    _project(tmp_path, manifest)
    rc, recorded = _run(tmp_path, "--state-file", str(_state(tmp_path, {})))
    assert rc == 0 and recorded == []
    assert "no virtual table or incremental models" in capsys.readouterr().out


def test_planner_reuses_an_unchanged_view_and_deploys_a_changed_one() -> None:
    fps = compute_manifest_fingerprints(_manifest())
    view_fp = fps["model.pkg.v"]
    assert view_fp.physical_identifier == "v"

    unchanged = classify_plan_actions(fps, env_state={"v": view_fp.fingerprint}, existing_physical_tables=set())
    assert unchanged["model.pkg.v"].action == "REUSE"
    models, reused = compute_dbt_select_for_build(unchanged)
    assert "v" not in models and "v" not in reused

    changed = classify_plan_actions(fps, env_state={"v": "oldoldold000"}, existing_physical_tables=set())
    assert changed["model.pkg.v"].action == "VIEW_DEPLOY"
    assert "v" in compute_dbt_select_for_build(changed)[0]

    new = classify_plan_actions(fps, env_state={}, existing_physical_tables=set())
    assert new["model.pkg.v"].action == "VIEW_DEPLOY"
