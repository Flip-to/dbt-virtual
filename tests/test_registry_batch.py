"""Batched registry writes (`flush_registry`) and the two-invocation `dbt-virtual build`.

Harness tests render the macros against a mocked dbt runtime; `test_dbt_duckdb_batched_registry.py`
runs the same behavior through real dbt-core + dbt-duckdb with several threads.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from test_cli_and_macros import _sample_manifest
from test_macros_execution import MacroTestHarness

from dbt_virtual.cli import main

CFG = {
    "physical_schema": "analytics_physical",
    "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
}
HOOK_NODE = {
    "operation.pkg.pkg-on-run-end-0": {
        "resource_type": "operation",
        "raw_code": "{{ dbt_virtual.flush_registry(results) }}",
    }
}


def _model(name: str, fp: str, mat: str = "table", status: str = "success", **cfg):
    return SimpleNamespace(
        status=status,
        node={
            "name": name,
            "alias": f"{name}__{fp}" if mat in ("table", "incremental") else name,
            "schema": "analytics_physical" if mat in ("table", "incremental") else "analytics_staging",
            "database": "test_proj",
            "resource_type": "model",
            "config": {"materialized": mat, "virtual_env": True, **cfg},
            "compiled_code": "SELECT 1",
        },
    )


def _harness(adapter_type: str = "bigquery", **extra_vars) -> MacroTestHarness:
    harness = MacroTestHarness(
        adapter_type=adapter_type,
        vars_dict={
            "dbt_virtual": dict(CFG, **extra_vars.pop("cfg", {})),
            "_dbt_virtual_release": "r1",
            "_dbt_virtual_start": "2026-10-01",
            "_dbt_virtual_end": "2026-10-27",
            **extra_vars,
        },
    )
    harness.graph = {"nodes": dict(HOOK_NODE)}
    harness.flags = SimpleNamespace(FULL_REFRESH=False, WHICH="build")
    return harness


def _results() -> list[SimpleNamespace]:
    results = [_model(f"m{i}", f"fp{i:010d}") for i in range(5)]
    results.append(_model("broken", "fpbroken00000", status="error"))
    results.append(
        _model("tvf", "x", mat="table_function", parameters="n INT64", returns="TABLE<a INT64>")
    )
    return results


def test_flush_registry_writes_all_successful_nodes_in_one_merge() -> None:
    harness = _harness()
    harness.call_macro("flush_registry", _results())

    queries = harness.executed_queries
    merges = [q for q in queries if q.startswith("MERGE")]
    assert len(merges) == 1
    # Six successful objects (5 tables + 1 routine) in one MERGE; the failed model is not in it.
    assert merges[0].count("SELECT\n") == 6
    assert merges[0].count("UNION ALL") == 5
    assert "'broken'" not in "\n".join(queries)

    inserts = [q for q in queries if q.startswith("INSERT INTO")]
    releases = [q for q in inserts if "_releases" in q]
    intervals = [q for q in inserts if "_intervals" in q]
    assert len(releases) == 1 and len(intervals) == 1
    assert releases[0].count("'candidate'") == 6
    assert "__DBT_VIRTUAL_ENV_SCHEMA__" in releases[0]  # the routine DDL template
    assert "CREATE OR REPLACE TABLE FUNCTION" in releases[0]
    # Intervals are recorded for tables only.
    assert intervals[0].count("'2026-10-01'") == 5
    assert len(queries) == 5  # MERGE, build_quality UPDATE, DELETE (idempotency), INSERT releases, INSERT intervals


def test_flush_registry_default_adapter_uses_one_delete_and_one_insert() -> None:
    harness = _harness(adapter_type="duckdb")
    harness.call_macro("flush_registry", _results())

    env_stmts = [q for q in harness.executed_queries if "_environments" in q]
    assert len(env_stmts) == 3  # DELETE + INSERT for all nodes, plus one build_quality UPDATE
    assert env_stmts[0].startswith("DELETE FROM")
    assert env_stmts[0].count("'m") == 5 and "'tvf'" in env_stmts[0]
    assert env_stmts[1].count("CURRENT_TIMESTAMP") == 6


def test_flush_registry_is_a_noop_for_per_model_mode_and_compile() -> None:
    per_model = _harness(cfg={"registry_write_mode": "per_model"})
    per_model.call_macro("flush_registry", _results())
    assert per_model.executed_queries == []

    compile_run = _harness()
    compile_run.flags = SimpleNamespace(FULL_REFRESH=False, WHICH="compile")
    compile_run.call_macro("flush_registry", _results())
    assert compile_run.executed_queries == []


def _sync_sql(harness: MacroTestHarness) -> str:
    from test_macros_execution import FakeRelation

    harness.model = {
        "name": "orders_metrics",
        "resource_type": "model",
        "config": {"materialized": "table", "virtual_env": True},
    }
    harness.this = FakeRelation("test_proj", "analytics_physical", "orders_metrics__fp1111222233")
    return harness.call_macro("sync_env_view")


def test_sync_env_view_only_creates_the_view_in_batch_mode() -> None:
    sql = _sync_sql(_harness())
    assert "CREATE OR REPLACE VIEW" in sql
    for forbidden in ("MERGE", "INSERT INTO", "DELETE FROM"):
        assert forbidden not in sql


def test_sync_env_view_per_model_mode_is_unchanged() -> None:
    sql = _sync_sql(_harness(cfg={"registry_write_mode": "per_model"}))
    assert "MERGE `test_proj`.`analytics_physical`._environments" in sql
    assert "INSERT INTO `test_proj`.`analytics_physical`._releases" in sql
    assert "INSERT INTO `test_proj`.`analytics_physical`._intervals" in sql


def test_batch_mode_without_the_on_run_end_hook_warns_and_writes_per_model() -> None:
    harness = _harness()
    harness.graph = {"nodes": {}}
    sql = _sync_sql(harness)
    assert "MERGE `test_proj`.`analytics_physical`._environments" in sql
    assert any("flush_registry" in line and "WARNING" in line for line in harness.logs)


def test_routine_registration_is_deferred_to_flush_in_batch_mode() -> None:
    from test_macros_execution import FakeRelation

    node = {
        "name": "tvf",
        "config": {"materialized": "table_function", "virtual_env": True},
    }
    rel = FakeRelation("test_proj", "analytics_staging", "tvf", "external")
    batch = _harness().call_macro("register_routine_deployment", node, rel, "table_function", "SELECT 1")
    assert not batch
    explicit = _harness().call_macro(
        "register_routine_deployment", node, rel, "table_function", "SELECT 1", ddl="CREATE FUNCTION x"
    )
    assert "MERGE" in explicit  # an explicit DDL cannot be recomputed in on-run-end
    per_model = _harness(cfg={"registry_write_mode": "per_model"}).call_macro(
        "register_routine_deployment", node, rel, "table_function", "SELECT 1"
    )
    assert "MERGE" in per_model


def _build_cmds(tmp_path: Path, argv: list[str], state: dict | None) -> list[list[str]]:
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "manifest.json").write_text(json.dumps(_sample_manifest()), encoding="utf-8")
    args = ["--project-dir", str(tmp_path), "build", "--env", "staging", *argv]
    if state is not None:
        state_file = tmp_path / "state.json"
        state_file.write_text(json.dumps(state), encoding="utf-8")
        args += ["--state-file", str(state_file)]
    recorded: list[list[str]] = []

    def fake_run_dbt(cmd: list[str], project_dir: Path) -> int:
        recorded.append(cmd)
        return 0

    with patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt):
        assert main(args) == 0
    return recorded


def _vars(cmd: list[str]) -> dict:
    return json.loads(cmd[cmd.index("--vars") + 1])


def test_cli_build_is_compile_plus_one_build_with_hook_payloads(tmp_path: Path) -> None:
    from dbt_virtual.fingerprint import compute_manifest_fingerprints

    fps = compute_manifest_fingerprints(_sample_manifest())
    reuse_fp = fps["model.pkg.orders_metrics"].fingerprint
    cmds = _build_cmds(tmp_path, ["--release", "r1"], {"env_state": {"orders_metrics": reuse_fp}})
    # Before the change: compile, run-operation (reuse), build, run-operation (snapshot).
    assert [c[0] for c in cmds] == ["compile", "build"]
    build_vars = _vars(cmds[1])
    assert build_vars["_dbt_virtual_reuse"] == {"orders_metrics": reuse_fp}
    assert "_dbt_virtual_seed" not in build_vars
    assert build_vars["_dbt_virtual_snapshot"] is True
    assert build_vars["_dbt_virtual_release"] == "r1"

    seed_cmds = _build_cmds(
        tmp_path / "seed", ["--release", "r1"], {"base_env_state": {"orders_metrics": "old000111222"}}
    )
    # Before the change: compile, run-operation (seed), build, run-operation (snapshot).
    assert [c[0] for c in seed_cmds] == ["compile", "build"]
    assert _vars(seed_cmds[1])["_dbt_virtual_seed"] == ["orders_metrics"]


def test_cli_build_without_release_or_reuse_passes_no_hook_payloads(tmp_path: Path) -> None:
    cmds = _build_cmds(tmp_path, [], None)
    assert [c[0] for c in cmds] == ["compile", "build"]
    build_vars = _vars(cmds[1])
    assert not {"_dbt_virtual_reuse", "_dbt_virtual_seed", "_dbt_virtual_snapshot"} & set(build_vars)


def test_cli_build_with_nothing_to_build_uses_one_run_operation(tmp_path: Path) -> None:
    from dbt_virtual.fingerprint import compute_manifest_fingerprints

    fps = compute_manifest_fingerprints(_sample_manifest())
    state = {"env_state": {fp.name: fp.fingerprint for fp in fps.values()}}
    cmds = _build_cmds(tmp_path, ["--release", "r1"], state)
    assert [c[:2] for c in cmds] == [["compile", "--target"], ["run-operation", "virtual_finalize_plan"]]
    op_args = json.loads(cmds[1][cmds[1].index("--args") + 1])
    assert op_args["release_id"] == "r1" and op_args["snapshot"] is True
    assert set(op_args["reuse"]) == {"orders_metrics"}  # routines are never rebuilt or re-pointed
