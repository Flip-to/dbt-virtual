from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

import jinja2

from dbt_virtual.cli import main
from dbt_virtual.fingerprint import compute_manifest_fingerprints
from dbt_virtual.planner import (
    classify_plan_actions,
    compute_dbt_select_for_build,
    diff_manifests,
    diff_model_columns,
)


def _sample_manifest() -> dict:
    return {
        "nodes": {
            "model.pkg.events_base": {
                "unique_id": "model.pkg.events_base",
                "name": "events_base",
                "alias": "events_base",
                "resource_type": "model",
                "schema": "core_prod",
                "compiled_code": "SELECT customer_id, dt FROM raw.events",
                "raw_code": "SELECT customer_id, dt FROM {{ source('raw', 'events') }}",
                "config": {
                    "materialized": "incremental",
                    "virtual_env": False,
                },
                "depends_on": {"nodes": []},
            },
            "model.pkg.orders_metrics": {
                "unique_id": "model.pkg.orders_metrics",
                "name": "orders_metrics",
                "alias": "orders_metrics",
                "resource_type": "model",
                "schema": "analytics_staging",
                "compiled_code": (
                    "SELECT customer_id, COUNT(*) AS orders "
                    "FROM `proj.core_prod.events_base` GROUP BY 1"
                ),
                "raw_code": (
                    "SELECT customer_id, COUNT(*) AS orders "
                    "FROM {{ ref('events_base') }} GROUP BY 1"
                ),
                "config": {
                    "materialized": "incremental",
                    "incremental_strategy": "insert_overwrite",
                    "partition_by": {"field": "dt", "data_type": "date"},
                    "cluster_by": ["customer_id"],
                    "virtual_env": True,
                },
                "columns": {
                    "customer_id": {"name": "customer_id", "data_type": "STRING"},
                    "orders": {"name": "orders", "data_type": "INT64"},
                },
                "depends_on": {"nodes": ["model.pkg.events_base"]},
            },
            "model.pkg.udf_dimension_key": {
                "unique_id": "model.pkg.udf_dimension_key",
                "name": "udf_dimension_key",
                "alias": "udf_dimension_key",
                "resource_type": "model",
                "schema": "analytics_staging",
                "compiled_code": "CASE dim WHEN 'store' THEN ['s1'] ELSE [] END",
                "raw_code": "CASE dim WHEN 'store' THEN ['s1'] ELSE [] END",
                "config": {
                    "materialized": "function",
                    "parameters": "dim STRING",
                    "returns": "ARRAY<STRING>",
                    "virtual_env": True,
                },
                "depends_on": {"nodes": []},
            },
            "model.pkg.tvf_orders_report": {
                "unique_id": "model.pkg.tvf_orders_report",
                "name": "tvf_orders_report",
                "alias": "tvf_orders_report",
                "resource_type": "model",
                "schema": "analytics_staging",
                "compiled_code": (
                    "SELECT customer_id, orders "
                    "FROM `proj.analytics_staging.orders_metrics`"
                ),
                "raw_code": (
                    "SELECT customer_id, orders "
                    "FROM {{ tvf_test_ref('orders_metrics') }}"
                ),
                "config": {
                    "materialized": "table_function",
                    "parameters": "start_dt DATE, end_dt DATE",
                    "virtual_env": True,
                },
                "depends_on": {
                    "nodes": [
                        "model.pkg.orders_metrics",
                        "model.pkg.udf_dimension_key",
                    ]
                },
            },
        }
    }


def test_cli_plan_json_and_text(tmp_path: Path, capsys) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True)
    (target_dir / "manifest.json").write_text(
        json.dumps(_sample_manifest()), encoding="utf-8"
    )

    rc = main(
        [
            "--project-dir",
            str(tmp_path),
            "plan",
            "--env",
            "staging",
            "--skip-compile",
            "--known-schema",
            "analytics_staging",
            "--format",
            "json",
        ]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["environment"] == "staging"
    assert out["total_virtual_nodes"] == 3
    assert out["action_counts"] == {"FULL_BUILD": 1, "ROUTINE_DEPLOY": 2}
    fp_file = tmp_path / ".dbt_virtual" / "fingerprints.json"
    assert fp_file.exists()
    fp_data = json.loads(fp_file.read_text(encoding="utf-8"))
    assert set(fp_data["by_name"].keys()) == {
        "orders_metrics",
        "udf_dimension_key",
        "tvf_orders_report",
    }

    # Text format output
    rc_txt = main(
        [
            "--project-dir",
            str(tmp_path),
            "plan",
            "--env",
            "staging",
            "--skip-compile",
            "--format",
            "text",
        ]
    )
    assert rc_txt == 0
    txt_out = capsys.readouterr().out
    assert "orders_metrics" in txt_out
    assert "tvf_orders_report" in txt_out
    assert "FULL_BUILD" in txt_out


def test_planner_reuse_and_seed_incremental_classification(tmp_path: Path) -> None:
    manifest = _sample_manifest()
    fps = compute_manifest_fingerprints(manifest, known_schemas=["analytics_staging"])
    orders_fp = fps["model.pkg.orders_metrics"].fingerprint
    udf_fp = fps["model.pkg.udf_dimension_key"].fingerprint
    tvf_fp = fps["model.pkg.tvf_orders_report"].fingerprint

    # Case 1: All fingerprints already active in target env -> 100% REUSE
    all_reuse = classify_plan_actions(
        fps,
        env_state={
            "orders_metrics": orders_fp,
            "udf_dimension_key": udf_fp,
            "tvf_orders_report": tvf_fp,
        },
    )
    assert all(a.action == "REUSE" for a in all_reuse.values())
    to_build, reused = compute_dbt_select_for_build(all_reuse)
    assert to_build == []
    assert reused == {"orders_metrics": orders_fp}

    # Case 2: Base environment (prod) has an older fingerprint for orders_metrics -> SEED_INCREMENTAL
    seed_inc = classify_plan_actions(
        fps,
        env_state={},
        base_env_state={"orders_metrics": "old000111222"},
    )
    assert seed_inc["model.pkg.orders_metrics"].action == "SEED_INCREMENTAL"
    assert seed_inc["model.pkg.orders_metrics"].base_fingerprint == "old000111222"


def test_cli_build_skips_dbt_when_all_models_reused(tmp_path: Path, capsys) -> None:
    manifest = _sample_manifest()
    fps = compute_manifest_fingerprints(manifest)
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True)
    (target_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    state_file = tmp_path / "state.json"
    state_file.write_text(
        json.dumps(
            {
                "env_state": {
                    "orders_metrics": fps["model.pkg.orders_metrics"].fingerprint,
                    "udf_dimension_key": fps["model.pkg.udf_dimension_key"].fingerprint,
                    "tvf_orders_report": fps["model.pkg.tvf_orders_report"].fingerprint,
                }
            }
        ),
        encoding="utf-8",
    )

    recorded_cmds: list[list[str]] = []

    def fake_run_dbt(args: list[str], project_dir: Path) -> int:
        recorded_cmds.append(args)
        return 0

    with patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt):
        rc = main(
            [
                "--project-dir",
                str(tmp_path),
                "build",
                "--env",
                "staging",
                "--skip-compile",
                "--state-file",
                str(state_file),
            ]
        )
    assert rc == 0
    # Only `run-operation virtual_sync_reused_views` is called; `dbt build` is skipped!
    assert len(recorded_cmds) == 1
    assert recorded_cmds[0][:2] == ["run-operation", "virtual_sync_reused_views"]
    assert "zero compute" in capsys.readouterr().out


def test_cli_build_promote_rollback_gc_init_drop_subcommands(tmp_path: Path) -> None:
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True)
    (target_dir / "manifest.json").write_text(
        json.dumps(_sample_manifest()), encoding="utf-8"
    )

    recorded_cmds: list[list[str]] = []

    def fake_run_dbt(args: list[str], project_dir: Path) -> int:
        recorded_cmds.append(args)
        return 0

    with patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt):
        assert (
            main(
                [
                    "--project-dir",
                    str(tmp_path),
                    "build",
                    "--env",
                    "staging",
                    "--release",
                    "v2026.10.27",
                    "--select",
                    "+orders_metrics",
                    "--start",
                    "2026-10-01",
                    "--end",
                    "2026-10-27",
                    "--full-refresh",
                    "--skip-compile",
                ]
            )
            == 0
        )
        assert (
            main(
                [
                    "--project-dir",
                    str(tmp_path),
                    "promote",
                    "--from",
                    "staging",
                    "--to",
                    "prod",
                    "--release",
                    "v2026.10.27",
                ]
            )
            == 0
        )
        assert (
            main(
                [
                    "--project-dir",
                    str(tmp_path),
                    "rollback",
                    "--env",
                    "prod",
                    "--to-release",
                    "v2026.10.26",
                ]
            )
            == 0
        )
        assert (
            main(
                [
                    "--project-dir",
                    str(tmp_path),
                    "init-env",
                    "--env",
                    "pr_123",
                    "--from",
                    "prod",
                ]
            )
            == 0
        )
        assert (
            main(
                [
                    "--project-dir",
                    str(tmp_path),
                    "drop-env",
                    "--env",
                    "pr_123",
                ]
            )
            == 0
        )
        assert (
            main(
                [
                    "--project-dir",
                    str(tmp_path),
                    "gc",
                    "--dry-run",
                    "--retention-releases",
                    "3",
                    "--retention-days",
                    "14",
                ]
            )
            == 0
        )

    assert len(recorded_cmds) == 6
    build_cmd = recorded_cmds[0]
    assert build_cmd[:3] == ["build", "--target", "staging"]
    assert "--full-refresh" in build_cmd
    vars_payload = json.loads(build_cmd[build_cmd.index("--vars") + 1])
    assert vars_payload["_dbt_virtual_env"] == "staging"
    assert vars_payload["_dbt_virtual_release"] == "v2026.10.27"
    assert vars_payload["_dbt_virtual_start"] == "2026-10-01"
    assert vars_payload["_dbt_virtual_end"] == "2026-10-27"
    assert "orders_metrics" in vars_payload["_dbt_virtual_fps"]

    assert recorded_cmds[1][:2] == ["run-operation", "virtual_promote"]
    assert recorded_cmds[2][:2] == ["run-operation", "virtual_rollback"]
    assert recorded_cmds[3][:2] == ["run-operation", "virtual_init_env"]
    assert recorded_cmds[4][:2] == ["run-operation", "virtual_drop_env"]
    assert recorded_cmds[5][:2] == ["run-operation", "virtual_gc"]


def test_schema_diff_and_cli_diff_subcommand(tmp_path: Path, capsys) -> None:
    d_non_breaking = diff_model_columns(
        "orders_metrics",
        {"customer_id": "STRING", "orders": "INT64"},
        {"customer_id": "STRING", "orders": "INT64", "revenue": "NUMERIC"},
    )
    assert d_non_breaking.classification == "NON_BREAKING"

    d_breaking = diff_model_columns(
        "orders_metrics",
        {"customer_id": "STRING", "orders": "INT64"},
        {"customer_id": "STRING", "orders": "FLOAT64"},
    )
    assert d_breaking.classification == "BREAKING"

    base_manifest = _sample_manifest()
    target_manifest = _sample_manifest()
    # Drop `orders` column in target manifest -> BREAKING
    target_manifest["nodes"]["model.pkg.orders_metrics"]["columns"] = {
        "customer_id": {"name": "customer_id", "data_type": "STRING"}
    }

    diffs = diff_manifests(base_manifest, target_manifest)
    assert len(diffs) == 1
    assert diffs[0].classification == "BREAKING"

    base_path = tmp_path / "base_manifest.json"
    target_dir = tmp_path / "target"
    target_dir.mkdir(parents=True)
    target_path = target_dir / "manifest.json"
    base_path.write_text(json.dumps(base_manifest), encoding="utf-8")
    target_path.write_text(json.dumps(target_manifest), encoding="utf-8")

    rc = main(
        [
            "--project-dir",
            str(tmp_path),
            "diff",
            "--base-manifest",
            str(base_path),
            "--fail-on-breaking",
            "--format",
            "json",
        ]
    )
    assert rc == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["has_breaking_changes"] is True
    assert payload["diffs"][0]["classification"] == "BREAKING"


def test_cycle_detection_in_manifest() -> None:
    manifest = {
        "nodes": {
            "model.pkg.a": {
                "unique_id": "model.pkg.a",
                "name": "a",
                "resource_type": "model",
                "config": {"virtual_env": True},
                "depends_on": {"nodes": ["model.pkg.b"]},
            },
            "model.pkg.b": {
                "unique_id": "model.pkg.b",
                "name": "b",
                "resource_type": "model",
                "config": {"virtual_env": True},
                "depends_on": {"nodes": ["model.pkg.a"]},
            },
        }
    }
    try:
        compute_manifest_fingerprints(manifest)
        raise AssertionError("Expected ValueError on cycle")
    except ValueError as exc:
        assert "Cycle detected" in str(exc)


def test_jinja_sql_macros_parse_cleanly() -> None:
    """Verify all SQL macro files have balanced Jinja blocks and valid template structure."""
    repo = Path(__file__).resolve().parent.parent
    macros_dir = repo / "macros"
    sql_files = list(macros_dir.rglob("*.sql"))
    assert len(sql_files) >= 8

    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    for sql_file in sql_files:
        text = sql_file.read_text(encoding="utf-8")
        # Materializations use dbt's custom {% materialization ... %} tag; replace with a block to verify Jinja balance
        sanitized = text.replace("{% materialization ", "{% block mat_").replace(
            "{% endmaterialization %}", "{% endblock %}"
        )
        sanitized = re.sub(
            r"\{%\s*block\s+mat_(\w+),\s*adapter='[^']+'\s*%\}",
            r"{% block mat_\1 %}",
            sanitized,
        )
        env.parse(sanitized)
