from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import jinja2

from dbt_virtual.cli import main
from dbt_virtual.fingerprint import (
    compute_manifest_fingerprints,
)


def _sample_manifest() -> dict:
    return {
        "nodes": {
            "model.pkg.sessions_base": {
                "unique_id": "model.pkg.sessions_base",
                "name": "sessions_base",
                "alias": "sessions_base",
                "resource_type": "model",
                "schema": "rt_pipeline_prod1",
                "compiled_code": "SELECT user_id, dt FROM raw.events",
                "raw_code": "SELECT user_id, dt FROM {{ source('raw', 'events') }}",
                "config": {
                    "materialized": "incremental",
                    "virtual_env": False,
                },
                "depends_on": {"nodes": []},
            },
            "model.pkg.traffic_metrics_root": {
                "unique_id": "model.pkg.traffic_metrics_root",
                "name": "traffic_metrics_root",
                "alias": "traffic_metrics_root",
                "resource_type": "model",
                "schema": "spacetime_staging",
                "compiled_code": (
                    "SELECT user_id, COUNT(*) AS sessions "
                    "FROM `proj.rt_pipeline_prod1.sessions_base` GROUP BY 1"
                ),
                "raw_code": (
                    "SELECT user_id, COUNT(*) AS sessions "
                    "FROM {{ ref('sessions_base') }} GROUP BY 1"
                ),
                "config": {
                    "materialized": "incremental",
                    "incremental_strategy": "insert_overwrite",
                    "partition_by": {"field": "dt", "data_type": "date"},
                    "cluster_by": ["user_id"],
                    "virtual_env": True,
                },
                "depends_on": {"nodes": ["model.pkg.sessions_base"]},
            },
            "model.pkg.udf_spacetime_gl": {
                "unique_id": "model.pkg.udf_spacetime_gl",
                "name": "udf_spacetime_gl",
                "alias": "udf_spacetime_gl",
                "resource_type": "model",
                "schema": "spacetime_staging",
                "compiled_code": "CASE gl WHEN 'property' THEN ['p1'] ELSE [] END",
                "raw_code": "CASE gl WHEN 'property' THEN ['p1'] ELSE [] END",
                "config": {
                    "materialized": "function",
                    "parameters": "gl STRING",
                    "returns": "ARRAY<STRING>",
                    "virtual_env": True,
                },
                "depends_on": {"nodes": []},
            },
            "model.pkg.ft_report_v9_traffic_metrics": {
                "unique_id": "model.pkg.ft_report_v9_traffic_metrics",
                "name": "ft_report_v9_traffic_metrics",
                "alias": "ft_report_v9_traffic_metrics",
                "resource_type": "model",
                "schema": "spacetime_staging",
                "compiled_code": (
                    "SELECT user_id, sessions "
                    "FROM `proj.spacetime_staging.traffic_metrics_root`"
                ),
                "raw_code": (
                    "SELECT user_id, sessions "
                    "FROM {{ tvf_test_ref('traffic_metrics_root') }}"
                ),
                "config": {
                    "materialized": "table_function",
                    "parameters": "start_dt DATE, end_dt DATE",
                    "virtual_env": True,
                },
                "depends_on": {
                    "nodes": [
                        "model.pkg.traffic_metrics_root",
                        "model.pkg.udf_spacetime_gl",
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
            "spacetime_staging",
            "--format",
            "json",
        ]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["environment"] == "staging"
    assert out["total_virtual_nodes"] == 3
    fp_file = tmp_path / ".dbt_virtual" / "fingerprints.json"
    assert fp_file.exists()
    fp_data = json.loads(fp_file.read_text(encoding="utf-8"))
    assert set(fp_data["by_name"].keys()) == {
        "traffic_metrics_root",
        "udf_spacetime_gl",
        "ft_report_v9_traffic_metrics",
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
    assert "traffic_metrics_root" in txt_out
    assert "ft_report_v9_traffic_metrics" in txt_out


def test_cli_build_promote_rollback_gc_subcommands(tmp_path: Path) -> None:
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
                    "+traffic_metrics_root",
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

    assert len(recorded_cmds) == 4
    build_cmd = recorded_cmds[0]
    assert build_cmd[:3] == ["build", "--target", "staging"]
    assert "--full-refresh" in build_cmd
    vars_payload = json.loads(build_cmd[build_cmd.index("--vars") + 1])
    assert vars_payload["_dbt_virtual_env"] == "staging"
    assert vars_payload["_dbt_virtual_release"] == "v2026.10.27"
    assert "traffic_metrics_root" in vars_payload["_dbt_virtual_fps"]

    promote_cmd = recorded_cmds[1]
    assert promote_cmd[:2] == ["run-operation", "virtual_promote"]

    rollback_cmd = recorded_cmds[2]
    assert rollback_cmd[:2] == ["run-operation", "virtual_rollback"]

    gc_cmd = recorded_cmds[3]
    assert gc_cmd[:2] == ["run-operation", "virtual_gc"]


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
    import re

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
