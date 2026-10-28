"""Real dbt-core + dbt-duckdb CLI end-to-end integration test on sample_dbt_project."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

from dbt_virtual.cli import main as dbt_virtual_main

REPO = Path(__file__).resolve().parent.parent
SAMPLE_PROJECT = REPO / "tests" / "fixtures" / "sample_dbt_project"


def _run_dbt_cmd(args: list[str], cwd: Path) -> None:
    cmd = [sys.executable, "-m", "dbt.cli.main", *args]
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        raise AssertionError(
            f"Command {' '.join(cmd)} failed (rc={res.returncode}):\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
        )


def test_real_dbt_duckdb_project_e2e(tmp_path: Path) -> None:
    proj_dir = tmp_path / "sample_dbt_project"
    shutil.copytree(SAMPLE_PROJECT, proj_dir)

    # Copy dbt_virtual package into a clean isolated directory (without tests/fixtures) so `dbt deps`
    # does not parse tests/fixtures/sample_dbt_project/dbt_project.yml as a model schema YAML
    pkg_dir = tmp_path / "dbt_virtual_pkg"
    pkg_dir.mkdir(parents=True)
    shutil.copy2(REPO / "dbt_project.yml", pkg_dir / "dbt_project.yml")
    shutil.copytree(REPO / "macros", pkg_dir / "macros")

    (proj_dir / "packages.yml").write_text(
        f'packages:\n  - local: "{pkg_dir.as_posix()}"\n',
        encoding="utf-8",
    )

    db_path = (tmp_path / "virtual_warehouse.duckdb").as_posix()
    (proj_dir / "profiles.yml").write_text(
        f"""sample_virtual_project:
  target: staging
  outputs:
    staging:
      type: duckdb
      path: "{db_path}"
      schema: analytics_staging
      threads: 1
    prod:
      type: duckdb
      path: "{db_path}"
      schema: analytics
      threads: 1
    pr_42:
      type: duckdb
      path: "{db_path}"
      schema: analytics_pr_42
      threads: 1
""",
        encoding="utf-8",
    )

    # Pre-create schemas in DuckDB so views can be created cleanly
    conn = duckdb.connect(db_path)
    for schema_name in ("analytics_physical", "analytics_staging", "analytics", "analytics_pr_42"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.close()

    # 1. Install local dbt_virtual package via `dbt deps`
    _run_dbt_cmd(["deps", "--profiles-dir", str(proj_dir)], proj_dir)

    # 2. Run `dbt compile` and `dbt-virtual plan --skip-compile`
    _run_dbt_cmd(["compile", "--profiles-dir", str(proj_dir), "--target", "staging"], proj_dir)
    rc_plan = dbt_virtual_main(
        [
            "--project-dir",
            str(proj_dir),
            "plan",
            "--env",
            "staging",
            "--skip-compile",
            "--format",
            "json",
        ]
    )
    assert rc_plan == 0
    fp_data = json.loads((proj_dir / ".dbt_virtual" / "fingerprints.json").read_text(encoding="utf-8"))
    fp_v1 = fp_data["by_name"]["orders_metrics"]
    assert len(fp_v1) == 12

    # 3. Execute real `dbt build` with `_dbt_virtual_fps`, `_dbt_virtual_release`, and interval vars
    dbt_vars = {
        "_dbt_virtual_fps": {"orders_metrics": fp_v1},
        "_dbt_virtual_env": "staging",
        "_dbt_virtual_release": "v1",
        "_dbt_virtual_start": "2026-10-01",
        "_dbt_virtual_end": "2026-10-27",
    }
    _run_dbt_cmd(
        [
            "build",
            "--profiles-dir",
            str(proj_dir),
            "--target",
            "staging",
            "--vars",
            json.dumps(dbt_vars),
        ],
        proj_dir,
    )

    # Verify physical table, virtual view, downstream view, _environments, _releases, and _intervals in DuckDB
    conn = duckdb.connect(db_path)
    phys_rows = conn.execute(
        f"SELECT SUM(orders) FROM analytics_physical.orders_metrics__{fp_v1}"
    ).fetchone()
    assert phys_rows == (35,)

    stg_view_rows = conn.execute("SELECT SUM(orders) FROM analytics_staging.orders_metrics").fetchone()
    assert stg_view_rows == (35,)

    summary_rows = conn.execute(
        "SELECT customer_id, doubled_orders FROM analytics_staging.orders_summary_view"
    ).fetchall()
    assert summary_rows == [("store_2", 50)]

    interval_rows = conn.execute(
        "SELECT model_name, fingerprint, start_ts, end_ts FROM analytics_physical._intervals"
    ).fetchall()
    assert interval_rows == [("orders_metrics", fp_v1, "2026-10-01", "2026-10-27")]
    conn.close()

    # 4. Run `dbt run-operation virtual_promote` (with `audit=true`) to promote `v1` from `staging` -> `prod`
    _run_dbt_cmd(
        [
            "run-operation",
            "virtual_promote",
            "--profiles-dir",
            str(proj_dir),
            "--args",
            json.dumps({"from_env": "staging", "to_env": "prod", "release_id": "v1", "audit": True}),
        ],
        proj_dir,
    )

    conn = duckdb.connect(db_path)
    prod_rows = conn.execute("SELECT SUM(orders) FROM analytics.orders_metrics").fetchone()
    assert prod_rows == (35,)
    conn.close()

    # 5. Run `virtual_acquire_lock`, `virtual_export_state`, and `virtual_release_lock` via `dbt run-operation`
    _run_dbt_cmd(
        [
            "run-operation",
            "virtual_acquire_lock",
            "--profiles-dir",
            str(proj_dir),
            "--args",
            json.dumps({"env": "prod", "owner": "e2e_test", "ttl_minutes": 10}),
        ],
        proj_dir,
    )
    _run_dbt_cmd(
        [
            "run-operation",
            "virtual_export_state",
            "--profiles-dir",
            str(proj_dir),
            "--args",
            json.dumps({"env": "staging", "base_env": "prod"}),
        ],
        proj_dir,
    )
    _run_dbt_cmd(
        [
            "run-operation",
            "virtual_release_lock",
            "--profiles-dir",
            str(proj_dir),
            "--args",
            json.dumps({"env": "prod", "owner": "e2e_test"}),
        ],
        proj_dir,
    )
