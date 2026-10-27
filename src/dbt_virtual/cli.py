"""CLI entry point for dbt-virtual (plan, build, promote, rollback, diff, gc)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence

from dbt_virtual import __version__
from dbt_virtual.fingerprint import (
    compute_manifest_fingerprints,
    write_fingerprints_file,
)


def _load_manifest(project_dir: Path) -> dict:
    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found at {manifest_path}. Run `dbt compile` or omit `--skip-compile`."
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _run_dbt(args: list[str], project_dir: Path) -> int:
    cmd = ["dbt", *args]
    result = subprocess.run(cmd, cwd=project_dir, check=False)
    return result.returncode


def cmd_plan(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    if not ns.skip_compile:
        rc = _run_dbt(["compile", "--target", ns.env], project_dir)
        if rc != 0:
            return rc

    manifest = _load_manifest(project_dir)
    fps = compute_manifest_fingerprints(manifest, known_schemas=ns.known_schemas)
    out_file = project_dir / ".dbt_virtual" / "fingerprints.json"
    write_fingerprints_file(fps, out_file)

    plan_summary = {
        "environment": ns.env,
        "total_virtual_nodes": len(fps),
        "fingerprints_file": str(out_file),
        "nodes": [fp.to_dict() for fp in fps.values()],
    }

    if ns.format == "json":
        print(json.dumps(plan_summary, indent=2))
    else:
        print(f"dbt-virtual plan (env={ns.env}) — {len(fps)} virtualized node(s):")
        for fp in fps.values():
            upstreams = len(fp.upstream_fingerprints)
            print(
                f"  - {fp.name:<36} [{fp.materialized:<14}] "
                f"fp={fp.fingerprint} -> {fp.physical_identifier} (upstreams={upstreams})"
            )
    return 0


def cmd_build(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    if not ns.skip_compile:
        rc = _run_dbt(["compile", "--target", ns.env], project_dir)
        if rc != 0:
            return rc

    manifest = _load_manifest(project_dir)
    fps = compute_manifest_fingerprints(manifest, known_schemas=ns.known_schemas)
    out_file = project_dir / ".dbt_virtual" / "fingerprints.json"
    write_fingerprints_file(fps, out_file)

    by_name = {fp.name: fp.fingerprint for fp in fps.values()}
    dbt_vars = {
        "_dbt_virtual_fps": by_name,
        "_dbt_virtual_env": ns.env,
        "_dbt_virtual_release": ns.release or "",
    }
    build_args = [
        "build",
        "--target",
        ns.env,
        "--vars",
        json.dumps(dbt_vars),
    ]
    if ns.select:
        build_args.extend(["--select", ns.select])
    if ns.full_refresh:
        build_args.append("--full-refresh")
    return _run_dbt(build_args, project_dir)


def cmd_promote(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "from_env": ns.from_env,
        "to_env": ns.to_env,
        "release_id": ns.release,
    }
    return _run_dbt(
        ["run-operation", "virtual_promote", "--args", json.dumps(op_args)],
        project_dir,
    )


def cmd_rollback(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "env": ns.env,
        "to_release": ns.to_release or "",
    }
    return _run_dbt(
        ["run-operation", "virtual_rollback", "--args", json.dumps(op_args)],
        project_dir,
    )


def cmd_gc(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "dry_run": ns.dry_run,
        "retention_releases": ns.retention_releases,
        "retention_days": ns.retention_days,
    }
    return _run_dbt(
        ["run-operation", "virtual_gc", "--args", json.dumps(op_args)],
        project_dir,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dbt-virtual",
        description="SQLMesh-style virtual data environments for dbt.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--project-dir",
        default=".",
        help="Path to dbt project directory (default: current directory).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # plan
    p_plan = sub.add_parser("plan", help="Compute Merkle-DAG fingerprints and preview environment plan.")
    p_plan.add_argument("--env", default="staging", help="Target virtual environment name.")
    p_plan.add_argument("--skip-compile", action="store_true", help="Reuse existing target/manifest.json.")
    p_plan.add_argument(
        "--known-schema",
        dest="known_schemas",
        action="append",
        default=[],
        help="Known environment schema names to normalize out of SQL fingerprints.",
    )
    p_plan.add_argument("--format", choices=["text", "json"], default="text")
    p_plan.set_defaults(func=cmd_plan)

    # build
    p_build = sub.add_parser("build", help="Seed clones, build physical tables, and sync virtual views.")
    p_build.add_argument("--env", default="staging", help="Target virtual environment name.")
    p_build.add_argument("--release", default="", help="Release identifier (e.g. v2026.10.27 or commit sha).")
    p_build.add_argument("--select", default=None, help="dbt --select expression.")
    p_build.add_argument("--full-refresh", action="store_true", help="Pass --full-refresh to dbt build.")
    p_build.add_argument("--skip-compile", action="store_true", help="Reuse existing target/manifest.json.")
    p_build.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_build.set_defaults(func=cmd_build)

    # promote
    p_promote = sub.add_parser("promote", help="Zero-rebuild promotion by repointing views and routines.")
    p_promote.add_argument("--from", dest="from_env", default="staging", help="Source environment.")
    p_promote.add_argument("--to", dest="to_env", default="prod", help="Target environment.")
    p_promote.add_argument("--release", required=True, help="Release ID to promote.")
    p_promote.set_defaults(func=cmd_promote)

    # rollback
    p_rollback = sub.add_parser("rollback", help="Instant rollback of views and routines to a previous release.")
    p_rollback.add_argument("--env", default="prod", help="Environment to roll back.")
    p_rollback.add_argument("--to-release", default=None, help="Specific release ID (defaults to last good release).")
    p_rollback.set_defaults(func=cmd_rollback)

    # gc
    p_gc = sub.add_parser("gc", help="Drop unreferenced physical fingerprint tables past retention.")
    p_gc.add_argument("--dry-run", action="store_true", help="Print tables to drop without dropping them.")
    p_gc.add_argument("--retention-releases", type=int, default=5)
    p_gc.add_argument("--retention-days", type=int, default=7)
    p_gc.set_defaults(func=cmd_gc)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    ns = parser.parse_args(argv)
    return ns.func(ns)


if __name__ == "__main__":
    sys.exit(main())
