"""CLI entry point for dbt-virtual (plan, build, promote, rollback, diff, audit, status, gc, init-env, drop-env)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from dbt_virtual import __version__
from dbt_virtual.fingerprint import (
    compute_manifest_fingerprints,
    write_fingerprints_file,
)
from dbt_virtual.planner import (
    classify_plan_actions,
    compute_dbt_select_for_build,
    diff_manifests,
)


def _load_manifest(project_dir: Path, manifest_override: str | None = None) -> dict:
    manifest_path = (
        Path(manifest_override).resolve()
        if manifest_override
        else project_dir / "target" / "manifest.json"
    )
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found at {manifest_path}. Run `dbt compile` or omit `--skip-compile`."
        )
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _load_state_file(state_path: str | None) -> tuple[dict[str, str], dict[str, str], set[str]]:
    """Load optional warehouse state snapshot JSON (`env_state`, `base_env_state`, `existing_physical_tables`)."""
    if not state_path:
        return {}, {}, set()
    raw = json.loads(Path(state_path).read_text(encoding="utf-8"))
    env_state = dict(raw.get("env_state") or {})
    base_env_state = dict(raw.get("base_env_state") or {})
    existing_tables = set(raw.get("existing_physical_tables") or [])
    return env_state, base_env_state, existing_tables


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

    env_state, base_env_state, existing_tables = _load_state_file(ns.state_file)
    actions = classify_plan_actions(
        fps,
        env_state=env_state,
        base_env_state=base_env_state,
        existing_physical_tables=existing_tables,
    )

    counts: dict[str, int] = {}
    for act in actions.values():
        counts[act.action] = counts.get(act.action, 0) + 1

    plan_summary = {
        "environment": ns.env,
        "total_virtual_nodes": len(fps),
        "action_counts": counts,
        "fingerprints_file": str(out_file),
        "nodes": [
            {
                **fp.to_dict(),
                "action": actions[uid].action,
                "reason": actions[uid].reason,
                "base_fingerprint": actions[uid].base_fingerprint,
            }
            for uid, fp in fps.items()
        ],
    }

    if ns.format == "json":
        print(json.dumps(plan_summary, indent=2))
    else:
        print(f"dbt-virtual plan (env={ns.env}) — {len(fps)} virtualized node(s):")
        for uid, fp in fps.items():
            act = actions[uid]
            upstreams = len(fp.upstream_fingerprints)
            print(
                f"  - [{act.action:<16}] {fp.name:<32} [{fp.materialized:<14}] "
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

    env_state, base_env_state, existing_tables = _load_state_file(ns.state_file)
    actions = classify_plan_actions(
        fps,
        env_state=env_state,
        base_env_state=base_env_state,
        existing_physical_tables=existing_tables,
        full_refresh=ns.full_refresh,
    )
    models_to_build, reused_models = compute_dbt_select_for_build(actions)

    # 1. Zero-compute view pointer sync for REUSE models
    if reused_models:
        sync_args = {
            "env": ns.env,
            "release_id": ns.release or "",
            "models_json": json.dumps(reused_models),
        }
        rc = _run_dbt(
            ["run-operation", "virtual_sync_reused_views", "--args", json.dumps(sync_args)],
            project_dir,
        )
        if rc != 0:
            return rc

    # If all virtual models are REUSE and no explicit --select was passed, skip dbt build altogether
    if not models_to_build and reused_models and not ns.select:
        print(f"dbt-virtual build: All {len(reused_models)} model(s) reused with zero compute.")
        return 0

    by_name = {fp.name: fp.fingerprint for fp in fps.values()}
    dbt_vars: dict[str, object] = {
        "_dbt_virtual_fps": by_name,
        "_dbt_virtual_env": ns.env,
        "_dbt_virtual_release": ns.release or "",
    }
    if ns.start:
        dbt_vars["_dbt_virtual_start"] = ns.start
    if ns.end:
        dbt_vars["_dbt_virtual_end"] = ns.end

    build_args = [
        "build",
        "--target",
        ns.env,
        "--vars",
        json.dumps(dbt_vars),
    ]
    if ns.select:
        build_args.extend(["--select", ns.select])
    elif models_to_build:
        build_args.extend(["--select", " ".join(models_to_build)])
    if ns.full_refresh:
        build_args.append("--full-refresh")
    return _run_dbt(build_args, project_dir)


def cmd_promote(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "from_env": ns.from_env,
        "to_env": ns.to_env,
        "release_id": ns.release,
        "audit": bool(ns.audit),
        "max_row_drop_pct": float(ns.max_row_drop_pct),
    }
    return _run_dbt(
        ["run-operation", "virtual_promote", "--args", json.dumps(op_args)],
        project_dir,
    )


def cmd_audit(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "from_env": ns.from_env,
        "to_env": ns.to_env,
        "release_id": ns.release or "",
        "allow_empty": bool(ns.allow_empty),
        "max_row_drop_pct": float(ns.max_row_drop_pct),
    }
    return _run_dbt(
        ["run-operation", "virtual_audit", "--args", json.dumps(op_args)],
        project_dir,
    )


def cmd_status(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "env": ns.env,
        "base_env": ns.base_env,
    }
    dbt_args = ["run-operation", "virtual_export_state", "--args", json.dumps(op_args)]
    if not ns.output:
        return _run_dbt(dbt_args, project_dir)

    result = subprocess.run(
        ["dbt", *dbt_args],
        cwd=project_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n")
    if result.stderr:
        print(result.stderr, file=sys.stderr, end="" if result.stderr.endswith("\n") else "\n")
    if result.returncode != 0:
        return result.returncode

    state_marker = "DBT_VIRTUAL_STATE_JSON:"
    state_json = None
    for output in (result.stdout, result.stderr):
        for line in output.splitlines():
            if state_marker in line:
                state_json = line.split(state_marker, 1)[1].strip()
                break
        if state_json is not None:
            break
    if state_json is None:
        print(
            "dbt-virtual status: dbt completed but did not emit DBT_VIRTUAL_STATE_JSON.",
            file=sys.stderr,
        )
        return 1

    try:
        state = json.loads(state_json)
    except json.JSONDecodeError as exc:
        print(f"dbt-virtual status: invalid state JSON: {exc}", file=sys.stderr)
        return 1

    output_path = Path(ns.output)
    if not output_path.is_absolute():
        output_path = project_dir / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    print(f"dbt-virtual status: wrote warehouse state to {output_path}")
    return 0


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


def cmd_diff(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    base_manifest = _load_manifest(project_dir, ns.base_manifest)
    target_manifest = _load_manifest(project_dir, ns.target_manifest)
    diffs = diff_manifests(base_manifest, target_manifest)
    has_breaking = any(d.classification == "BREAKING" for d in diffs)

    payload = {
        "has_breaking_changes": has_breaking,
        "models_changed": len(diffs),
        "diffs": [d.to_dict() for d in diffs],
    }
    if ns.format == "json":
        print(json.dumps(payload, indent=2))
    else:
        print(f"dbt-virtual diff — {len(diffs)} model(s) with schema changes:")
        for d in diffs:
            print(f"  - {d.model_name} [{d.classification}]:")
            for c in d.changes:
                print(
                    f"      {c.change_type}: {c.column} ({c.old_type or '-'} -> {c.new_type or '-'})"
                )
    if has_breaking and ns.fail_on_breaking:
        return 2
    return 0


def cmd_init_env(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "env": ns.env,
        "from_env": ns.from_env,
    }
    return _run_dbt(
        ["run-operation", "virtual_init_env", "--args", json.dumps(op_args)],
        project_dir,
    )


def cmd_drop_env(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "env": ns.env,
    }
    return _run_dbt(
        ["run-operation", "virtual_drop_env", "--args", json.dumps(op_args)],
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


def cmd_adopt(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    manifest_path = project_dir / "target" / "manifest.json"
    if not ns.skip_compile or not manifest_path.exists():
        rc = _run_dbt(["compile"], project_dir)
        if rc != 0:
            return rc

    manifest = _load_manifest(project_dir)
    fps = compute_manifest_fingerprints(manifest, known_schemas=ns.known_schemas or [])
    fp_map = {v.name: v.fingerprint for v in fps.values()}

    op_args = {
        "env": ns.env,
        "release_id": ns.release,
        "drop_existing_table": not ns.keep_existing_table,
    }
    dbt_vars = {"_dbt_virtual_fps": fp_map}
    return _run_dbt(
        [
            "run-operation",
            "virtual_adopt",
            "--args",
            json.dumps(op_args),
            "--vars",
            json.dumps(dbt_vars),
        ],
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
        "--state-file",
        default=None,
        help="Optional JSON file containing warehouse env_state / base_env_state / existing_physical_tables.",
    )
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
    p_build.add_argument("--start", default=None, help="Optional backfill start timestamp/date var.")
    p_build.add_argument("--end", default=None, help="Optional backfill end timestamp/date var.")
    p_build.add_argument("--state-file", default=None, help="Optional warehouse state JSON file for REUSE skipping.")
    p_build.add_argument("--full-refresh", action="store_true", help="Pass --full-refresh to dbt build.")
    p_build.add_argument("--skip-compile", action="store_true", help="Reuse existing target/manifest.json.")
    p_build.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_build.set_defaults(func=cmd_build)

    # promote
    p_promote = sub.add_parser("promote", help="Zero-rebuild promotion by repointing views and routines.")
    p_promote.add_argument("--from", dest="from_env", default="staging", help="Source environment.")
    p_promote.add_argument("--to", dest="to_env", default="prod", help="Target environment.")
    p_promote.add_argument("--release", required=True, help="Release ID to promote.")
    p_promote.add_argument(
        "--audit",
        action="store_true",
        help="Run virtual_audit quality gate before promoting.",
    )
    p_promote.add_argument(
        "--max-row-drop-pct",
        type=float,
        default=100.0,
        help="Maximum allowed row drop percentage when --audit is enabled.",
    )
    p_promote.set_defaults(func=cmd_promote)

    # audit
    p_audit = sub.add_parser("audit", help="Pre-promotion quality gate comparing candidate vs base table row counts.")
    p_audit.add_argument("--from", dest="from_env", default="staging", help="Candidate environment.")
    p_audit.add_argument("--to", dest="to_env", default="prod", help="Base environment to compare against.")
    p_audit.add_argument("--release", default="", help="Optional release ID to audit.")
    p_audit.add_argument("--allow-empty", action="store_true", help="Allow 0-row candidate tables.")
    p_audit.add_argument(
        "--max-row-drop-pct",
        type=float,
        default=100.0,
        help="Fail if row count drops by more than this percentage vs base.",
    )
    p_audit.set_defaults(func=cmd_audit)

    # status
    p_status = sub.add_parser("status", help="Export live warehouse virtual environment state JSON.")
    p_status.add_argument("--env", default="staging", help="Target environment to inspect.")
    p_status.add_argument("--base-env", default="prod", help="Base environment to compare.")
    p_status.add_argument("--output", default=None, help="Optional path to write the exported warehouse state JSON.")
    p_status.set_defaults(func=cmd_status)

    # rollback
    p_rollback = sub.add_parser("rollback", help="Instant rollback of views and routines to a previous release.")
    p_rollback.add_argument("--env", default="prod", help="Environment to roll back.")
    p_rollback.add_argument("--to-release", default=None, help="Specific release ID (defaults to last good release).")
    p_rollback.set_defaults(func=cmd_rollback)

    # diff
    p_diff = sub.add_parser("diff", help="Compare column schemas across manifests and classify breaking changes.")
    p_diff.add_argument("--base-manifest", required=True, help="Path to base (e.g. prod) manifest.json.")
    p_diff.add_argument("--target-manifest", default=None, help="Path to target manifest.json (defaults to target/manifest.json).")
    p_diff.add_argument("--fail-on-breaking", action="store_true", help="Exit with code 2 if breaking column changes are detected.")
    p_diff.add_argument("--format", choices=["text", "json"], default="text")
    p_diff.set_defaults(func=cmd_diff)

    # init-env
    p_init = sub.add_parser("init-env", help="Bootstrap an ephemeral PR environment from base_environment in 0 compute.")
    p_init.add_argument("--env", required=True, help="New virtual environment name (e.g. pr_123).")
    p_init.add_argument("--from", dest="from_env", default="prod", help="Base environment to point initial views at.")
    p_init.set_defaults(func=cmd_init_env)

    # drop-env
    p_drop = sub.add_parser("drop-env", help="Tear down an ephemeral virtual environment schema and registry pointers.")
    p_drop.add_argument("--env", required=True, help="Virtual environment name to drop (e.g. pr_123).")
    p_drop.set_defaults(func=cmd_drop_env)

    # gc
    p_gc = sub.add_parser("gc", help="Drop unreferenced physical fingerprint tables past retention.")
    p_gc.add_argument("--dry-run", action="store_true", help="Print tables to drop without dropping them.")
    p_gc.add_argument("--retention-releases", type=int, default=5)
    p_gc.add_argument("--retention-days", type=int, default=7)
    p_gc.set_defaults(func=cmd_gc)

    # adopt
    p_adopt = sub.add_parser(
        "adopt",
        help="Zero-compute migration: clone existing production tables into physical_schema and convert them to virtual views.",
    )
    p_adopt.add_argument("--env", default="prod", help="Existing environment to adopt (default: prod).")
    p_adopt.add_argument("--release", default="v_adopt_initial", help="Initial baseline release ID.")
    p_adopt.add_argument(
        "--keep-existing-table",
        action="store_true",
        help="Clone into physical_schema and register in _environments without replacing the existing table with a view yet.",
    )
    p_adopt.add_argument("--skip-compile", action="store_true", help="Reuse existing target/manifest.json.")
    p_adopt.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_adopt.set_defaults(func=cmd_adopt)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    ns = parser.parse_args(argv)
    return ns.func(ns)


if __name__ == "__main__":
    sys.exit(main())
