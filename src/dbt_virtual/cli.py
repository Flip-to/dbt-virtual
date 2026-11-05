"""CLI entry point for dbt-virtual (plan, build, run, promote, rollback, diff, audit, status, gc, init-env, drop-env)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from dbt_virtual import __version__
from dbt_virtual.fingerprint import (
    MissingCompiledCodeError,
    compute_manifest_fingerprints,
    explain_fingerprint,
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


def _fingerprints(manifest: dict, ns: argparse.Namespace) -> dict:
    """Fingerprints for the manifest; exits 2 with a hint when it was parsed, not compiled."""
    try:
        return compute_manifest_fingerprints(
            manifest,
            known_schemas=getattr(ns, "known_schemas", None) or [],
            hash_compiled_ancestors=getattr(ns, "hash_compiled_ancestors", False),
        )
    except MissingCompiledCodeError as exc:
        raise SystemExit(f"dbt-virtual: {exc}") from exc


def _load_state_file(
    state_path: str | None,
) -> tuple[dict[str, str], dict[str, str], set[str] | None, set[str] | None]:
    """Load an optional warehouse state snapshot (`dbt-virtual status --output`).

    Returns `env_state`, `base_env_state`, `existing_physical_tables` and
    `registered_physical_tables` (the last two are None when the snapshot lacks them).
    """
    if not state_path:
        return {}, {}, set(), None
    raw = json.loads(Path(state_path).read_text(encoding="utf-8"))
    env_state = dict(raw.get("env_state") or {})
    base_env_state = dict(raw.get("base_env_state") or {})
    existing_tables_raw = raw.get("existing_physical_tables")
    existing_tables = set(existing_tables_raw) if existing_tables_raw is not None else None
    registered_raw = raw.get("registered_physical_tables")
    registered = set(registered_raw) if registered_raw is not None else None
    return env_state, base_env_state, existing_tables, registered


def _load_partial_tables(state_path: str | None) -> set[str]:
    """Physical tables the state snapshot marks as built from a window or with `--empty`."""
    if not state_path:
        return set()
    raw = json.loads(Path(state_path).read_text(encoding="utf-8"))
    return set(raw.get("partial_physical_tables") or [])


def _base_environment(project_dir: Path) -> str:
    """`vars.dbt_virtual.base_environment` from dbt_project.yml (default `prod`)."""
    try:
        import yaml

        project = yaml.safe_load((project_dir / "dbt_project.yml").read_text(encoding="utf-8")) or {}
        value = ((project.get("vars") or {}).get("dbt_virtual") or {}).get("base_environment")
    except Exception:  # noqa: BLE001 - no project file / unparsable: use the default
        value = None
    return value if isinstance(value, str) and value and "{{" not in value else "prod"


def _default_lock_owner() -> str:
    """A lease owner that is stable for one user on one machine, so a failed promote can be retried."""
    import getpass
    import socket

    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 - no login name in some CI containers
        user = "unknown"
    return f"dbt-virtual-cli:{user}@{socket.gethostname()}"


def _dbt_target(ns: argparse.Namespace) -> str:
    """dbt profile target: `--target`, else the environment name (previous behaviour)."""
    return getattr(ns, "target", None) or ns.env


def _user_vars(ns: argparse.Namespace) -> dict[str, object]:
    """Extra dbt vars from `--vars` (JSON object). dbt-virtual's own `_dbt_virtual_*` vars win."""
    raw = getattr(ns, "vars", None)
    if not raw:
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SystemExit("dbt-virtual: --vars must be a JSON object")
    return value


def _state_args(ns: argparse.Namespace) -> list[str]:
    args: list[str] = []
    if getattr(ns, "defer", False):
        args.append("--defer")
    if getattr(ns, "state", None):
        args.extend(["--state", ns.state])
    return args


_DEFAULT_TARGET: str | None = None


def _run_dbt(args: list[str], project_dir: Path) -> int:
    # Commands without their own target (promote, rollback, status, gc, adopt, init/drop-env)
    # used the profile's default target, which is prod in some projects.
    if _DEFAULT_TARGET and "--target" not in args:
        args = [*args, "--target", _DEFAULT_TARGET]
    cmd = ["dbt", *args]
    result = subprocess.run(cmd, cwd=project_dir, check=False)
    return result.returncode


def cmd_plan(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    if not ns.skip_compile:
        compile_args = ["compile", "--target", _dbt_target(ns)]
        if _user_vars(ns):
            compile_args.extend(["--vars", json.dumps(_user_vars(ns))])
        rc = _run_dbt(compile_args, project_dir)
        if rc != 0:
            return rc

    manifest = _load_manifest(project_dir)
    if ns.explain_fingerprint:
        try:
            print(
                explain_fingerprint(
                    manifest,
                    ns.explain_fingerprint,
                    known_schemas=ns.known_schemas,
                    hash_compiled_ancestors=ns.hash_compiled_ancestors,
                    include_ancestors=ns.explain_ancestors,
                )
            )
        except MissingCompiledCodeError as exc:
            raise SystemExit(f"dbt-virtual: {exc}") from exc
        except ValueError as exc:
            raise SystemExit(f"dbt-virtual: {exc}") from exc
        return 0
    fps = _fingerprints(manifest, ns)
    out_file = project_dir / ".dbt_virtual" / "fingerprints.json"
    write_fingerprints_file(fps, out_file)

    env_state, base_env_state, existing_tables, registered = _load_state_file(ns.state_file)
    actions = classify_plan_actions(
        fps,
        env_state=env_state,
        base_env_state=base_env_state,
        existing_physical_tables=existing_tables,
        registered_physical_tables=registered,
        partial_physical_tables=_load_partial_tables(ns.state_file),
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
        compile_args = ["compile", "--target", _dbt_target(ns)]
        if _user_vars(ns):
            compile_args.extend(["--vars", json.dumps(_user_vars(ns))])
        rc = _run_dbt(compile_args, project_dir)
        if rc != 0:
            return rc

    manifest = _load_manifest(project_dir)
    fps = _fingerprints(manifest, ns)
    out_file = project_dir / ".dbt_virtual" / "fingerprints.json"
    write_fingerprints_file(fps, out_file)

    env_state, base_env_state, existing_tables, registered = _load_state_file(ns.state_file)
    actions = classify_plan_actions(
        fps,
        env_state=env_state,
        base_env_state=base_env_state,
        existing_physical_tables=existing_tables,
        full_refresh=ns.full_refresh,
        registered_physical_tables=registered,
        partial_physical_tables=_load_partial_tables(ns.state_file),
    )
    models_to_build, reused_models = compute_dbt_select_for_build(actions)

    by_name = {fp.name: fp.fingerprint for fp in fps.values()}
    dbt_vars: dict[str, object] = {
        **_user_vars(ns),
        "_dbt_virtual_fps": by_name,
        "_dbt_virtual_env": ns.env,
        "_dbt_virtual_release": ns.release or "",
    }
    if ns.start:
        dbt_vars["_dbt_virtual_start"] = ns.start
    if ns.end:
        dbt_vars["_dbt_virtual_end"] = ns.end

    seed_names = sorted(a.name for a in actions.values() if a.action == "SEED_INCREMENTAL")
    # DECISION(single_dbt_build_invocation): REUSE view sync, incremental seeding (dbt decides
    # "new table" before model pre-hooks run, so it must precede the build) and the release
    # snapshot used to be three `dbt run-operation` calls around `dbt build`, each paying a full
    # project parse. They now run in the build's own on-run-start / on-run-end hooks
    # (`dbt_virtual.run_start()`, `dbt_virtual.flush_registry(results)`), driven by these vars.
    if reused_models:
        dbt_vars["_dbt_virtual_reuse"] = reused_models
    if seed_names:
        dbt_vars["_dbt_virtual_seed"] = seed_names
    if ns.release:
        # Record every current virtual node in the release, not only what dbt built, so that
        # promote/rollback of this release do not demote REUSE models and unchanged routines.
        dbt_vars["_dbt_virtual_snapshot"] = True

    # If the plan has nothing to build and no explicit selector overrides it, do not fall through
    # to an unscoped `dbt build` (which would build the whole project). `dbt build` of an empty
    # selection runs no hooks, so REUSE sync and the snapshot take one `run-operation` instead.
    if not models_to_build and not ns.select:
        if reused_models:
            print(f"dbt-virtual build: All {len(reused_models)} model(s) reused with zero compute.")
        else:
            print("dbt-virtual build: No planned models require a build.")
        if not reused_models and not ns.release:
            return 0
        return _run_dbt(
            [
                "run-operation",
                "virtual_finalize_plan",
                "--target",
                _dbt_target(ns),
                "--args",
                json.dumps(
                    {
                        "env": ns.env,
                        "release_id": ns.release or "",
                        "reuse": reused_models,
                        "snapshot": bool(ns.release),
                    }
                ),
                "--vars",
                json.dumps(dbt_vars),
            ],
            project_dir,
        )

    build_args = [
        "build",
        "--target",
        _dbt_target(ns),
        *_state_args(ns),
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


def _selected_model_names(project_dir: Path, env: str, select: str) -> set[str] | None:
    """Names of the models a dbt `--select` expression matches (via `dbt ls`), or None on failure."""
    result = subprocess.run(
        [
            "dbt", "ls", "--quiet", "--target", env,
            "--resource-type", "model", "--select", select,
            "--output", "json", "--output-keys", "name",
        ],
        cwd=project_dir,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout, end="")
        print(result.stderr, file=sys.stderr, end="")
        return None
    names: set[str] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                names.add(json.loads(line)["name"])
            except (json.JSONDecodeError, KeyError):
                continue
    return names


def cmd_run(ns: argparse.Namespace) -> int:
    """Refresh the data of the table/incremental models an environment already has, in place.

    SQLMesh `run` semantics: a model whose fingerprint is unchanged keeps its physical table
    (`<model>__<fp>`) and is re-run against it (incrementals append the new interval, tables are
    rebuilt). It uses `dbt build`, not `dbt run`, so the models' tests run right after each one and
    a failing test stops the models downstream of it from refreshing on top of bad data.
    A fingerprint that differs from the environment's registered one is a deploy and belongs to
    `build`, so `run` refuses unless `--allow-changes`.
    """
    project_dir = Path(ns.project_dir).resolve()
    if not ns.skip_compile:
        rc = _run_dbt(["compile", "--target", ns.env], project_dir)
        if rc != 0:
            return rc

    manifest = _load_manifest(project_dir)
    fps = _fingerprints(manifest, ns)
    write_fingerprints_file(fps, project_dir / ".dbt_virtual" / "fingerprints.json")

    if ns.state_file:
        env_state, _base, _existing, _registered = _load_state_file(ns.state_file)
    else:
        rc, state = _capture_state(
            project_dir,
            {"env": ns.env, "base_env": ns.base_env},
            target=ns.env,
        )
        if state is None:
            return rc or 1
        env_state = dict(state.get("env_state") or {})

    candidates = sorted(
        (fp for fp in fps.values() if fp.materialized in {"table", "incremental"}),
        key=lambda fp: fp.name,
    )
    if ns.select:
        selected = _selected_model_names(project_dir, ns.env, ns.select)
        if selected is None:
            return 1
        candidates = [fp for fp in candidates if fp.name in selected]
    if not candidates:
        print("dbt-virtual run: no virtual table or incremental models to run.")
        return 0

    changed = [fp for fp in candidates if env_state.get(fp.name) != fp.fingerprint]
    if changed and not ns.allow_changes:
        print(
            f"dbt-virtual run: refusing to run; {len(changed)} model(s) differ from env "
            f"'{ns.env}' (a change is a deploy, use `dbt-virtual build`, or pass --allow-changes):",
            file=sys.stderr,
        )
        for fp in changed:
            registered = env_state.get(fp.name)
            print(
                f"  - {fp.name}: registered={registered or 'none'} current={fp.fingerprint}",
                file=sys.stderr,
            )
        return 2

    by_name = {fp.name: fp.fingerprint for fp in fps.values()}
    dbt_vars: dict[str, object] = {"_dbt_virtual_fps": by_name, "_dbt_virtual_env": ns.env}
    if ns.start:
        dbt_vars["_dbt_virtual_start"] = ns.start
    if ns.end:
        dbt_vars["_dbt_virtual_end"] = ns.end

    # A changed incremental gets a new physical table: seed it from the base environment first,
    # like `build` does for SEED_INCREMENTAL.
    seed_names = [fp.name for fp in changed if fp.materialized == "incremental"]
    if seed_names and not ns.full_refresh:
        rc = _run_dbt(
            [
                "run-operation", "virtual_seed_incremental_clones",
                "--target", ns.env,
                "--args", json.dumps({"model_names": seed_names}),
                "--vars", json.dumps(dbt_vars),
            ],
            project_dir,
        )
        if rc != 0:
            return rc

    build_args = [
        "build",
        "--target",
        ns.env,
        "--vars",
        json.dumps(dbt_vars),
        "--select",
        *[fp.name for fp in candidates],
    ]
    if ns.full_refresh:
        build_args.append("--full-refresh")
    return _run_dbt(build_args, project_dir)


def cmd_promote(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    # Auditing is on by default when promoting into the base environment (usually prod).
    audit = ns.audit if ns.audit is not None else ns.to_env == _base_environment(project_dir)
    op_args: dict[str, object] = {
        "from_env": ns.from_env,
        "to_env": ns.to_env,
        "release_id": ns.release,
        "audit": bool(audit),
        "max_row_drop_pct": float(ns.max_row_drop_pct),
        "lock": not ns.no_lock,
        "lock_owner": ns.lock_owner or _default_lock_owner(),
        "allow_unsealed": bool(ns.allow_unsealed),
        "require_sealed": bool(ns.require_sealed),
        "check_coverage": not ns.no_coverage_check,
        "coverage_ignore_before": ns.coverage_ignore_before or "",
    }
    if ns.skip_fingerprint_check:
        print(
            "dbt-virtual promote: WARNING fingerprint check skipped (--skip-fingerprint-check); a table "
            "built from other code or variables (e.g. a windowed build) can be promoted.",
            file=sys.stderr,
        )
    else:
        # The release must have been built from the code (and variables) of the target environment.
        compile_target = ns.compile_target or ns.to_env
        if not ns.skip_compile:
            rc = _run_dbt(["compile", "--target", compile_target], project_dir)
            if rc != 0:
                return rc
        manifest = _load_manifest(project_dir)
        fps = compute_manifest_fingerprints(manifest, known_schemas=ns.known_schemas)
        op_args["expected_fps_json"] = json.dumps({fp.name: fp.fingerprint for fp in fps.values()})
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
        "check_coverage": not ns.no_coverage_check,
        "coverage_ignore_before": ns.coverage_ignore_before or "",
    }
    return _run_dbt(
        ["run-operation", "virtual_audit", "--args", json.dumps(op_args)],
        project_dir,
    )


def _capture_state(
    project_dir: Path, op_args: dict[str, object], *, target: str | None = None
) -> tuple[int, dict | None]:
    """Run `virtual_export_state` and parse the `DBT_VIRTUAL_STATE_JSON:` line it logs.

    Echoes dbt's output. Returns `(returncode, state)`; `state` is None on any failure.
    """
    dbt_args = ["run-operation", "virtual_export_state", "--args", json.dumps(op_args)]
    if target:
        dbt_args.extend(["--target", target])
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
        return result.returncode, None

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
        return 1, None

    try:
        return 0, json.loads(state_json)
    except json.JSONDecodeError as exc:
        print(f"dbt-virtual status: invalid state JSON: {exc}", file=sys.stderr)
        return 1, None


def cmd_status(ns: argparse.Namespace) -> int:
    project_dir = Path(ns.project_dir).resolve()
    op_args = {
        "env": ns.env,
        "base_env": ns.base_env,
    }
    if not ns.output:
        return _run_dbt(
            ["run-operation", "virtual_export_state", "--args", json.dumps(op_args)],
            project_dir,
        )

    rc, state = _capture_state(project_dir, op_args)
    if state is None:
        return rc

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
        "lock": not ns.no_lock,
        "lock_owner": ns.lock_owner or _default_lock_owner(),
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
    fps = _fingerprints(manifest, ns)
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
        "--hash-compiled-ancestors",
        action="store_true",
        help="Hash the compiled SQL of non-virtual ancestors too (default: raw SQL + macros, which do not depend on warehouse state).",
    )
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
    p_plan.add_argument(
        "--explain-fingerprint",
        metavar="MODEL",
        default=None,
        help="Print what went into MODEL's fingerprint (normalized SQL, config, upstream fingerprints, "
        "tokens that may differ per environment) instead of the plan.",
    )
    p_plan.add_argument(
        "--explain-ancestors",
        action="store_true",
        help="With --explain-fingerprint: also print every ancestor, to find which one differs between environments.",
    )
    p_plan.add_argument(
        "--target",
        default=None,
        help="dbt profile target (default: the --env name). Lets e.g. env pr_42 build with target ci.",
    )
    p_plan.add_argument(
        "--vars",
        default=None,
        help="Extra dbt vars as a JSON object, merged under dbt-virtual's own vars (e.g. window vars).",
    )
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
    p_build.add_argument(
        "--hash-compiled-ancestors",
        action="store_true",
        help="Hash the compiled SQL of non-virtual ancestors too (default: raw SQL + macros, which do not depend on warehouse state).",
    )
    p_build.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_build.add_argument(
        "--target",
        default=None,
        help="dbt profile target (default: the --env name). Lets e.g. env pr_42 build with target ci.",
    )
    p_build.add_argument(
        "--vars",
        default=None,
        help="Extra dbt vars as a JSON object, merged under dbt-virtual's own vars (e.g. window vars).",
    )
    p_build.add_argument("--defer", action="store_true", help="Pass --defer to dbt build.")
    p_build.add_argument("--state", default=None, help="Pass --state <path> to dbt build (with --defer).")
    p_build.set_defaults(func=cmd_build)

    # run
    p_run = sub.add_parser(
        "run",
        help="Refresh table/incremental models in place (same fingerprint, same physical table).",
    )
    p_run.add_argument("--env", default="prod", help="Virtual environment to refresh.")
    p_run.add_argument("--base-env", default="prod", help="Base environment incremental clones are seeded from.")
    p_run.add_argument("--select", default=None, help="Only run the matching models (dbt --select expression).")
    p_run.add_argument("--start", default=None, help="Interval start; recorded in _intervals and passed as a var.")
    p_run.add_argument("--end", default=None, help="Interval end; recorded in _intervals and passed as a var.")
    p_run.add_argument("--state-file", default=None, help="Warehouse state JSON (`status --output`); default: query the warehouse.")
    p_run.add_argument(
        "--allow-changes",
        action="store_true",
        help="Run even when a model's fingerprint differs from the environment's (a deploy).",
    )
    p_run.add_argument("--full-refresh", action="store_true", help="Pass --full-refresh to dbt build.")
    p_run.add_argument("--skip-compile", action="store_true", help="Reuse existing target/manifest.json.")
    p_run.add_argument(
        "--hash-compiled-ancestors",
        action="store_true",
        help="Hash the compiled SQL of non-virtual ancestors too (default: raw SQL + macros, which do not depend on warehouse state).",
    )
    p_run.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_run.set_defaults(func=cmd_run)

    # promote
    p_promote = sub.add_parser("promote", help="Zero-rebuild promotion by repointing views and routines.")
    p_promote.add_argument("--from", dest="from_env", default="staging", help="Source environment.")
    p_promote.add_argument("--to", dest="to_env", default="prod", help="Target environment.")
    p_promote.add_argument("--release", required=True, help="Release ID to promote.")
    p_promote.add_argument(
        "--audit",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run the virtual_audit gate (row counts, BigQuery partition coverage) before promoting. "
        "Default: on when --to is the base environment (vars.dbt_virtual.base_environment, default prod).",
    )
    p_promote.add_argument(
        "--no-coverage-check",
        action="store_true",
        help="Skip the BigQuery partition coverage part of the audit.",
    )
    p_promote.add_argument(
        "--coverage-ignore-before",
        default=None,
        help="Ignore partitions older than this date (e.g. 2026-01-01) in the coverage audit "
        "(default: vars.dbt_virtual.coverage_ignore_before).",
    )
    p_promote.add_argument(
        "--skip-fingerprint-check",
        action="store_true",
        help="DANGEROUS: do not compare the release's fingerprints with the code compiled for --to. "
        "A windowed, --empty or other-branch build can then reach the target.",
    )
    p_promote.add_argument(
        "--compile-target",
        default=None,
        help="dbt --target used to compile the expected fingerprints (default: --to).",
    )
    p_promote.add_argument("--skip-compile", action="store_true", help="Reuse existing target/manifest.json.")
    p_promote.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_promote.add_argument(
        "--require-sealed",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also refuse releases created before sealing existed (default). Seal one after checking it "
        "with `dbt run-operation virtual_seal_release`, or pass --no-require-sealed.",
    )
    p_promote.add_argument(
        "--allow-unsealed",
        action="store_true",
        help="DANGEROUS: promote a release whose `build --release` did not finish successfully.",
    )
    p_promote.add_argument(
        "--max-row-drop-pct",
        type=float,
        default=100.0,
        help="Maximum allowed row drop percentage when --audit is enabled.",
    )
    p_promote.add_argument(
        "--no-lock",
        action="store_true",
        help="Do not take the environment lease lock in <physical_schema>._locks.",
    )
    p_promote.add_argument(
        "--lock-owner",
        default=None,
        help="Lease owner name (default: dbt-virtual-cli:<user>@<host>).",
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
    p_audit.add_argument(
        "--no-coverage-check", action="store_true", help="Skip the BigQuery partition coverage check."
    )
    p_audit.add_argument(
        "--coverage-ignore-before",
        default=None,
        help="Ignore partitions older than this date (e.g. 2026-01-01); default vars.dbt_virtual.coverage_ignore_before.",
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
    p_rollback.add_argument(
        "--no-lock",
        action="store_true",
        help="Do not take the environment lease lock in <physical_schema>._locks.",
    )
    p_rollback.add_argument(
        "--lock-owner",
        default=None,
        help="Lease owner name (default: dbt-virtual-cli:<user>@<host>).",
    )
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
    p_adopt.add_argument(
        "--hash-compiled-ancestors",
        action="store_true",
        help="Hash the compiled SQL of non-virtual ancestors too (default: raw SQL + macros, which do not depend on warehouse state).",
    )
    p_adopt.add_argument("--known-schema", dest="known_schemas", action="append", default=[])
    p_adopt.set_defaults(func=cmd_adopt)

    for subparser in sub.choices.values():
        if not any("--target" in action.option_strings for action in subparser._actions):
            subparser.add_argument(
                "--target",
                default=None,
                help="dbt profile target for the dbt calls (default: the profile's default target).",
            )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    ns = parser.parse_args(argv)
    global _DEFAULT_TARGET
    _DEFAULT_TARGET = getattr(ns, "target", None)
    return ns.func(ns)


if __name__ == "__main__":
    sys.exit(main())
