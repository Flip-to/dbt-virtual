"""Upgrade v0.1.0 -> current: a registry written by the v0.1.0 package, driven by the current one.

Real dbt-core + dbt-duckdb. The project is built and promoted with the v0.1.0 package and CLI
(exported from the git tag), then `packages.yml` is switched to this checkout and the current CLI
runs `status`, `plan`, `build`, `promote` and `rollback` against the same warehouse file.

What a failure here means: an existing deployment would break (or silently lose a release) the
day it upgrades the package. The v0.1.0 registry has no `schema_key`, `build_quality` or
`sealed_at` columns, and its releases were never sealed.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from dbt_virtual.cli import main as dbt_virtual_main
from tests.legacy_release import REPO, export_release, run_release_cli, tag_available

pytestmark = pytest.mark.skipif(
    not tag_available("v0.1.0"), reason="git tag v0.1.0 not available (clone with `git fetch --tags`)"
)

PROJECT_YML = """name: 'upg'
version: '1.0.0'
config-version: 2
profile: 'upg'
model-paths: ["models"]
macro-paths: ["macros"]
vars:
  dbt_virtual:
    enabled: true
    use_ctas_for_clone: true
    physical_schema: "analytics_physical"
    environment: "{{ var('_dbt_virtual_env', target.name) }}"
    base_environment: "prod"
    ref_resolution: "env_view"
    env_schemas:
      staging: "analytics_staging"
      prod: "analytics"
models:
  upg:
    +virtual_env: true
    +pre-hook:
      - "{{ dbt_virtual.clone_if_new_fp() }}"
    +post-hook:
      - "{{ dbt_virtual.sync_env_view() }}"
"""

# The routing shim of the v0.1.0 sample project (the shim a v0.1.0 user has).
SHIM = """{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {{ return(target.schema) }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
{%- endmacro %}

{% macro ref() -%}
  {{ return(dbt_virtual.virtual_ref(*varargs, **kwargs)) }}
{%- endmacro %}
"""


def _table(value: str) -> str:
    return "{{ config(materialized='table') }}\nselect " + value + "\n"


class UpgradeProject:
    def __init__(self, root: Path, create: bool = True) -> None:
        self.root = root
        self.dir = root / "proj"
        self.db = (root / "warehouse.duckdb").as_posix()
        if not create:
            self._rewrite_profiles()
            return
        (self.dir / "models").mkdir(parents=True)
        (self.dir / "macros").mkdir()
        (self.dir / "dbt_project.yml").write_text(PROJECT_YML, encoding="utf-8")
        (self.dir / "macros" / "routing.sql").write_text(SHIM, encoding="utf-8")
        self._rewrite_profiles()
        con = duckdb.connect(self.db)
        for schema in ("analytics_physical", "analytics_staging", "analytics"):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.close()

    def _rewrite_profiles(self) -> None:
        outputs = "".join(
            f'    {name}:\n      type: duckdb\n      path: "{self.db}"\n'
            f"      schema: {schema}\n      threads: 1\n"
            for name, schema in (("staging", "analytics_staging"), ("prod", "analytics"))
        )
        (self.dir / "profiles.yml").write_text(
            f"upg:\n  target: staging\n  outputs:\n{outputs}", encoding="utf-8"
        )

    def install(self, package_dir: Path) -> None:
        """Point `packages.yml` at `package_dir` (a package with dbt_project.yml + macros/)."""
        (self.dir / "packages.yml").write_text(
            f'packages:\n  - local: "{package_dir.as_posix()}"\n', encoding="utf-8"
        )
        shutil.rmtree(self.dir / "dbt_packages", ignore_errors=True)
        self.dbt("deps")

    def models(self, models: dict[str, str]) -> None:
        for name, sql in models.items():
            (self.dir / "models" / f"{name}.sql").write_text(sql, encoding="utf-8")

    def dbt(self, *args: str, expect_ok: bool = True) -> subprocess.CompletedProcess:
        cmd = [sys.executable, "-m", "dbt.cli.main", *args, "--profiles-dir", str(self.dir)]
        res = subprocess.run(cmd, cwd=self.dir, capture_output=True, text=True, check=False)
        if expect_ok and res.returncode != 0:
            raise AssertionError(f"dbt {' '.join(args)} failed:\n{res.stdout}\n{res.stderr}")
        return res

    def operation(self, name: str, args: dict, *, expect_ok: bool = True) -> subprocess.CompletedProcess:
        return self.dbt(
            "run-operation", name, "--target", "prod", "--args", json.dumps(args), expect_ok=expect_ok
        )

    def query(self, sql: str) -> list[tuple]:
        con = duckdb.connect(self.db, read_only=True)
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def columns(self, table: str) -> list[str]:
        return [
            r[0]
            for r in self.query(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = 'analytics_physical' AND table_name = '{table}' "
                "ORDER BY ordinal_position"
            )
        ]


@pytest.fixture(scope="module")
def v010(tmp_path_factory) -> Path:
    return export_release("v0.1.0", tmp_path_factory.mktemp("v010"))


@pytest.fixture(scope="module")
def legacy_state(tmp_path_factory, v010: Path) -> Path:
    """Releases v1 and v2 built and promoted with the v0.1.0 package and CLI (built once, copied per test)."""
    root = tmp_path_factory.mktemp("legacy")
    proj = UpgradeProject(root)
    proj.models({"orders": _table("1 as n"), "daily": _table("100 as d")})
    proj.install(v010)

    def old(*args: str) -> None:
        res = run_release_cli(v010, proj.dir, *args)
        assert res.returncode == 0, f"v0.1.0 CLI {args} failed:\n{res.stdout}\n{res.stderr}"

    old("build", "--env", "staging", "--release", "v1")
    old("promote", "--from", "staging", "--to", "prod", "--release", "v1")
    proj.models({"orders": _table("2 as n")})
    old("build", "--env", "staging", "--release", "v2")
    old("promote", "--from", "staging", "--to", "prod", "--release", "v2")
    assert proj.query("SELECT n FROM analytics.orders") == [(2,)]
    return root


@pytest.fixture
def upgraded(tmp_path: Path, legacy_state: Path):
    """A copy of the v0.1.0 deployment with the project switched to this checkout's package."""
    root = tmp_path / "copy"
    shutil.copytree(legacy_state, root, ignore=shutil.ignore_patterns("dbt_packages", "target", "logs"))
    proj = UpgradeProject(root, create=False)
    pkg = tmp_path / "current_pkg"
    pkg.mkdir()
    shutil.copy2(REPO / "dbt_project.yml", pkg / "dbt_project.yml")
    shutil.copytree(REPO / "macros", pkg / "macros")
    proj.install(pkg)
    return proj


def _prod_fingerprints(proj: UpgradeProject, env: str = "prod") -> dict[str, str]:
    return {
        r[0]: r[1]
        for r in proj.query(
            "SELECT model_name, fingerprint FROM analytics_physical._environments "
            f"WHERE environment = '{env}'"
        )
    }


def cli(proj: UpgradeProject, *args: str) -> int:
    return dbt_virtual_main(["--project-dir", str(proj.dir), *args])


def test_status_reads_a_v010_registry_and_migrates_it(upgraded: UpgradeProject) -> None:
    # Premise of every test here: if v0.1.0 ever wrote the new columns, they would prove nothing
    # about migrations.
    assert "sealed_at" not in upgraded.columns("_releases")
    assert "schema_key" not in upgraded.columns("_releases")
    assert "build_quality" not in upgraded.columns("_environments")
    # The first command after an upgrade is often `status` (and `build` queries the same state);
    # it failed with "Referenced column build_quality not found". It must read the old registry.
    res = upgraded.operation("virtual_export_state", {"env": "prod", "base_env": "prod"})
    assert "orders" in res.stdout and "daily" in res.stdout, res.stdout
    assert {"schema_key", "build_quality", "sealed_at"} <= set(upgraded.columns("_releases"))


def test_upgrade_migrates_registry_and_every_lifecycle_command_works(upgraded: UpgradeProject) -> None:
    old_fps = _prod_fingerprints(upgraded)

    # build v3 (only `daily` changed) with the current package: the first write migrates the registry.
    # Fingerprints differ from v0.1.0's (see the pinning test below), so both models are rebuilt.
    upgraded.models({"daily": _table("200 as d")})
    assert cli(upgraded, "build", "--env", "staging", "--release", "v3") == 0
    new_fps = _prod_fingerprints(upgraded, env="staging")
    assert all(new_fps[m] != old_fps[m] for m in old_fps), (old_fps, new_fps)
    cols = upgraded.columns("_releases")
    assert {"schema_key", "build_quality", "sealed_at"} <= set(cols), cols
    assert {"schema_key", "build_quality"} <= set(upgraded.columns("_environments"))

    # Old releases stay readable: the rows v0.1.0 wrote survive, with NULL in the new columns.
    legacy = upgraded.query(
        "SELECT release_id, model_name, sealed_at, build_quality FROM analytics_physical._releases "
        "WHERE release_id IN ('v1', 'v2') ORDER BY release_id, model_name"
    )
    assert [(r[0], r[1]) for r in legacy] == [
        ("v1", "daily"),
        ("v1", "orders"),
        ("v2", "daily"),
        ("v2", "orders"),
    ]
    assert all(r[2] is None and r[3] is None for r in legacy)

    # promote the new release (sealed by `build --release`).
    assert cli(upgraded, "promote", "--from", "staging", "--to", "prod", "--release", "v3") == 0
    assert upgraded.query("SELECT d FROM analytics.daily") == [(200,)]
    assert upgraded.query("SELECT n FROM analytics.orders") == [(2,)]

    # rollback to a release written by v0.1.0 (never sealed, no schema_key): prod returns to v1.
    assert cli(upgraded, "rollback", "--env", "prod", "--to-release", "v1") == 0
    assert upgraded.query("SELECT n FROM analytics.orders") == [(1,)]
    assert upgraded.query("SELECT d FROM analytics.daily") == [(100,)]
    statuses = dict(
        upgraded.query(
            "SELECT release_id, MAX(status) FROM analytics_physical._releases "
            "WHERE target_environment = 'prod' GROUP BY release_id"
        )
    )
    assert statuses["v3"] == "rolled_back"

    # status reads the migrated registry. The CLI's `promote` passes the fingerprints of the current
    # code, which differ from the ones v0.1.0 recorded, so it refuses to re-promote a v0.1.0 release
    # (rollback, which only repoints, is the way back to one) and changes nothing.
    assert cli(upgraded, "status", "--env", "prod") == 0
    assert cli(upgraded, "promote", "--from", "staging", "--to", "prod", "--release", "v2") != 0
    assert upgraded.query("SELECT n FROM analytics.orders") == [(1,)]


def test_unsealed_v010_release_is_refused_only_when_sealing_is_required(
    upgraded: UpgradeProject,
) -> None:
    # `require_sealed` is the opt-in for teams that want every release sealed; a release written
    # before sealing existed must get the actionable message, not a crash on a missing column.
    upgraded.models({"daily": _table("200 as d")})
    assert cli(upgraded, "build", "--env", "staging", "--release", "v3") == 0  # migrates the registry
    args = {"from_env": "staging", "to_env": "prod", "release_id": "v2", "require_sealed": True}
    res = upgraded.operation("virtual_promote", args, expect_ok=False)
    assert res.returncode != 0
    assert "has no seal" in res.stdout, res.stdout
    assert upgraded.query("SELECT n FROM analytics.orders") == [(2,)]
    upgraded.operation("virtual_seal_release", {"release_id": "v2"})
    upgraded.operation("virtual_promote", args)


def test_pinning_physical_version_to_the_v010_fingerprint_reuses_the_old_tables(
    upgraded: UpgradeProject,
) -> None:
    # Fingerprints are computed differently since v0.1.0 (they no longer depend on the target's
    # datasets), so an unpinned model gets a new physical table on the first build after the
    # upgrade. `physical_version` is the way to keep the v0.1.0 table: the upgrade then costs no
    # compute. Catches: a pin that is ignored, or a registry that cannot reference old tables.
    old_fps = _prod_fingerprints(upgraded)
    physical_before = {r[0] for r in upgraded.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics_physical'"
    )}
    upgraded.models(
        {
            name: _table(body).replace("materialized='table'", f"materialized='table', physical_version='{old_fps[name]}'")
            for name, body in (("orders", "2 as n"), ("daily", "100 as d"))
        }
    )
    assert cli(upgraded, "build", "--env", "staging", "--release", "v3") == 0
    physical_after = {r[0] for r in upgraded.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics_physical'"
    )}
    assert physical_after == physical_before, physical_after - physical_before
    assert upgraded.query("SELECT n FROM analytics_staging.orders") == [(2,)]
    assert _prod_fingerprints(upgraded, env="staging") == old_fps
    assert cli(upgraded, "promote", "--from", "staging", "--to", "prod", "--release", "v3") == 0
    assert upgraded.query("SELECT d FROM analytics.daily") == [(100,)]


def test_rollback_is_safe_as_the_first_command_after_the_upgrade(upgraded: UpgradeProject) -> None:
    # An incident right after upgrading: `rollback` reads `schema_key` from the registry before any
    # command has migrated it. Run without the lock (the CLI's default lock migrates the registry
    # through `ensure_registry` first); fails with "Referenced column ... not found" without
    # `migrate_registry_if_old` in `virtual_rollback`, and the migration must be committed.
    assert "schema_key" not in upgraded.columns("_releases")
    upgraded.operation("virtual_rollback", {"env": "prod", "to_release": "v1"})
    assert upgraded.query("SELECT n FROM analytics.orders") == [(1,)]
    assert "schema_key" in upgraded.columns("_releases")
