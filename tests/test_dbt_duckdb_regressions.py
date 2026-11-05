"""Regression tests for review findings, run through real dbt-core + dbt-duckdb.

Each test reproduces a bug that the mock harness could not catch, because the harness never
executes the SQL dbt would run or the order in which dbt runs it:

- incremental clone seeding was replaced by the materialization (dbt loads `existing_relation`
  before model pre-hooks run);
- a release from a partial build demoted (dropped) unchanged models on promote and rollback;
- a second default rollback went back to the same release;
- `virtual_adopt` executed `ensure_registry_tables_sql` one character at a time;
- `drop-env` / `init-env` for an unmapped env used `target.schema` (dropping the caller's schema);
- Jinja nested in `vars.dbt_virtual` (the documented `environment:` config) was never rendered.
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

REPO = Path(__file__).resolve().parent.parent

PROJECT_YML = """name: 'regress'
version: '1.0.0'
config-version: 2
profile: 'regress'
model-paths: ["models"]
macro-paths: ["macros"]
on-run-start:
  - "{{ dbt_virtual.seed_incremental_clones() }}"
vars:
  dbt_virtual:
    physical_schema: "analytics_physical"
    environment: "{{ var('_dbt_virtual_env', target.name) }}"
    base_environment: "prod"
    use_ctas_for_clone: true
    env_schemas:
      staging: "analytics_staging"
      prod: "analytics"
      shared_with_prod: "analytics"
models:
  regress:
    +virtual_env: true
    +pre-hook:
      - "{{ dbt_virtual.clone_if_new_fp() }}"
    +post-hook:
      - "{{ dbt_virtual.sync_env_view() }}"
"""

ROUTING_SHIM = """{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_schema_name(custom_schema_name, node)) }}
{%- endmacro %}

{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_alias_name(custom_alias_name, node)) }}
{%- endmacro %}

{% macro ref() -%}
  {{ return(dbt_virtual.virtual_ref(*varargs, **kwargs)) }}
{%- endmacro %}

{# The override docs/guides/getting_started.md recommends; it used to break `dbt parse`. #}
{% macro is_incremental() -%}
  {{ return(dbt_virtual.virtual_is_incremental()) }}
{%- endmacro %}
"""

TARGETS = {"staging": "analytics_staging", "prod": "analytics"}


def _table_model(sql: str) -> str:
    return "{{ config(materialized='table') }}\n" + sql + "\n"


class Project:
    """A throwaway dbt project wired to the local dbt_virtual macros and a DuckDB file."""

    def __init__(
        self, root: Path, models: dict[str, str], project_yml: str = PROJECT_YML, threads: int = 1
    ) -> None:
        self.dir = root / "proj"
        self.db_path = (root / "warehouse.duckdb").as_posix()
        (self.dir / "models").mkdir(parents=True)
        (self.dir / "macros").mkdir()
        # Install a copy without tests/fixtures, which dbt would otherwise parse.
        pkg_dir = root / "dbt_virtual_pkg"
        pkg_dir.mkdir(parents=True)
        shutil.copy2(REPO / "dbt_project.yml", pkg_dir / "dbt_project.yml")
        shutil.copytree(REPO / "macros", pkg_dir / "macros")
        (self.dir / "packages.yml").write_text(
            f'packages:\n  - local: "{pkg_dir.as_posix()}"\n', encoding="utf-8"
        )
        (self.dir / "dbt_project.yml").write_text(project_yml, encoding="utf-8")
        (self.dir / "macros" / "routing.sql").write_text(ROUTING_SHIM, encoding="utf-8")
        outputs = "".join(
            f'    {name}:\n      type: duckdb\n      path: "{self.db_path}"\n'
            f"      schema: {schema}\n      threads: {threads}\n"
            for name, schema in TARGETS.items()
        )
        (self.dir / "profiles.yml").write_text(
            f"regress:\n  target: staging\n  outputs:\n{outputs}", encoding="utf-8"
        )
        con = duckdb.connect(self.db_path)
        for schema in ("analytics_physical", *TARGETS.values()):
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        con.close()
        self.write_models(models)
        self.dbt("deps")

    def write_models(self, models: dict[str, str]) -> None:
        for name, sql in models.items():
            (self.dir / "models" / f"{name}.sql").write_text(sql, encoding="utf-8")

    def dbt(self, *args: str, expect_ok: bool = True) -> subprocess.CompletedProcess:
        cmd = [sys.executable, "-m", "dbt.cli.main", *args, "--profiles-dir", str(self.dir)]
        res = subprocess.run(cmd, cwd=self.dir, capture_output=True, text=True, check=False)
        if expect_ok and res.returncode != 0:
            raise AssertionError(f"{' '.join(args)} failed:\n{res.stdout}\n{res.stderr}")
        return res

    def fingerprints(self, env: str) -> dict[str, str]:
        self.dbt("compile", "--target", env)
        assert (
            dbt_virtual_main(
                ["--project-dir", str(self.dir), "plan", "--env", env, "--skip-compile"]
            )
            == 0
        )
        data = json.loads((self.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))
        return data["by_name"]

    def build(self, env: str, release: str, *select: str) -> dict[str, str]:
        """`dbt build` with the variables `dbt-virtual build` passes."""
        fps = self.fingerprints(env)
        dbt_vars = {"_dbt_virtual_fps": fps, "_dbt_virtual_env": env, "_dbt_virtual_release": release}
        args = ["build", "--target", env, "--vars", json.dumps(dbt_vars)]
        if select:
            args += ["--select", *select]
        self.dbt(*args)
        if release:
            # `dbt-virtual build --release` seals the release after a successful `dbt build`.
            self.operation("virtual_seal_release", {"release_id": release}, env=env)
        return fps

    def cli(self, *args: str) -> int:
        return dbt_virtual_main(["--project-dir", str(self.dir), *args])

    def operation(
        self, name: str, args: dict, *, expect_ok: bool = True, env: str = "staging"
    ) -> subprocess.CompletedProcess:
        return self.dbt("run-operation", name, "--target", env, "--args", json.dumps(args), expect_ok=expect_ok)

    def query(self, sql: str) -> list[tuple]:
        con = duckdb.connect(self.db_path, read_only=True)
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def relations(self, schema: str) -> list[tuple[str, str]]:
        return self.query(
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' ORDER BY 1"
        )

    def schemas(self) -> set[str]:
        return {r[0] for r in self.query("SELECT schema_name FROM information_schema.schemata")}


@pytest.fixture
def make_project(tmp_path: Path):
    def _make(models: dict[str, str]) -> Project:
        return Project(tmp_path, models)

    return _make


INCREMENTAL_V1 = """{{ config(materialized='incremental', unique_key='id') }}
select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(id, v)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""
# v2 changes the transformation (new fingerprint) and has one new source row.
INCREMENTAL_V2 = """{{ config(materialized='incremental', unique_key='id') }}
select id, upper(v) as v from (values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) as t(id, v)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""


def test_incremental_seed_clone_is_kept_by_the_materialization(make_project) -> None:
    proj = make_project({"events": INCREMENTAL_V1})
    fp_v1 = proj.build("prod", "r1")["events"]
    proj.write_models({"events": INCREMENTAL_V2})

    # `dbt compile` (used by `dbt-virtual plan`) must not clone anything.
    fp_v2 = proj.fingerprints("staging")["events"]
    physical = {name for name, _ in proj.relations("analytics_physical")}
    assert physical == {"_environments", "_intervals", "_locks", "_releases", f"events__{fp_v1}"}

    proj.build("staging", "r2")
    # Before the fix: "Table with name events__<fp> already exists" (DuckDB); on BigQuery the
    # clone was silently replaced by `create or replace table`.
    rows = proj.query("SELECT id, v FROM analytics_staging.events ORDER BY id")
    # History comes from the prod clone (lower case); only the new row ran through v2.
    assert rows == [(1, "a"), (2, "b"), (3, "c"), (4, "D")]
    assert {name for name, _ in proj.relations("analytics_physical")} == physical | {f"events__{fp_v2}"}


def test_partial_release_promote_and_rollback_keep_unchanged_models(make_project) -> None:
    proj = make_project({"a": _table_model("select 1 as a"), "b": _table_model("select 10 as b")})

    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    assert proj.relations("analytics") == [("a", "VIEW"), ("b", "VIEW")]

    for release, value in (("r2", 2), ("r3", 3)):
        proj.write_models({"a": _table_model(f"select {value} as a")})
        # Only `a` is built; `b` is not part of what dbt runs for this release.
        assert proj.cli("build", "--env", "staging", "--release", release, "--select", "a") == 0
        proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": release})
        # Before the fix, promote demoted (dropped) `b` from prod here.
        assert proj.relations("analytics") == [("a", "VIEW"), ("b", "VIEW")]
        assert proj.query("SELECT a FROM analytics.a") == [(value,)]

    released = proj.query("SELECT model_name FROM analytics_physical._releases WHERE release_id = 'r2' ORDER BY 1")
    assert released == [("a",), ("b",)]

    proj.operation("virtual_rollback", {"env": "prod"})
    # Rollback restores r2 and removes objects absent from r2: `b` must still be there.
    assert proj.relations("analytics") == [("a", "VIEW"), ("b", "VIEW")]
    assert proj.query("SELECT a FROM analytics.a") == [(2,)]

    proj.operation("virtual_rollback", {"env": "prod"})
    # Before the fix, the second default rollback returned to r2 again.
    assert proj.query("SELECT a FROM analytics.a") == [(1,)]
    statuses = dict(
        proj.query(
            "SELECT release_id, MAX(status) FROM analytics_physical._releases "
            "WHERE target_environment = 'prod' GROUP BY release_id"
        )
    )
    assert statuses == {"r1": "promoted", "r2": "rolled_back", "r3": "rolled_back"}


def test_virtual_adopt_converts_existing_table_to_view(make_project) -> None:
    proj = make_project({"a": _table_model("select 1 as a")})
    con = duckdb.connect(proj.db_path)
    con.execute("CREATE TABLE analytics.a AS SELECT 42 AS a")
    con.close()
    fp = proj.fingerprints("prod")["a"]

    # Before the fix: "Parser Error: syntax error at or near "C"".
    proj.dbt(
        "run-operation", "virtual_adopt", "--target", "prod",
        "--args", json.dumps({"env": "prod", "release_id": "adopt"}),
        "--vars", json.dumps({"_dbt_virtual_fps": {"a": fp}}),
    )
    assert proj.relations("analytics") == [("a", "VIEW")]
    assert proj.query("SELECT a FROM analytics.a") == [(42,)]
    assert proj.query(f"SELECT a FROM analytics_physical.a__{fp}") == [(42,)]
    pointers = proj.query(
        "SELECT environment, physical_identifier FROM analytics_physical._environments"
    )
    assert pointers == [("prod", f"analytics_physical.a__{fp}")]


def test_unmapped_env_gets_its_own_schema_and_drop_env_is_guarded(make_project) -> None:
    proj = make_project({"a": _table_model("select 1 as a")})
    proj.build("staging", "r1")
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})

    # `pr_7` is not in env_schemas: it must get `<base_env_schema>_pr_7`, not `target.schema`.
    proj.operation("virtual_init_env", {"env": "pr_7", "from_env": "prod"})
    assert proj.relations("analytics_pr_7") == [("a", "VIEW")]
    assert proj.relations("analytics_staging") == [("a", "VIEW")]

    proj.operation("virtual_drop_env", {"env": "pr_7"})
    assert "analytics_pr_7" not in proj.schemas()
    # Before the fix this dropped `analytics_staging`, the staging target's schema.
    proj.operation("virtual_drop_env", {"env": "pr_99"})
    assert proj.relations("analytics_staging") == [("a", "VIEW")]

    # An env whose schema is another environment's schema is refused.
    res = proj.operation("virtual_drop_env", {"env": "shared_with_prod"}, expect_ok=False)
    assert res.returncode != 0
    assert "Refusing to drop schema 'analytics'" in res.stdout
    assert proj.relations("analytics") == [("a", "VIEW")]


def test_bare_dbt_run_renders_nested_config_and_registers_physical_fingerprint(make_project) -> None:
    proj = make_project(
        {
            "up": _table_model("select 1 as x"),
            "down": _table_model("select x from {{ ref('up') }}"),
        }
    )
    # Before the fix: the environment literal `{{ var('_dbt_virtual_env', target.name) }}`
    # reached the registry SQL and the run failed with a parser error.
    proj.dbt("run", "--target", "staging")
    rows = proj.query(
        "SELECT environment, model_name, fingerprint, physical_identifier "
        "FROM analytics_physical._environments ORDER BY model_name"
    )
    assert [r[0] for r in rows] == ["staging", "staging"]
    for _env, name, fingerprint, physical_identifier in rows:
        # A bare run registers the run-time Merkle fingerprint (parents included), which
        # `guard_shared_tables` compares; the physical name is the parse-time alias.
        assert physical_identifier.startswith(f"analytics_physical.{name}__")
        assert len(fingerprint) == 12
    assert proj.query("SELECT x FROM analytics_staging.down") == [(1,)]
