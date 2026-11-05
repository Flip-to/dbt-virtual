"""`dbt parse` with the package installed, for the warehouses CI cannot run end to end.

dbt resolves `adapter.dispatch` while parsing, so a dispatched macro that lacks an implementation
for an adapter (and a `default__`) breaks every dbt command on that adapter, even though the
DuckDB-based tests never see it (commit 0f42ee9: `orphan_table_age_predicate_sql` had no
`default__`, found only by a real BigQuery run). Parsing needs no warehouse connection, so these
tests use fake credentials.

The tests run `dbt parse` through `uv run --no-project --with dbt-<adapter>` in an isolated
environment, and are skipped when `uv` is missing or the adapter cannot be installed (offline).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

PROJECT_YML = """name: 'parse_check'
version: '1.0.0'
config-version: 2
profile: 'parse_check'
model-paths: ["models"]
macro-paths: ["macros"]
on-run-start:
  - "{{ dbt_virtual.seed_incremental_clones() }}"
vars:
  dbt_virtual:
    physical_schema: "analytics_physical"
    environment: "{{ var('_dbt_virtual_env', target.name) }}"
    env_schemas:
      dev: "analytics_dev"
models:
  parse_check:
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

{% macro is_incremental() -%}
  {{ return(dbt_virtual.virtual_is_incremental()) }}
{%- endmacro %}
"""

MODELS = {
    "orders": "{{ config(materialized='table') }}\nselect 1 as id\n",
    "events": (
        "{{ config(materialized='incremental', unique_key='id') }}\n"
        "select id from {{ ref('orders') }}\n"
        "{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}\n"
    ),
    "orders_view": "{{ config(materialized='view') }}\nselect id from {{ ref('orders') }}\n",
}

# Fake credentials: `dbt parse` never opens a connection.
PROFILES = {
    "bigquery": """parse_check:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: fake-project
      dataset: analytics_dev
      threads: 1
""",
    "snowflake": """parse_check:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: fake_account
      user: fake_user
      password: fake_password
      role: fake_role
      database: FAKE_DB
      warehouse: FAKE_WH
      schema: analytics_dev
      threads: 1
""",
}


def _parse(root: Path, adapter: str, macros_dir: Path) -> subprocess.CompletedProcess:
    proj = root / "proj"
    (proj / "models").mkdir(parents=True)
    (proj / "macros").mkdir()
    pkg_dir = root / "dbt_virtual_pkg"
    pkg_dir.mkdir()
    shutil.copy2(REPO / "dbt_project.yml", pkg_dir / "dbt_project.yml")
    shutil.copytree(macros_dir, pkg_dir / "macros")
    (proj / "packages.yml").write_text(
        f'packages:\n  - local: "{pkg_dir.as_posix()}"\n', encoding="utf-8"
    )
    (proj / "dbt_project.yml").write_text(PROJECT_YML, encoding="utf-8")
    (proj / "macros" / "routing.sql").write_text(ROUTING_SHIM, encoding="utf-8")
    (proj / "profiles.yml").write_text(PROFILES[adapter], encoding="utf-8")
    for name, sql in MODELS.items():
        (proj / "models" / f"{name}.sql").write_text(sql, encoding="utf-8")

    base = [
        "uv", "run", "--no-project", "--quiet",
        "--with", "dbt-core>=1.8", "--with", f"dbt-{adapter}",
        "dbt",
    ]
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "False"}
    for args in (["deps"], ["parse"]):
        res = subprocess.run(
            [*base, *args, "--profiles-dir", str(proj)],
            cwd=proj, capture_output=True, text=True, check=False, env=env, timeout=900,
        )
        if res.returncode != 0:
            return res
    return res


def _installable(adapter: str, tmp_path: Path) -> bool:
    """Whether `dbt-<adapter>` can run here. CI sets DBT_VIRTUAL_REQUIRE_PARSE_ADAPTERS=1 so a
    failed install fails the job instead of skipping the test."""
    if os.environ.get("DBT_VIRTUAL_REQUIRE_PARSE_ADAPTERS"):
        return True
    if shutil.which("uv") is None:
        return False
    res = subprocess.run(
        ["uv", "run", "--no-project", "--quiet", "--with", f"dbt-{adapter}", "dbt", "--version"],
        cwd=tmp_path, capture_output=True, text=True, check=False, timeout=900,
    )
    return res.returncode == 0


@pytest.mark.parametrize("adapter", ["bigquery", "snowflake"])
def test_package_parses_on_adapter_without_a_connection(adapter: str, tmp_path: Path) -> None:
    if not _installable(adapter, tmp_path):
        pytest.skip(f"uv or dbt-{adapter} is not installable here")
    res = _parse(tmp_path, adapter, REPO / "macros")
    assert res.returncode == 0, f"dbt parse failed on {adapter}:\n{res.stdout}\n{res.stderr}"


def test_parse_check_detects_a_dispatched_macro_without_default(tmp_path: Path) -> None:
    """The check is a real gate: removing a `default__` implementation makes parse fail."""
    adapter = "snowflake"
    if not _installable(adapter, tmp_path):
        pytest.skip(f"uv or dbt-{adapter} is not installable here")
    broken = tmp_path / "broken_macros"
    shutil.copytree(REPO / "macros", broken)
    removed = False
    for path in broken.rglob("*.sql"):
        text = path.read_text(encoding="utf-8")
        if "default__orphan_table_age_predicate_sql" in text:
            text = re.sub(
                r"\{%-?\s*macro default__orphan_table_age_predicate_sql.*?\{%-?\s*endmacro\s*-?%\}",
                "",
                text,
                flags=re.DOTALL,
            )
            path.write_text(text, encoding="utf-8")
            removed = True
    assert removed, "macro to remove not found: update this test"
    assert not any(
        "default__orphan_table_age_predicate_sql" in p.read_text(encoding="utf-8")
        for p in broken.rglob("*.sql")
    ), "the macro was not removed: update the pattern"
    res = _parse(tmp_path / "run", adapter, broken)
    assert res.returncode != 0
    assert "orphan_table_age_predicate_sql" in res.stdout + res.stderr
