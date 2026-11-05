"""Environments that expose virtual objects in several schemas, run through real dbt + DuckDB.

Staging puts everything in one schema (like most non-production targets); prod splits objects
by custom schema: `default` -> `analytics`, `gold` -> `analytics_gold` (derived), and
`spacetime` -> `spacetime` (explicit). See docs/decisions/20261104_per_custom_schema_env_datasets.md.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project

MULTI_SCHEMA_YML = PROJECT_YML.replace(
    """    env_schemas:
      staging: "analytics_staging"
      prod: "analytics"
      shared_with_prod: "analytics"
""",
    """    env_schemas:
      staging: "analytics_staging"
      prod:
        default: "analytics"
        spacetime: "spacetime"
""",
)
assert MULTI_SCHEMA_YML != PROJECT_YML

MODELS = {
    "orders": "{{ config(materialized='table', schema='gold') }}\n"
    "select * from (values (1, 10), (2, 25)) as t(id, amount)\n",
    "daily": "{{ config(materialized='table') }}\nselect 7 as d\n",
    # dbt-duckdb ships its own `table_function` materialization, which wins over the package's
    # default one, so the routine under test is a scalar `virtual_function` (a DuckDB macro).
    "big_total": "{{ config(materialized='virtual_function', schema='spacetime', parameters='min_amount') }}\n"
    "(select sum(amount) from {{ ref('orders') }} where amount >= min_amount)\n",
}


def test_promote_places_objects_in_each_custom_schema(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=MULTI_SCHEMA_YML)
    proj.build("staging", "r1")

    # Staging: one schema for every custom schema.
    assert proj.relations("analytics_staging") == [("daily", "VIEW"), ("orders", "VIEW")]
    assert proj.query("SELECT analytics_staging.big_total(20)") == [(25,)]

    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})

    assert proj.relations("analytics") == [("daily", "VIEW")]
    assert proj.relations("analytics_gold") == [("orders", "VIEW")]
    # The function moved to its own schema and now reads prod's `analytics_gold.orders`,
    # not staging's `analytics_staging.orders` it was built against.
    assert proj.query("SELECT spacetime.big_total(20)") == [(25,)]
    definition = proj.query(
        "SELECT macro_definition FROM duckdb_functions() "
        "WHERE schema_name = 'spacetime' AND function_name = 'big_total'"
    )[0][0]
    assert "analytics_gold" in definition and "analytics_staging" not in definition

    keys = dict(
        proj.query(
            "SELECT model_name, schema_key FROM analytics_physical._environments "
            "WHERE environment = 'prod'"
        )
    )
    assert keys == {"orders": "gold", "daily": None, "big_total": "spacetime"}

    # Rollback and demotion use the recorded schema of each object.
    proj.write_models({"daily": "{{ config(materialized='table') }}\nselect 8 as d\n"})
    proj.build("staging", "r2")
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r2"})
    assert proj.query("SELECT d FROM analytics.daily") == [(8,)]
    proj.operation("virtual_rollback", {"env": "prod"})
    assert proj.query("SELECT d FROM analytics.daily") == [(7,)]
    assert proj.query("SELECT spacetime.big_total(0)") == [(35,)]


def test_registry_without_schema_key_is_migrated(tmp_path: Path) -> None:
    proj = Project(tmp_path, {"daily": MODELS["daily"]}, project_yml=MULTI_SCHEMA_YML)
    con = duckdb.connect(proj.db_path)
    # Registry layout before `schema_key` existed.
    con.execute(
        "CREATE TABLE analytics_physical._environments (environment VARCHAR, model_name VARCHAR, "
        "resource_type VARCHAR, fingerprint VARCHAR, physical_identifier VARCHAR, release_id VARCHAR, "
        "updated_at TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE analytics_physical._releases (release_id VARCHAR, source_environment VARCHAR, "
        "target_environment VARCHAR, model_name VARCHAR, resource_type VARCHAR, dependency_tier INTEGER, "
        "fingerprint VARCHAR, physical_identifier VARCHAR, routine_ddl_template VARCHAR, status VARCHAR, "
        "created_at TIMESTAMP, promoted_at TIMESTAMP, rolled_back_at TIMESTAMP)"
    )
    con.execute(
        "INSERT INTO analytics_physical._environments VALUES "
        "('prod', 'old_model', 'model', 'abc', 'analytics_physical.old_model__abc', 'r0', CURRENT_TIMESTAMP)"
    )
    con.close()

    proj.build("staging", "r1")
    rows = proj.query(
        "SELECT environment, model_name, schema_key FROM analytics_physical._environments ORDER BY 1, 2"
    )
    assert rows == [("prod", "old_model", None), ("staging", "daily", None)]
    columns = {r[0] for r in proj.query("DESCRIBE analytics_physical._releases")}
    assert "schema_key" in columns
