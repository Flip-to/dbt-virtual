"""Two locations emulated as two physical schemas, run through real dbt + DuckDB.

DuckDB has no locations; the point is the bookkeeping: models with `+virtual_location: US` keep
their physical tables, registry and environment views apart from the default (EU) ones, and
promote, rollback and incremental seeding use the registry of the model's own location.
See docs/decisions/20261105_physical_location_per_region.md.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from tests.test_dbt_duckdb_regressions import PROJECT_YML, Project

MULTI_REGION_YML = PROJECT_YML.replace(
    """    env_schemas:
      staging: "analytics_staging"
      prod: "analytics"
      shared_with_prod: "analytics"
""",
    """    physical_schemas_by_location:
      EU: "analytics_physical"
      US: "analytics_physical_us"
    schema_locations:
      analytics_staging: EU
      analytics: EU
      analytics_staging_us: US
      analytics_us: US
    env_schemas:
      staging:
        default: "analytics_staging"
        us: "analytics_staging_us"
      prod:
        default: "analytics"
        us: "analytics_us"
""",
)
assert MULTI_REGION_YML != PROJECT_YML

EVENTS_US_V1 = """{{ config(materialized='incremental', unique_key='id', schema='us', virtual_location='US') }}
select * from (values (1, 'a'), (2, 'b'), (3, 'c')) as t(id, v)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""
EVENTS_US_V2 = """{{ config(materialized='incremental', unique_key='id', schema='us', virtual_location='US') }}
select id, upper(v) as v from (values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')) as t(id, v)
{% if is_incremental() %} where id > (select max(id) from {{ this }}) {% endif %}
"""

MODELS = {
    "orders_eu": "{{ config(materialized='table', virtual_location='EU') }}\nselect 1 as id, 10 as amount\n",
    "events_us": EVENTS_US_V1,
}


def _extra_schemas(proj: Project) -> None:
    con = duckdb.connect(proj.db_path)
    for schema in ("analytics_physical_us", "analytics_staging_us", "analytics_us", "analytics"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.close()


def test_each_location_has_its_own_physical_schema_registry_and_env_views(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=MULTI_REGION_YML)
    _extra_schemas(proj)
    fps = proj.build("staging", "r1")

    # Physical tables and registries are split by location.
    assert [r[0] for r in proj.relations("analytics_physical") if not r[0].startswith("_")] == [f"orders_eu__{fps['orders_eu']}"]
    assert [r[0] for r in proj.relations("analytics_physical_us") if not r[0].startswith("_")] == [f"events_us__{fps['events_us']}"]
    assert proj.query("SELECT model_name FROM analytics_physical._environments") == [("orders_eu",)]
    assert proj.query("SELECT model_name FROM analytics_physical_us._environments") == [("events_us",)]
    assert proj.query("SELECT model_name FROM analytics_physical_us._releases") == [("events_us",)]
    # Environment views sit in the dataset of their location.
    assert proj.relations("analytics_staging") == [("orders_eu", "VIEW")]
    assert proj.relations("analytics_staging_us") == [("events_us", "VIEW")]

    # Promote handles both locations in one call, from each registry.
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    assert proj.relations("analytics") == [("orders_eu", "VIEW")]
    assert proj.relations("analytics_us") == [("events_us", "VIEW")]
    assert proj.query("SELECT model_name, status FROM analytics_physical_us._releases") == [("events_us", "promoted")]
    assert proj.query("SELECT environment FROM analytics_physical_us._environments ORDER BY 1") == [("prod",), ("staging",)]
    assert proj.query("SELECT count(*) FROM analytics_us.events_us") == [(3,)]


def test_incremental_seed_and_rollback_use_the_registry_of_the_model_location(tmp_path: Path) -> None:
    proj = Project(tmp_path, MODELS, project_yml=MULTI_REGION_YML)
    _extra_schemas(proj)
    proj.build("prod", "r1")
    proj.write_models({"events_us": EVENTS_US_V2})
    fps = proj.build("staging", "r2")

    # The US table was seeded from prod's US table (rows 1-3 keep their old value), then topped up.
    seeded = proj.query(f"SELECT id, v FROM analytics_physical_us.events_us__{fps['events_us']} ORDER BY id")
    assert seeded == [(1, "a"), (2, "b"), (3, "c"), (4, "D")]

    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r2"})
    assert proj.query("SELECT count(*) FROM analytics_us.events_us") == [(4,)]
    # orders_eu did not change: only the US registry holds a second release.
    assert proj.query("SELECT count(DISTINCT release_id) FROM analytics_physical_us._releases") == [(2,)]
    proj.operation("virtual_rollback", {"env": "prod", "to_release": "r1"})
    assert proj.query("SELECT count(*) FROM analytics_us.events_us") == [(3,)]


def test_status_merges_the_state_of_every_location(tmp_path: Path, capsys) -> None:
    proj = Project(tmp_path, MODELS, project_yml=MULTI_REGION_YML)
    _extra_schemas(proj)
    proj.build("staging", "r1")
    out = proj.dbt("run-operation", "virtual_export_state", "--target", "staging", "--args", json.dumps({"env": "staging", "base_env": "prod"})).stdout
    line = next(ln for ln in out.splitlines() if "DBT_VIRTUAL_STATE_JSON:" in ln)
    state = json.loads(line.split("DBT_VIRTUAL_STATE_JSON:", 1)[1])
    assert sorted(state["env_state"]) == ["events_us", "orders_eu"]
    assert state["physical_schemas"] == ["analytics_physical", "analytics_physical_us"]


def test_model_location_that_disagrees_with_its_environment_dataset_fails_to_parse(tmp_path: Path) -> None:
    # A US model without `schema='us'` would put its view in the EU dataset `analytics_staging`.
    bad = {"events_us": EVENTS_US_V1.replace("schema='us', ", "")}
    proj = Project(tmp_path, bad, project_yml=MULTI_REGION_YML)
    res = proj.dbt("parse", "--target", "staging", expect_ok=False)
    assert res.returncode != 0
    assert "events_us" in res.stdout + res.stderr
    assert "is in EU" in res.stdout + res.stderr
