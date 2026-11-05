"""Multi-region (BigQuery location) support: one physical schema and registry per location.

See docs/decisions/20261105_physical_location_per_region.md. A BigQuery view cannot read a table
in another location and CLONE needs the same location, so a project with EU and US datasets needs
a physical schema and a registry per location.
"""

from __future__ import annotations

import json

import pytest

from tests.test_macros_execution import FakeRelation, FakeResult, MacroTestHarness

REGION_CONFIG = {
    "physical_schema": "rt_virtual_physical",
    "physical_schemas_by_location": {"EU": "rt_virtual_physical", "US": "rt_virtual_physical_us"},
    "base_environment": "prod",
    "env_schemas": {
        "staging": {"default": "rt_staging", "us": "rt_staging_us"},
        "prod": {"default": "rt", "us": "rt_us"},
    },
}


def _harness(config: dict | None = None, **kwargs) -> MacroTestHarness:
    return MacroTestHarness(vars_dict={"dbt_virtual": config or REGION_CONFIG}, **kwargs)


def _table_node(name: str, location: str | None, schema: str | None = None, mat: str = "table") -> dict:
    cfg: dict = {"materialized": mat, "virtual_env": True}
    if location is not None:
        cfg["virtual_location"] = location
    if schema is not None:
        cfg["schema"] = schema
    return {"name": name, "config": cfg}


def test_virtual_table_routes_to_the_physical_schema_of_its_location() -> None:
    harness = _harness(target_location="EU")
    assert harness.call_macro("virtual_schema_name", None, _table_node("eu_default", None)) == "rt_virtual_physical"
    assert harness.call_macro("virtual_schema_name", "us", _table_node("us_model", "US", "us")) == "rt_virtual_physical_us"
    # Case-insensitive, and `target.location` decides when the model sets none.
    assert harness.call_macro("virtual_schema_name", "us", _table_node("us_model", "us", "us")) == "rt_virtual_physical_us"
    us_target = _harness(target_location="US")
    assert us_target.call_macro("virtual_schema_name", None, _table_node("m", None)) == "rt_virtual_physical_us"


def test_location_in_meta_is_used_too() -> None:
    harness = _harness(target_location="EU")
    node = {"name": "m", "config": {"materialized": "table", "virtual_env": True, "meta": {"virtual_location": "US"}}}
    assert harness.call_macro("virtual_schema_name", "us", node) == "rt_virtual_physical_us"


def test_physical_table_ref_resolves_to_env_view_of_any_location_schema() -> None:
    harness = _harness(target_location="EU")
    harness.graph["nodes"]["model.p.us_model"] = _table_node("us_model", "US", "us")
    harness.ref_relations["us_model"] = FakeRelation("test_proj", "rt_virtual_physical_us", "us_model__abcdef123456", "table")
    rel = harness.call_macro("virtual_ref", "us_model")
    assert (rel.schema, rel.identifier, rel.type) == ("rt_staging_us", "us_model", "view")


def test_location_without_a_physical_schema_is_an_error_unless_a_default_is_set() -> None:
    no_default = {k: v for k, v in REGION_CONFIG.items() if k != "physical_schema"}
    harness = _harness(no_default, target_location="EU")
    with pytest.raises(RuntimeError, match="asia-east1"):
        harness.call_macro("virtual_schema_name", None, _table_node("m", "asia-east1"))
    # No location at all and no default: say what to set.
    with pytest.raises(RuntimeError, match="has no location"):
        MacroTestHarness(vars_dict={"dbt_virtual": no_default}).call_macro("virtual_schema_name", None, _table_node("m", None))
    # With a default `physical_schema`, unlisted locations use it.
    with_default = _harness(target_location="EU")
    assert with_default.call_macro("virtual_schema_name", None, _table_node("m", "asia-east1")) == "rt_virtual_physical"


def test_registry_and_dataset_are_created_in_the_location_of_the_model() -> None:
    harness = _harness({**REGION_CONFIG, "env_schemas": {"staging": {"default": "rt_staging", "us": "rt_staging_us"}}}, target_location="EU")
    harness.model = _table_node("us_model", "US", "us", mat="incremental")
    harness.this = FakeRelation("test_proj", "rt_virtual_physical_us", "us_model__fp1111222233")
    harness.adapter_extras = {"check_schema_exists": lambda database, schema: False, "create_schema": lambda relation: None}
    sync_sql = harness.call_macro("sync_env_view")
    assert "MERGE `test_proj`.`rt_virtual_physical_us`._environments" in sync_sql
    assert "`test_proj`.`rt_virtual_physical`._environments" not in sync_sql
    assert "CREATE OR REPLACE VIEW `test_proj`.`rt_staging_us`.`us_model`" in sync_sql
    # The registry of the US location (not the EU one) is created, as a US dataset.
    assert any(
        "CREATE SCHEMA IF NOT EXISTS `test_proj`.`rt_virtual_physical_us` OPTIONS (location = 'US')" in q
        for q in harness.executed_queries
    )
    assert any(
        "CREATE SCHEMA IF NOT EXISTS `test_proj`.`rt_staging_us` OPTIONS (location = 'US')" in q
        for q in harness.executed_queries
    )


def test_create_schema_sql_carries_the_location_on_bigquery_only() -> None:
    bq = _harness()
    assert bq.call_macro("create_schema_if_not_exists_sql", "rt_us", "US").strip() == (
        "CREATE SCHEMA IF NOT EXISTS `test_proj`.`rt_us` OPTIONS (location = 'US')"
    )
    assert bq.call_macro("create_schema_if_not_exists_sql", "rt_us").strip() == "CREATE SCHEMA IF NOT EXISTS `test_proj`.`rt_us`"
    pg = _harness(adapter_type="postgres")
    assert pg.call_macro("create_schema_if_not_exists_sql", "rt_us", "US").strip() == "CREATE SCHEMA IF NOT EXISTS rt_us"


def test_promote_runs_once_per_location_and_skips_a_location_without_the_release() -> None:
    harness = _harness()
    harness.existing_relations.add(("test_proj", "rt_virtual_physical_us", "us_model__fpus"))
    # EU registry: the release is not there. US registry: it is.
    harness.query_responses.append(FakeResult([]))
    harness.query_responses.append(
        FakeResult([("us_model", "model", "fpus", "rt_virtual_physical_us.us_model__fpus", None, 2, "us")])
    )
    harness.query_responses.append(FakeResult([]))
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1")
    joined = "\n".join(harness.executed_queries)
    assert "FROM `test_proj`.`rt_virtual_physical`._releases" in joined
    assert "FROM `test_proj`.`rt_virtual_physical_us`._releases" in joined
    assert "CREATE OR REPLACE VIEW `test_proj`.`rt_us`.`us_model`" in joined
    assert "SELECT * FROM `test_proj`.`rt_virtual_physical_us`.`us_model__fpus`" in joined
    assert "UPDATE `test_proj`.`rt_virtual_physical_us`._releases SET status = 'promoted'" in joined
    assert "UPDATE `test_proj`.`rt_virtual_physical`._releases SET status = 'promoted'" not in joined


def test_promote_fails_when_no_location_has_the_release() -> None:
    harness = _harness()
    with pytest.raises(RuntimeError, match="nothing found in any location"):
        harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="nope")


def test_promote_with_an_explicit_location_touches_only_that_registry() -> None:
    harness = _harness()
    harness.existing_relations.add(("test_proj", "rt_virtual_physical", "eu_model__fpeu"))
    harness.query_responses.append(FakeResult([("eu_model", "model", "fpeu", "rt_virtual_physical.eu_model__fpeu", None, 2, None)]))
    harness.query_responses.append(FakeResult([]))
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1", location="EU")
    joined = "\n".join(harness.executed_queries)
    assert "rt_virtual_physical_us" not in joined
    assert "CREATE OR REPLACE VIEW `test_proj`.`rt`.`eu_model`" in joined


def test_rollback_gc_and_locks_iterate_locations() -> None:
    harness = _harness()
    harness.call_macro("virtual_gc", dry_run=True)
    gc_sql = "\n".join(harness.executed_queries)
    assert "rt_virtual_physical_us" in gc_sql and "`rt_virtual_physical`" in gc_sql

    locked = _harness()
    locked.call_macro("virtual_acquire_lock", env="prod", owner="me")
    lock_sql = "\n".join(locked.executed_queries)
    assert "`test_proj`.`rt_virtual_physical`._locks" in lock_sql
    assert "`test_proj`.`rt_virtual_physical_us`._locks" in lock_sql

    rolled = _harness()
    with pytest.raises(RuntimeError, match="nothing found in any location"):
        rolled.call_macro("virtual_rollback", env="prod", to_release="r0")
    rolled_sql = "\n".join(rolled.executed_queries)
    assert "`rt_virtual_physical`._releases" in rolled_sql and "`rt_virtual_physical_us`._releases" in rolled_sql


def test_export_state_merges_every_location() -> None:
    harness = _harness()
    for schema in ("rt_virtual_physical", "rt_virtual_physical_us"):
        for registry in ("_environments", "_releases"):
            harness.existing_relations.add((harness.target["database"], schema, registry))
    # Per location: environment rows, physical tables, registered tables, build quality.
    harness.query_responses.extend(
        [
            FakeResult([("staging", "eu_model", "fpeu"), ("prod", "eu_model", "fpeu0")]),
            FakeResult([("eu_model__fpeu",)]),
            FakeResult([("rt_virtual_physical.eu_model__fpeu",)]),
            FakeResult([]),  # build_quality rows (promotion guards)
            FakeResult([("staging", "us_model", "fpus")]),
            FakeResult([("us_model__fpus",)]),
            FakeResult([("rt_virtual_physical_us.us_model__fpus",)]),
            FakeResult([]),
        ]
    )
    harness.call_macro("virtual_export_state", env="staging", base_env="prod")
    line = next(m for m in harness.logs if m.startswith("DBT_VIRTUAL_STATE_JSON:"))
    state = json.loads(line.split(":", 1)[1])
    assert state["env_state"] == {"eu_model": "fpeu", "us_model": "fpus"}
    assert state["base_env_state"] == {"eu_model": "fpeu0"}
    assert state["existing_physical_tables"] == ["eu_model__fpeu", "us_model__fpus"]
    assert state["registered_physical_tables"] == ["eu_model__fpeu", "us_model__fpus"]
    assert state["physical_schemas"] == ["rt_virtual_physical", "rt_virtual_physical_us"]


def test_mismatched_environment_dataset_location_is_a_compile_error() -> None:
    config = {**REGION_CONFIG, "schema_locations": {"rt_staging": "EU", "rt": "EU", "rt_staging_us": "US", "rt_us": "US"}}
    harness = _harness(config, target_location="EU")
    # A US model with no custom schema lands in the EU default environment datasets.
    with pytest.raises(RuntimeError) as excinfo:
        harness.call_macro("virtual_schema_name", None, _table_node("us_model", "US"))
    message = str(excinfo.value)
    assert "us_model" in message and "US" in message and "rt_staging" in message and "EU" in message
    # Mapped to its US datasets it is fine.
    assert harness.call_macro("virtual_schema_name", "us", _table_node("us_model", "US", "us")) == "rt_virtual_physical_us"


def test_physical_schema_declared_in_another_location_is_a_compile_error() -> None:
    config = {**REGION_CONFIG, "schema_locations": {"rt_virtual_physical": "EU", "rt_virtual_physical_us": "EU"}}
    harness = _harness(config, target_location="EU")
    with pytest.raises(RuntimeError, match="physical schema 'rt_virtual_physical_us' is in EU"):
        harness.call_macro("virtual_schema_name", "us", _table_node("us_model", "US", "us"))


def test_routine_env_dataset_must_match_its_location() -> None:
    config = {**REGION_CONFIG, "schema_locations": {"rt_staging": "EU", "rt": "EU", "rt_staging_us": "US", "rt_us": "US"}}
    harness = _harness(config, target_location="EU")
    node = _table_node("tvf_us", "US", mat="table_function")
    with pytest.raises(RuntimeError, match="tvf_us"):
        harness.call_macro("virtual_schema_name", None, node)
    ok = _table_node("tvf_us", "US", "us", mat="table_function")
    assert harness.call_macro("virtual_schema_name", "us", ok) == "rt_staging_us"


def test_unmapped_environment_keeps_one_schema_per_base_schema_when_locations_are_declared() -> None:
    config = {**REGION_CONFIG, "schema_locations": {"rt": "EU", "rt_us": "US"}}
    harness = _harness(config)
    assert harness.call_macro("env_schema_for", "pr_7", None) == "rt_pr_7"
    assert harness.call_macro("env_schema_for", "pr_7", "us") == "rt_us_pr_7"
    # Without `schema_locations` the single-schema rule of the per-custom-schema decision holds.
    plain = _harness()
    assert plain.call_macro("env_schema_for", "pr_7", "us") == "rt_pr_7"


def test_single_location_projects_are_unchanged() -> None:
    harness = MacroTestHarness(
        vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical", "env_schemas": {"staging": "analytics_staging"}}},
        target_location="EU",
    )
    assert harness.call_macro("physical_locations") == [None]
    assert harness.call_macro("virtual_schema_name", None, _table_node("m", "US")) == "analytics_physical"
    assert harness.call_macro("create_location_for", "EU") is None
