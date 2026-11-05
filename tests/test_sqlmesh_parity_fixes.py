"""Fixes for gaps found by comparing dbt-virtual with SQLMesh (2026-11-05 review).

- A table that exists but was never registered (a clone whose build failed) is not reused.
- `forward_only` / `on_schema_change` are metadata: changing them does not rebuild.
- Promotion deploys routines after the routines they read (DAG depth), not alphabetically.
- Rollback refuses a release whose physical tables are gone (GC) before touching any view.
"""

from __future__ import annotations

from dbt_virtual.fingerprint import compute_manifest_fingerprints, compute_node_fingerprint
from dbt_virtual.planner import classify_plan_actions
from tests.test_macros_execution import FakeResult, MacroTestHarness


def _node(name: str, sql: str, **config: object) -> dict:
    return {
        "unique_id": f"model.p.{name}",
        "name": name,
        "alias": name,
        "resource_type": "model",
        "schema": "analytics_physical",
        "compiled_code": sql,
        "raw_code": sql,
        "config": {"materialized": "incremental", "virtual_env": True, **config},
        "depends_on": {"nodes": []},
    }


def test_unregistered_physical_table_is_rebuilt_not_reused() -> None:
    fps = compute_manifest_fingerprints({"nodes": {"model.p.events": _node("events", "select 1 as id")}})
    phys = fps["model.p.events"].physical_identifier
    # The table exists (e.g. cloned by seeding) but no environment/release points at it.
    actions = classify_plan_actions(
        fps,
        existing_physical_tables={phys},
        registered_physical_tables=set(),
    )
    assert actions["model.p.events"].action == "FULL_BUILD"
    reused = classify_plan_actions(
        fps,
        existing_physical_tables={phys},
        registered_physical_tables={phys},
    )
    assert reused["model.p.events"].action == "REUSE"


def test_export_state_lists_registered_physical_tables() -> None:
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}})
    harness.query_responses.extend(
        [
            FakeResult([]),
            FakeResult([]),
            FakeResult([("analytics_physical.events__abc12345",)]),
        ]
    )
    harness.call_macro("virtual_export_state", env="staging", base_env="prod")
    registered_sql = harness.executed_queries[2]
    assert "FROM `test_proj`.`analytics_physical`._environments" in registered_sql
    assert "FROM `test_proj`.`analytics_physical`._releases" in registered_sql
    state = next(line for line in harness.logs if "DBT_VIRTUAL_STATE_JSON:" in line)
    assert '"registered_physical_tables": ["events__abc12345"]' in state


def test_forward_only_and_on_schema_change_do_not_change_fingerprint() -> None:
    base = compute_node_fingerprint(_node("events", "select 1 as id")).fingerprint
    assert compute_node_fingerprint(_node("events", "select 1 as id", forward_only=True)).fingerprint == base
    assert (
        compute_node_fingerprint(_node("events", "select 1 as id", on_schema_change="append_new_columns")).fingerprint
        == base
    )
    assert compute_node_fingerprint(_node("events", "select 2 as id")).fingerprint != base


def _routine(name: str, parents: list[str], materialized: str = "table_function") -> dict:
    return {
        "unique_id": f"model.p.{name}",
        "name": name,
        "resource_type": "model",
        "config": {"materialized": materialized, "virtual_env": True},
        "depends_on": {"nodes": [f"model.p.{p}" for p in parents]},
    }


def test_promote_deploys_routines_in_dependency_order() -> None:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
            }
        }
    )
    # `a_outer` reads `b_inner`; alphabetical order would deploy `a_outer` first and fail.
    harness.graph = {
        "nodes": {
            "model.p.a_outer": _routine("a_outer", ["b_inner"]),
            "model.p.b_inner": _routine("b_inner", []),
            "model.p.orders": {
                "unique_id": "model.p.orders",
                "name": "orders",
                "resource_type": "model",
                "config": {"materialized": "table", "virtual_env": True},
                "depends_on": {"nodes": []},
            },
        }
    }
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders__fp1"))

    def ddl(name: str) -> str:
        return f"CREATE OR REPLACE TABLE FUNCTION `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`{name}`() AS (SELECT 1)"

    harness.query_responses.extend(
        [
            FakeResult(
                [
                    ("a_outer", "table_function", "fpa", "analytics_staging.a_outer", ddl("a_outer"), 3),
                    ("b_inner", "table_function", "fpb", "analytics_staging.b_inner", ddl("b_inner"), 3),
                    ("orders", "model", "fp1", "analytics_physical.orders__fp1", None, 2),
                ]
            ),
            FakeResult([]),
        ]
    )
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1")
    executed = harness.executed_queries
    view_at = next(i for i, q in enumerate(executed) if "VIEW `test_proj`.`analytics`.`orders`" in q)
    inner_at = next(i for i, q in enumerate(executed) if "`analytics`.`b_inner`()" in q)
    outer_at = next(i for i, q in enumerate(executed) if "`analytics`.`a_outer`()" in q)
    assert view_at < inner_at < outer_at


def test_rollback_refuses_release_with_missing_physical_tables() -> None:
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}})
    harness.query_responses.extend(
        [
            FakeResult([("orders", "model")]),
            FakeResult([("orders", "model", "fp0", "analytics_physical.orders__fp0", None)]),
        ]
    )
    try:
        harness.call_macro("virtual_rollback", env="prod", to_release="r0")
    except RuntimeError as exc:
        assert "rollback pre-flight failed" in str(exc)
    else:
        raise AssertionError("rollback must refuse a release whose tables were dropped")
    assert not any("VIEW" in q for q in harness.executed_queries)


def test_promote_and_rollback_take_the_lease_lock_when_asked() -> None:
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}})
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders__fp1"))
    harness.query_responses.extend(
        [
            FakeResult([]),  # delete expired
            FakeResult([]),  # delete own lease
            FakeResult([]),  # insert own lease
            FakeResult([("cli@host", "2026-11-05T00:15:00Z")]),  # live leases: ours
            FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1", None, 2)]),
            FakeResult([]),
        ]
    )
    harness.call_macro(
        "virtual_promote", from_env="staging", to_env="prod", release_id="r1", lock=True, lock_owner="cli@host"
    )
    executed = harness.executed_queries
    assert executed[2].startswith("INSERT INTO `test_proj`.`analytics_physical`._locks")
    assert executed[-1].startswith("DELETE FROM `test_proj`.`analytics_physical`._locks")
    assert "owner = 'cli@host'" in executed[-1]


def test_promote_fails_when_routine_is_missing_after_deploy() -> None:
    """The existence check after deploying a routine used to ignore its result."""
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}})
    ddl = "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`() AS (SELECT 1)"
    harness.query_responses.extend(
        [
            FakeResult([("tvf", "table_function", "fpt", "staging.tvf", ddl, 3)]),
            FakeResult([]),  # existing prod pointers
            FakeResult([]),  # create statement
            FakeResult([]),  # INFORMATION_SCHEMA.ROUTINES: not found
        ]
    )
    try:
        harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1")
    except RuntimeError as exc:
        assert "was not found after deployment" in str(exc)
    else:
        raise AssertionError("promote must fail when the routine does not exist after deployment")


def _seed_harness(**adapter_extras: object) -> MacroTestHarness:
    harness = MacroTestHarness(
        vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical", "base_environment": "prod"}}
    )
    harness.adapter_extras.update(adapter_extras)
    harness.graph = {
        "nodes": {
            "model.p.events": {
                "unique_id": "model.p.events",
                "name": "events",
                "resource_type": "model",
                "database": "test_proj",
                "schema": "analytics_physical",
                "alias": "events__new000000001",
                "config": {
                    "materialized": "incremental",
                    "virtual_env": True,
                    "partition_by": {"field": "d", "data_type": "date"},
                    "require_partition_filter": True,
                },
            }
        }
    }
    harness.existing_relations.add(("test_proj", "analytics_physical", "events__old000000001"))
    harness.query_responses.append(FakeResult([("events", "analytics_physical.events__old000000001")]))
    return harness


def test_seeding_skips_clone_when_partitioning_or_clustering_changed() -> None:
    """A CLONE keeps the base table's layout; a changed partition/cluster spec needs a full build."""
    harness = _seed_harness(is_replaceable=lambda rel, partition, cluster: False, parse_partition_by=lambda raw: raw)
    harness.call_macro("seed_incremental_clones")
    assert not any("CLONE" in q for q in harness.executed_queries)
    assert harness.cache_added_relations == []

    same_shape = _seed_harness(is_replaceable=lambda rel, partition, cluster: True, parse_partition_by=lambda raw: raw)
    same_shape.call_macro("seed_incremental_clones")
    executed = "\n".join(same_shape.executed_queries)
    assert "CLONE `test_proj`.`analytics_physical`.`events__old000000001`" in executed
    # Table options the model sets are applied to the clone.
    assert "SET OPTIONS (require_partition_filter=true)" in executed
