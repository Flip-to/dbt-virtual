"""Cross-project environments: CI writes to its own project, prod promotes by cloning (macro harness).

`vars.dbt_virtual.physical_locations` puts an environment's registry and physical tables in
another project; `virtual_promote` then clones the release's tables into the target location.
"""

from __future__ import annotations

import pytest

from tests.test_macros_execution import FakeResult, MacroTestHarness

LOCATIONS = {
    "prod": {"database": "prod_proj", "schema": "phys_prod"},
    "default": {"database": "ci_proj", "schema": "phys_ci"},
}


def _harness(*, target_name: str = "prod", target_database: str = "prod_proj", locations=LOCATIONS, extra=None):
    cfg = {
        "physical_schema": "phys_default",
        "base_environment": "prod",
        "env_schemas": {"prod": "analytics", "ci": "analytics_ci"},
        **({"physical_locations": locations} if locations is not None else {}),
        **(extra or {}),
    }
    return MacroTestHarness(
        vars_dict={"dbt_virtual": cfg}, target_name=target_name, target_database=target_database
    )


# (model, type, fingerprint, physical id, routine ddl, tier, schema_key, build_quality, sealed_at)
RELEASE_ROWS = [
    ("orders", "model", "fp1", "phys_ci.orders__fp1", None, 2, None, "full", "2026-11-04"),
]


def test_physical_location_resolution() -> None:
    h = _harness()
    assert h.call_macro("physical_location", "prod") == {"database": "prod_proj", "schema": "phys_prod"}
    # Unmapped environments use the `default` entry.
    assert h.call_macro("physical_location", "pr_42") == {"database": "ci_proj", "schema": "phys_ci"}
    # The current environment (target prod) is prod's location.
    assert h.call_macro("get_config")["physical_schema"] == "phys_prod"
    assert h.call_macro("active_database") == "prod_proj"
    # Without `physical_locations`: target database and `physical_schema`, as before.
    plain = _harness(locations=None, target_database="test_proj")
    assert plain.call_macro("physical_location", "pr_42") == {"database": "test_proj", "schema": "phys_default"}
    assert plain.call_macro("active_database") == "test_proj"


def test_cross_location_promote_clones_then_points_the_view_at_the_clone() -> None:
    h = _harness()
    h.existing_relations.add(("ci_proj", "phys_ci", "orders__fp1"))
    h.query_responses.extend([FakeResult(RELEASE_ROWS), FakeResult([])])
    h.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="pr_42_abc")
    queries = h.executed_queries

    # The release is read from the CI registry.
    assert any("FROM `ci_proj`.`phys_ci`._releases" in q for q in queries)
    clone = next(q for q in queries if "CREATE TABLE IF NOT EXISTS `prod_proj`.`phys_prod`.`orders__fp1`" in q)
    assert "CLONE `ci_proj`.`phys_ci`.`orders__fp1`" in clone
    view = next(q for q in queries if q.startswith("CREATE OR REPLACE VIEW `prod_proj`.`analytics`.`orders`"))
    assert "`prod_proj`.`phys_prod`.`orders__fp1`" in view
    assert queries.index(clone) < queries.index(view)
    # Pointer and release row go to the prod registry; the CI registry is never written.
    pointer = next(q for q in queries if "`prod_proj`.`phys_prod`._environments" in q and q.startswith("MERGE"))
    assert "'phys_prod.orders__fp1'" in pointer
    insert = next(q for q in queries if q.startswith("INSERT INTO `prod_proj`.`phys_prod`._releases"))
    assert "'pr_42_abc', 'pr_42', 'prod', 'orders'" in insert and "'promoted'" in insert
    assert not any(
        q.split("(")[0].startswith(("INSERT", "UPDATE", "DELETE", "MERGE")) and "`ci_proj`" in q.split("(")[0]
        for q in queries
    )


def test_cross_location_promote_does_not_clone_an_existing_target_table() -> None:
    h = _harness()
    h.existing_relations.add(("ci_proj", "phys_ci", "orders__fp1"))
    h.existing_relations.add(("prod_proj", "phys_prod", "orders__fp1"))
    h.query_responses.extend([FakeResult(RELEASE_ROWS), FakeResult([])])
    h.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="pr_42_abc")
    assert not any("CLONE" in q for q in h.executed_queries)
    assert any(q.startswith("CREATE OR REPLACE VIEW `prod_proj`.`analytics`.`orders`") for q in h.executed_queries)


def test_cross_location_preflight_checks_the_source_tables() -> None:
    h = _harness()  # the CI table does not exist
    h.query_responses.extend([FakeResult(RELEASE_ROWS), FakeResult([])])
    with pytest.raises(RuntimeError, match=r"pre-flight failed: physical table ci_proj\.phys_ci\.orders__fp1"):
        h.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="pr_42_abc")
    assert not any("CLONE" in q for q in h.executed_queries)


def test_cross_location_routine_is_deployed_in_the_target_project() -> None:
    h = _harness()
    rows = [
        (
            "tvf",
            "table_function",
            "fpt",
            "analytics_ci.tvf",
            "CREATE OR REPLACE TABLE FUNCTION `ci_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`() AS (SELECT 1)",
            3,
            None,
            "full",
            "2026-11-04",
        )
    ]
    h.query_responses.extend([FakeResult(rows), FakeResult([])])
    h.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="r")
    ddl = next(q for q in h.executed_queries if q.startswith("CREATE OR REPLACE TABLE FUNCTION"))
    assert "`prod_proj`.`analytics`.`tvf`" in ddl and "ci_proj" not in ddl


def test_cross_location_promote_stores_the_routine_template_with_the_target_project() -> None:
    """The release rows copied into the target registry keep a routine template that names the target
    project. Before, they kept the source project, so a later `virtual_rollback` in prod redeployed the
    routine from a template that read the CI project (real BigQuery: `Dataset <ci>:<ds> was not found`)."""
    h = _harness()
    rows = [
        (
            "tvf",
            "table_function",
            "fpt",
            "analytics_ci.tvf",
            "CREATE OR REPLACE TABLE FUNCTION `ci_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`() AS (SELECT 1)",
            3,
            None,
            "full",
            "2026-11-04",
        )
    ]
    h.query_responses.extend([FakeResult(rows), FakeResult([])])
    h.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="r")
    stored = " ".join(q for q in h.executed_queries if "INSERT INTO" in q and "_releases" in q)
    assert "`prod_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`" in stored
    assert "ci_proj" not in stored


def test_retarget_project_in_ddl_only_replaces_whole_project_ids() -> None:
    h = _harness()
    ddl = "SELECT * FROM `ci_proj`.`ds`.`t` JOIN `other-ci_proj2`.`ds`.`u` JOIN ci_proj_x.ds.v"
    out = h.call_macro("retarget_project_in_ddl", ddl, "ci_proj")
    assert "`prod_proj`.`ds`.`t`" in out and "other-ci_proj2" in out and "ci_proj_x" in out
    assert h.call_macro("retarget_project_in_ddl", ddl, "") == ddl
    assert h.call_macro("retarget_project_in_ddl", ddl, "prod_proj") == ddl


def test_same_location_promote_is_unchanged() -> None:
    # No physical_locations: the generated SQL names the target database and `physical_schema`.
    h = _harness(locations=None, target_database="test_proj")
    h.existing_relations.add(("test_proj", "phys_default", "orders__fp1"))
    rows = [("orders", "model", "fp1", "phys_default.orders__fp1", None, 2, None, "full", "2026-11-04")]
    h.query_responses.extend([FakeResult(rows), FakeResult([])])
    h.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1")
    joined = "\n".join(h.executed_queries)
    assert "CLONE" not in joined and "INSERT INTO" not in joined
    assert "CREATE OR REPLACE VIEW `test_proj`.`analytics`.`orders`" in joined
    assert "SET status = 'promoted'" in joined


def test_cross_location_promote_needs_a_release_and_the_target_environments_target() -> None:
    h = _harness()
    with pytest.raises(RuntimeError, match="needs a release"):
        h.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="")
    ci = _harness(target_name="ci", target_database="ci_proj")
    with pytest.raises(RuntimeError, match="Run promote with the target of the environment"):
        ci.call_macro("virtual_promote", from_env="pr_42", to_env="prod", release_id="r", source_database="other_proj")


def test_explicit_source_location_overrides_the_config() -> None:
    h = _harness(locations={"prod": {"database": "prod_proj", "schema": "phys_prod"}})
    h.existing_relations.add(("other_proj", "other_phys", "orders__fp1"))
    h.query_responses.extend([FakeResult(RELEASE_ROWS), FakeResult([])])
    h.call_macro(
        "virtual_promote", from_env="pr_42", to_env="prod", release_id="r",
        source_database="other_proj", source_schema="other_phys",
    )
    assert any("FROM `other_proj`.`other_phys`._releases" in q for q in h.executed_queries)
    assert any("CLONE `other_proj`.`other_phys`.`orders__fp1`" in q for q in h.executed_queries)


def test_seeding_reads_base_pointers_from_the_base_location_and_clones_into_ci() -> None:
    h = _harness(target_name="pr_42", target_database="ci_proj", extra={"environment": "pr_42"})
    node = {
        "name": "events",
        "unique_id": "model.p.events",
        "alias": "events__newfp",
        "schema": "phys_ci",
        "database": "ci_proj",
        "resource_type": "model",
        "config": {"virtual_env": True, "materialized": "incremental"},
    }
    h.graph["nodes"]["model.p.events"] = node
    h.existing_relations.add(("prod_proj", "phys_prod", "_environments"))
    h.existing_relations.add(("prod_proj", "phys_prod", "events__oldfp"))
    h.existing_relations.add(("ci_proj", "phys_ci", "_releases"))
    # base registry rows, then registered tables of the CI registry
    h.query_responses.extend([
        FakeResult([("events", "phys_prod.events__oldfp", "oldfp")]),
        FakeResult([]),
    ])
    h.call_macro("seed_incremental_clones", model_names=["events"])
    queries = h.executed_queries
    assert any("FROM `prod_proj`.`phys_prod`._environments" in q for q in queries)
    clone = next(q for q in queries if "CLONE" in q)
    assert "`ci_proj`.`phys_ci`.`events__newfp`" in clone and "CLONE `prod_proj`.`phys_prod`.`events__oldfp`" in clone


def test_export_state_reads_the_base_environment_from_its_location() -> None:
    h = _harness(target_name="pr_42", target_database="ci_proj", extra={"environment": "pr_42"})
    for ident in ("_environments", "_releases"):
        h.existing_relations.add(("prod_proj", "phys_prod", ident))
    h.existing_relations.add(("ci_proj", "phys_ci", "_environments"))
    h.existing_relations.add(("ci_proj", "phys_ci", "_releases"))
    h.call_macro("virtual_export_state", env="pr_42", base_env="prod")
    queries = h.executed_queries
    assert any("FROM `prod_proj`.`phys_prod`._environments" in q and "environment = 'prod'" in q for q in queries)
    assert any("`prod_proj`.`phys_prod`.INFORMATION_SCHEMA.TABLES" in q for q in queries)
    assert any("`ci_proj`.`phys_ci`.INFORMATION_SCHEMA.TABLES" in q for q in queries)
    payload = next(m for m in h.logs if m.startswith("DBT_VIRTUAL_STATE_JSON:"))
    assert '"base_physical_tables"' in payload


def test_bigquery_gates_read_prod_from_its_own_location() -> None:
    """Found on real BigQuery (docs/findings/20261105_bigquery_sandbox_verification.md): run from a CI
    target, `virtual_audit` and `virtual_data_diff` read `to_env`'s registry from the CI location, saw
    no prod pointers and reported every model as new, so the gates passed without comparing anything."""
    from types import SimpleNamespace

    h = _harness(target_name="staging", target_database="ci_proj")
    h.graph["nodes"]["model.p.orders"] = {"name": "orders", "resource_type": "model", "config": {"unique_key": "id"}}
    h.adapter_extras["get_columns_in_relation"] = lambda rel: [SimpleNamespace(name="id")]
    h.adapter_extras["quote"] = lambda name: f"`{name}`"
    h.query_responses.extend(
        [
            FakeResult([("orders", "phys_ci.orders__fp2")]),  # candidate release rows (CI registry)
            FakeResult([("orders", "phys_prod.orders__fp1")]),  # prod pointer (prod registry)
        ]
    )
    h.call_macro("virtual_data_diff", from_env="staging", to_env="prod", release_id="r1")
    q = h.executed_queries
    assert "`ci_proj`.`phys_ci`._releases" in q[0]
    assert "`prod_proj`.`phys_prod`._environments" in q[1]
    assert any("`prod_proj`.`phys_prod`.`orders__fp1`" in x for x in q[2:])
    assert any("`ci_proj`.`phys_ci`.`orders__fp2`" in x for x in q[2:])

    a = _harness(target_name="staging", target_database="ci_proj")
    a.query_responses.extend(
        [
            FakeResult([("orders", "phys_ci.orders__fp2")]),
            FakeResult([("orders", "phys_prod.orders__fp1")]),
            FakeResult([(4,)]),
            FakeResult([(3,)]),
        ]
    )
    a.call_macro("virtual_audit", from_env="staging", to_env="prod", release_id="r1", check_coverage=False)
    assert "`prod_proj`.`phys_prod`._environments" in a.executed_queries[1]
    assert "`prod_proj`.`phys_prod`.__TABLES__" in a.executed_queries[3]
    assert any("audit [orders]: OK (candidate=4 rows, prod=3 rows)" in m for m in a.logs)
