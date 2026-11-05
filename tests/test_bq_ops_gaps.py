"""BigQuery operations gaps: env view authorization, expiry of abandoned envs, view metadata.

Harness tests only (mocked adapter): nothing here ran against real BigQuery.
"""

from __future__ import annotations

from typing import Any

from test_macros_execution import FakeRelation, FakeResult, MacroTestHarness, _bq_harness


def _promote_two_models(harness: MacroTestHarness, to_env: str = "prod") -> None:
    harness.existing_relations.update(
        {("test_proj", "analytics_physical", "a__fp1"), ("test_proj", "analytics_physical", "b__fp2")}
    )
    harness.query_responses.extend(
        [
            FakeResult(
                [
                    ("a", "model", "fp1", "analytics_physical.a__fp1", None, 2),
                    ("b", "model", "fp2", "analytics_physical.b__fp2", None, 2),
                ]
            ),
            FakeResult([]),
        ]
    )
    harness.call_macro("virtual_promote", from_env="staging", to_env=to_env, release_id="r1")


def test_authorize_env_views_authorizes_each_env_dataset_once_on_bigquery() -> None:
    harness = _bq_harness(authorize_env_views=True)
    _promote_two_models(harness)
    # Two views in one env dataset: one dataset-level authorization, not one per view.
    assert harness.grant_calls == [
        (
            {
                "dataset": {"projectId": "test_proj", "datasetId": "analytics"},
                "targetTypes": ["VIEWS"],
            },
            "dataset",
            None,
            {"project": "test_proj", "dataset": "analytics_physical"},
        )
    ]


def test_authorize_env_views_is_off_by_default_and_a_noop_off_bigquery() -> None:
    harness = _bq_harness()
    _promote_two_models(harness)
    assert harness.grant_calls == []

    pg = MacroTestHarness(
        adapter_type="postgres",
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
                "authorize_env_views": True,
            }
        },
    )
    pg.existing_relations.add(("test_proj", "analytics_physical", "a__fp1"))
    pg.query_responses.extend(
        [FakeResult([("a", "model", "fp1", "analytics_physical.a__fp1", None, 2)]), FakeResult([])]
    )
    pg.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1")
    assert pg.grant_calls == []


def test_authorize_env_views_covers_rollback_and_each_env_dataset() -> None:
    harness = _bq_harness(authorize_env_views=True)
    harness.vars_dict["dbt_virtual"]["env_schemas"]["prod"] = {
        "default": "analytics",
        "mart": "analytics_mart",
    }
    harness.existing_relations.update(
        {("test_proj", "analytics_physical", "a__fp1"), ("test_proj", "analytics_physical", "b__fp2")}
    )
    harness.query_responses.extend(
        [
            FakeResult([]),
            FakeResult(
                [
                    ("a", "model", "fp1", "analytics_physical.a__fp1", None, None),
                    ("b", "model", "fp2", "analytics_physical.b__fp2", None, "mart"),
                ]
            ),
        ]
    )
    harness.call_macro("virtual_rollback", env="prod", to_release="r0")
    datasets = sorted(c[0]["dataset"]["datasetId"] for c in harness.grant_calls)
    assert datasets == ["analytics", "analytics_mart"]


def _documented_node() -> dict[str, Any]:
    return {
        "unique_id": "model.pkg.a",
        "name": "a",
        "resource_type": "model",
        "description": "Orders; it's final",
        "columns": {
            "id": {"name": "id", "description": "Primary key"},
            "x": {"name": "x", "description": ""},
        },
        "config": {
            "materialized": "table",
            "virtual_env": True,
            "persist_docs": {"relation": True, "columns": True},
            "labels": {"team": "data", "tier": "gold"},
        },
    }


def test_env_view_ddl_carries_description_and_labels_on_bigquery() -> None:
    harness = _bq_harness()
    harness.graph = {"nodes": {"model.pkg.a": _documented_node()}}
    _promote_two_models(harness)
    ddl = next(q for q in harness.executed_queries if "VIEW `test_proj`.`analytics`.`a`" in q)
    assert "OPTIONS(description='Orders; it\\'s final', labels=[('team', 'data'), ('tier', 'gold')])" in ddl
    # A model without a node in the project gets a bare view.
    bare = next(q for q in harness.executed_queries if "VIEW `test_proj`.`analytics`.`b`" in q)
    assert "OPTIONS" not in bare
    assert harness.column_updates == [
        ("`test_proj`.`analytics`.`a`", {"id": {"name": "id", "description": "Primary key"}})
    ]


def test_sync_env_view_creates_view_with_options_and_persists_columns() -> None:
    harness = _bq_harness()
    harness.model = _documented_node()
    harness.this = FakeRelation("test_proj", "analytics_physical", "a__fp1")
    harness.call_macro("sync_env_view")
    ddl = next(q for q in harness.executed_queries if "CREATE OR REPLACE VIEW" in q)
    assert "OPTIONS(description=" in ddl and "labels=[" in ddl
    assert len(harness.column_updates) == 1


def test_description_and_labels_stay_out_of_the_fingerprint() -> None:
    harness = _bq_harness()
    node = _documented_node()
    node["raw_code"] = "select 1"
    before = harness.call_macro("get_fingerprint", node)
    node["description"] = "changed"
    node["columns"]["id"]["description"] = "changed"
    node["config"]["labels"] = {"team": "other"}
    assert harness.call_macro("get_fingerprint", node) == before


def test_gc_expires_unmapped_idle_envs_and_keeps_mapped_ones() -> None:
    harness = _bq_harness(expire_env_days=14)
    harness.query_responses.append(FakeResult([("pr_7",), ("staging",), ("prod",), ("pr_9",)]))
    harness.call_macro("virtual_gc", dry_run=False)
    executed = "\n".join(harness.executed_queries)
    assert "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)" in harness.executed_queries[0]
    assert "DROP SCHEMA IF EXISTS `test_proj`.`analytics_pr_7` CASCADE" in executed
    assert "DROP SCHEMA IF EXISTS `test_proj`.`analytics_pr_9` CASCADE" in executed
    assert "DROP SCHEMA IF EXISTS `test_proj`.`analytics_staging`" not in executed
    assert "DROP SCHEMA IF EXISTS `test_proj`.`analytics` " not in executed
    assert "SET status = 'expired'" in executed and "source_environment = 'pr_7'" in executed


def test_gc_expire_dry_run_only_logs_and_is_off_by_default() -> None:
    harness = _bq_harness(expire_env_days=14)
    harness.query_responses.append(FakeResult([("pr_7",)]))
    harness.call_macro("virtual_gc", dry_run=True)
    assert not any("DROP SCHEMA" in q or q.startswith("DELETE") for q in harness.executed_queries)
    assert any("Would drop expired environment pr_7" in m for m in harness.logs)

    off = _bq_harness()
    off.call_macro("virtual_gc", dry_run=False)
    assert not any("GROUP BY environment" in q for q in off.executed_queries)

    explicit = _bq_harness()
    explicit.query_responses.append(FakeResult([("pr_7",)]))
    explicit.call_macro("virtual_gc", dry_run=True, expire_env_days=3)
    assert "INTERVAL 3 DAY" in explicit.executed_queries[0]
