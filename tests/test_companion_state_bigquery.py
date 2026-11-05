"""Companion state on BigQuery: the SQL the macros render for the gaps closed in dbt-virtual-0024.

Macro harness (no warehouse), BigQuery dialect. The same macros run against the BigQuery emulator in
`scripts/run_emulator_e2e.py` (step 19) and through real dbt-duckdb in
`tests/test_dbt_duckdb_companion_state.py`.
"""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from typing import Any

from tests.test_macros_execution import FakeResult, MacroTestHarness

MANIFEST_COLUMNS = ["model", "last_success"]
EVENTS_NODE = {
    "unique_id": "model.p.events",
    "name": "events",
    "resource_type": "model",
    "alias": "events__fp2",
    "database": "prod_proj",
    "schema": "phys_prod",
    "tags": [],
    "config": {"materialized": "incremental", "virtual_env": True},
    "depends_on": {"nodes": []},
}
LOCATIONS = {
    "prod": {"database": "prod_proj", "schema": "phys_prod"},
    "default": {"database": "ci_proj", "schema": "phys_ci"},
}


class Col:
    def __init__(self, name: str, data_type: str) -> None:
        self.name, self.data_type = name, data_type


class Script:
    """Answers `run_query` by the first regular expression that matches the SQL."""

    def __init__(self) -> None:
        self.answers: list[tuple[str, FakeResult]] = []

    def on(self, pattern: str, rows: list[tuple[Any, ...]], columns: list[str] | None = None) -> None:
        self.answers.append((pattern, FakeResult(rows, columns or [])))

    def __call__(self, sql: str) -> tuple[list[tuple[Any, ...]], list[str]]:
        flat = " ".join(sql.split())
        for pattern, result in self.answers:
            if re.search(pattern, flat):
                return result.rows, result.column_names
        return [], []


def _harness(
    script: Script, *, target_name: str, target_database: str, extra: dict[str, Any] | None = None
) -> MacroTestHarness:
    cfg = {
        "physical_schema": "phys_default",
        "base_environment": "prod",
        "env_schemas": {"prod": "analytics", "ci": "analytics_ci", "staging": "analytics_staging"},
        "companion_state": [
            {
                "table": "manifest",
                "key_column": "model",
                "env_tables": {
                    "prod": "prod_proj.analytics.manifest",
                    "ci": "ci_proj.analytics_ci.manifest",
                    "staging": "prod_proj.analytics_staging.manifest",
                },
            }
        ],
        **(extra or {}),
    }
    h = MacroTestHarness(vars_dict={"dbt_virtual": cfg}, target_name=target_name, target_database=target_database)
    h.graph = {"nodes": {"model.p.events": EVENTS_NODE}}
    h.query_executor = script
    h.adapter_extras = {
        "get_columns_in_relation": lambda rel: [Col("model", "STRING"), Col("last_success", "TIMESTAMP")],
        "quote": lambda name: f"`{name}`",
    }
    return h


def _insert_statements(h: MacroTestHarness) -> list[str]:
    return [q for q in h.executed_queries if q.startswith("INSERT INTO") and "manifest" in q]


def _cross_harness(script: Script, relations: list[tuple[str, str, str]]) -> MacroTestHarness:
    h = _harness(script, target_name="prod", target_database="prod_proj", extra={"physical_locations": LOCATIONS})
    h.existing_relations.update(relations)
    return h


def test_cross_location_promote_copies_the_source_environments_live_rows() -> None:
    script = Script()
    release = ("events", "model", "fp1", "phys_ci.events__fp1", None, 2, None, "full", "2026-11-04")
    script.on(r"FROM `ci_proj`\.`phys_ci`\._releases WHERE release_id", [release])
    script.on(r"SELECT environment FROM `ci_proj`\.`phys_ci`\._environments .*fingerprint = 'fp1'", [("ci",)])
    script.on(
        r"SELECT \* FROM `ci_proj`\.`analytics_ci`\.`manifest` WHERE model = 'events'",
        [("events", "2026-11-03 00:00:00+00:00")],
        MANIFEST_COLUMNS,
    )
    h = _cross_harness(
        script,
        [
            ("ci_proj", "phys_ci", "events__fp1"),
            ("prod_proj", "analytics", "manifest"),
            ("ci_proj", "analytics_ci", "manifest"),
            ("prod_proj", "phys_prod", "_releases"),
            ("prod_proj", "phys_prod", "_companion_state"),
        ],
    )

    h.call_macro("virtual_promote", from_env="ci", to_env="prod", release_id="r1")

    # The ci environment's rows were read and written into prod's manifest table for the model.
    assert any("FROM `ci_proj`.`analytics_ci`.`manifest`" in q for q in h.executed_queries)
    assert any(
        q.startswith("DELETE FROM `prod_proj`.`analytics`.`manifest` WHERE model = 'events'")
        for q in h.executed_queries
    )
    inserts = _insert_statements(h)
    assert len(inserts) == 1 and "`prod_proj`.`analytics`.`manifest`" in inserts[0], inserts
    assert "CAST('2026-11-03 00:00:00+00:00' AS TIMESTAMP)" in inserts[0]


def test_cross_location_promote_falls_back_to_rows_saved_in_the_source_registry() -> None:
    script = Script()
    release = ("events", "model", "fp1", "phys_ci.events__fp1", None, 2, None, "full", "2026-11-04")
    script.on(r"FROM `ci_proj`\.`phys_ci`\._releases WHERE release_id", [release])
    saved = json.dumps([{"model": "events", "last_success": "2026-11-02 00:00:00+00:00"}])
    script.on(r"SELECT state_json FROM `ci_proj`\.`phys_ci`\._companion_state .*fingerprint = 'fp1'", [(saved,)])
    h = _cross_harness(
        script,
        [
            ("ci_proj", "phys_ci", "events__fp1"),
            ("prod_proj", "analytics", "manifest"),
            ("prod_proj", "phys_prod", "_releases"),
            ("prod_proj", "phys_prod", "_companion_state"),
            ("ci_proj", "phys_ci", "_companion_state"),
        ],
    )

    h.call_macro("virtual_promote", from_env="ci", to_env="prod", release_id="r1")

    inserts = _insert_statements(h)
    assert len(inserts) == 1 and "CAST('2026-11-02 00:00:00+00:00' AS TIMESTAMP)" in inserts[0], inserts
    assert any("rows saved when an environment left it" in m for m in h.logs)


def _result(node: dict[str, Any], status: str) -> SimpleNamespace:
    return SimpleNamespace(node=node, status=status)


def _failed_build_harness(pointer_fp: str) -> MacroTestHarness:
    script = Script()
    script.on(
        r"SELECT fingerprint, updated_at FROM `prod_proj`\.`phys_default`\._environments",
        [(pointer_fp, "2026-11-01")],
    )
    saved = json.dumps([{"model": "events", "last_success": "2026-11-04 00:00:00+00:00"}])
    script.on(
        r"SELECT state_json FROM `prod_proj`\.`phys_default`\._companion_state .*fingerprint = 'fp1'", [(saved,)]
    )
    h = _harness(script, target_name="staging", target_database="prod_proj")
    h.existing_relations.update(
        [
            ("prod_proj", "phys_default", "_releases"),
            ("prod_proj", "phys_default", "_companion_state"),
            ("prod_proj", "analytics_staging", "manifest"),
        ]
    )
    return h


def test_failed_build_restores_the_state_of_the_table_the_environment_still_points_at() -> None:
    h = _failed_build_harness("fp1")
    h.call_macro("companion_repair_failed", [_result(EVENTS_NODE, "error")], "staging")
    inserts = _insert_statements(h)
    assert len(inserts) == 1 and "`prod_proj`.`analytics_staging`.`manifest`" in inserts[0], inserts
    assert "CAST('2026-11-04 00:00:00+00:00' AS TIMESTAMP)" in inserts[0]


def test_repair_leaves_successful_builds_and_unmoved_pointers_alone() -> None:
    ok = _failed_build_harness("fp1")
    ok.call_macro("companion_repair_failed", [_result(EVENTS_NODE, "success")], "staging")
    assert not _insert_statements(ok)
    # The pointer is already on the table the failed run tried to build: nothing was moved.
    same = _failed_build_harness("fp2")
    same.call_macro("companion_repair_failed", [_result(EVENTS_NODE, "error")], "staging")
    assert not _insert_statements(same)


def test_flush_registry_repairs_in_per_model_mode_too() -> None:
    h = _failed_build_harness("fp1")
    h.vars_dict["dbt_virtual"]["registry_write_mode"] = "per_model"
    h.flags.WHICH = "build"
    h.call_macro("flush_registry", [_result(EVENTS_NODE, "error")])
    assert len(_insert_statements(h)) == 1


def _gc_harness(*, registry_exists: bool = True) -> MacroTestHarness:
    script = Script()
    script.on(r"INFORMATION_SCHEMA\.TABLES", [("events__fp1",), ("events__fp3",)])
    script.on(r"SELECT COUNT\(\*\) FROM `prod_proj`\.`phys_default`\._companion_state", [(2,)])
    h = _harness(script, target_name="prod", target_database="prod_proj")
    if registry_exists:
        h.existing_relations.add(("prod_proj", "phys_default", "_companion_state"))
    return h


def test_gc_deletes_saved_state_of_the_tables_it_drops() -> None:
    h = _gc_harness()
    h.call_macro("virtual_gc", dry_run=False, retention_releases=1, retention_days=0)
    deletes = [
        q for q in h.executed_queries if q.startswith("DELETE FROM `prod_proj`.`phys_default`._companion_state")
    ]
    assert len(deletes) == 1, h.executed_queries
    assert "CONCAT(model_name, '__', fingerprint) IN ('events__fp1', 'events__fp3')" in deletes[0]
    assert any(
        q.startswith("DROP TABLE IF EXISTS `prod_proj`.`phys_default`.`events__fp1`") for q in h.executed_queries
    )


def test_gc_dry_run_only_counts_the_state_rows_and_skips_a_missing_registry() -> None:
    h = _gc_harness()
    h.call_macro("virtual_gc", dry_run=True, retention_releases=1, retention_days=0)
    assert not any(q.startswith("DELETE FROM") for q in h.executed_queries)
    assert any("Would delete 2 saved companion state row(s)" in m for m in h.logs)
    none = _gc_harness(registry_exists=False)
    none.call_macro("virtual_gc", dry_run=False, retention_releases=1, retention_days=0)
    assert not any("_companion_state" in q for q in none.executed_queries)
