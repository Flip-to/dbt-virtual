"""The SQL `virtual_acquire_lock` sends on each adapter (DECISION(lease_lock_single_statement_acquire)).

The behavior under concurrency is in `tests/registry_concurrency.py` (DuckDB, PostgreSQL, BigQuery
emulator); these tests pin the SQL shape per adapter, which the emulators cannot prove for the real
warehouses: one conditional statement (never DELETE + INSERT), the key and the takeover condition.
"""

from __future__ import annotations

import pytest

from tests.test_macros_execution import FakeResult, MacroTestHarness

CFG = {"dbt_virtual": {"physical_schema": "phys"}}


def _acquire(adapter_type: str, live_rows: list[tuple]) -> tuple[MacroTestHarness, str | None]:
    harness = MacroTestHarness(vars_dict=CFG, adapter_type=adapter_type)
    for registry in ("_environments", "_releases"):
        harness.existing_relations.add(("test_proj", "phys", registry))
    if adapter_type in ("duckdb", "postgres"):
        harness.query_responses.append(FakeResult([(1,)]))  # unique index exists
    harness.query_responses.extend([FakeResult([]), FakeResult(live_rows)])
    try:
        harness.call_macro("virtual_acquire_lock", env="prod", owner="me", ttl_minutes=15)
    except RuntimeError as exc:
        return harness, str(exc)
    return harness, None


@pytest.mark.parametrize("adapter_type", ["bigquery", "snowflake", "databricks"])
def test_merge_adapters_acquire_with_one_merge(adapter_type) -> None:
    harness, error = _acquire(adapter_type, [("me", "2999-01-01")])
    assert error is None
    sql = harness.executed_queries
    assert sql[0].lstrip().startswith("MERGE INTO")
    assert "ON t.environment = s.environment" in sql[0]
    assert "WHEN MATCHED AND (t.owner = s.owner OR t.expires_at <= s.acquired_at)" in sql[0]
    assert "WHEN NOT MATCHED THEN" in sql[0]
    assert not any(q.lstrip().startswith(("DELETE", "INSERT")) for q in sql), sql  # no check-then-write


@pytest.mark.parametrize("adapter_type", ["duckdb", "postgres"])
def test_key_enforcing_adapters_acquire_with_on_conflict(adapter_type) -> None:
    harness, error = _acquire(adapter_type, [("me", "2999-01-01")])
    assert error is None
    sql = harness.executed_queries
    assert "_locks_environment_uq" in sql[0]  # the index check
    upsert = sql[1]
    assert "ON CONFLICT (environment) DO UPDATE" in upsert
    assert "WHERE l.owner = EXCLUDED.owner OR l.expires_at <= EXCLUDED.acquired_at" in upsert
    assert not any(q.lstrip().startswith("DELETE") for q in sql), sql


def test_missing_unique_index_is_created_after_deduplicating() -> None:
    harness = MacroTestHarness(vars_dict=CFG, adapter_type="duckdb")
    for registry in ("_environments", "_releases"):
        harness.existing_relations.add(("test_proj", "phys", registry))
    harness.query_responses.extend([FakeResult([])] * 4 + [FakeResult([("me", "2999-01-01")])])
    harness.call_macro("virtual_acquire_lock", env="prod", owner="me")
    sql = harness.executed_queries
    assert sql[1].startswith("DELETE FROM") and "o.acquired_at > " in sql[1]
    assert sql[2].startswith("CREATE UNIQUE INDEX IF NOT EXISTS _locks_environment_uq")
    assert "ON CONFLICT" in sql[3]


@pytest.mark.parametrize("adapter_type", ["bigquery", "snowflake", "databricks", "duckdb", "postgres"])
def test_another_live_owner_is_a_conflict_and_the_loser_removes_its_row(adapter_type) -> None:
    harness, error = _acquire(adapter_type, [("other", "2999-01-01"), ("me", "2999-01-01")])
    assert error and "dbt-virtual lock conflict" in error and "'other'" in error
    assert harness.executed_queries[-1].startswith("DELETE FROM")
    assert "owner = 'me'" in harness.executed_queries[-1]


def test_a_lease_that_is_not_live_after_the_statement_is_a_conflict() -> None:
    harness, error = _acquire("bigquery", [])
    assert error and "dbt-virtual lock conflict" in error
    assert not harness.executed_queries[-1].startswith("DELETE")
