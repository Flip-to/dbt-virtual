"""Registry upserts are atomic on every adapter (DECISION(atomic_registry_upserts)).

`upsert_environment_pointer_sql` used to be a MERGE on BigQuery only; Snowflake, Databricks,
DuckDB, PostgreSQL and the default adapter ran `DELETE` and `INSERT` as two statements, committed
one by one, so a failure between them left the model without a pointer.
"""

from __future__ import annotations

import duckdb
import pytest

from tests.test_macros_execution import MacroTestHarness

ARGS = ("analytics_physical", "staging", "orders", "model", "fp1", "analytics_physical.orders__fp1", "rel_1")
MERGE_ADAPTERS = ["bigquery", "snowflake", "databricks", "duckdb"]


def _rows(n: int) -> list[dict]:
    return [
        {
            "name": f"m{i}", "resource_type": "model", "fingerprint": f"fp{i}",
            "physical_identifier": f"analytics_physical.m{i}__fp{i}", "release_id": "rel_1",
            "schema_key": "marts" if i % 2 else None,
        }
        for i in range(n)
    ]


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


@pytest.mark.parametrize("adapter", MERGE_ADAPTERS)
def test_single_row_pointer_upsert_is_one_merge_statement(adapter: str) -> None:
    harness = MacroTestHarness(adapter_type=adapter, target_database="" if adapter != "bigquery" else "test_proj")
    sql = _normalize(harness.call_macro("upsert_environment_pointer_sql", *ARGS))
    assert sql.startswith("MERGE ")
    assert ("MERGE INTO" in sql) == (adapter != "bigquery")
    assert "ON T.environment = S.environment AND T.model_name = S.model_name" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "DELETE" not in sql
    assert ";" not in sql


def test_postgres_pointer_upsert_is_one_statement_with_a_writable_cte() -> None:
    harness = MacroTestHarness(adapter_type="postgres", target_database="")
    sql = _normalize(harness.call_macro("upsert_environment_pointer_sql", *ARGS))
    assert sql.startswith("WITH replaced AS ( DELETE FROM analytics_physical._environments")
    assert "RETURNING 1 ) INSERT INTO analytics_physical._environments" in sql
    assert ";" not in sql, "one statement: it commits or fails as a whole"


def test_default_adapter_keeps_delete_insert_for_the_transaction_wrapper() -> None:
    harness = MacroTestHarness(adapter_type="sqlserver", target_database="")
    sql = _normalize(harness.call_macro("upsert_environment_pointer_sql", *ARGS))
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    assert [s.split(" ", 1)[0] for s in statements] == ["DELETE", "INSERT"]


@pytest.mark.parametrize("adapter", [*MERGE_ADAPTERS, "postgres", "sqlserver"])
def test_multi_row_upsert_covers_every_row_in_one_statement_per_adapter(adapter: str) -> None:
    harness = MacroTestHarness(adapter_type=adapter, target_database="" if adapter != "bigquery" else "test_proj")
    sql = _normalize(harness.call_macro("upsert_environment_pointers_sql", "analytics_physical", "staging", _rows(3)))
    for i in range(3):
        assert f"'m{i}'" in sql
    if adapter in MERGE_ADAPTERS:
        assert sql.count("SELECT") == 3 and sql.count("UNION ALL") == 2
        assert "DELETE" not in sql
    # schema_key is NULL for even rows and a literal for odd ones.
    assert "'marts'" in sql


def test_exec_ddl_dml_runs_each_statement_of_a_script() -> None:
    harness = MacroTestHarness(adapter_type="sqlserver", target_database="")
    harness.call_macro(
        "exec_ddl_dml",
        _normalize(harness.call_macro("upsert_environment_pointer_sql", *ARGS)),
    )
    assert [q.split(" ", 1)[0] for q in harness.executed_queries] == ["DELETE", "INSERT"]


def test_batched_flush_writes_environment_pointers_with_one_merge() -> None:
    harness = MacroTestHarness(
        adapter_type="snowflake",
        target_database="",
        vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}},
    )
    harness.existing_relations.update(
        {("", "analytics_physical", "_releases"), ("", "analytics_physical", "_environments")}
    )
    rows = [{**r, "tier": 2, "routine_ddl_template": None} for r in _rows(2)]
    harness.call_macro("write_location_registry_rows", rows, None, "staging", "rel_1")
    pointer = [q for q in harness.executed_queries if "_environments" in q and q.lstrip().startswith("MERGE")]
    assert len(pointer) == 1
    assert not [q for q in harness.executed_queries if q.lstrip().startswith("DELETE FROM analytics_physical._environments")]


# --- Real execution on an in-process DuckDB (MERGE INTO needs DuckDB 1.4 or newer) -------------------------


def _duckdb_registry() -> tuple[duckdb.DuckDBPyConnection, MacroTestHarness]:
    harness = MacroTestHarness(adapter_type="duckdb", target_database="")
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA analytics_physical")
    for stmt in harness.call_macro("ensure_registry_tables_sql", "analytics_physical").split(";"):
        if stmt.strip():
            con.execute(stmt)
    return con, harness


def test_duckdb_merge_inserts_then_updates_in_place() -> None:
    con, harness = _duckdb_registry()
    first = harness.call_macro("upsert_environment_pointer_sql", *ARGS, "marts")
    con.execute(first)
    second = harness.call_macro(
        "upsert_environment_pointer_sql",
        "analytics_physical", "staging", "orders", "model", "fp2", "analytics_physical.orders__fp2", "rel_2",
    )
    con.execute(second)
    rows = con.execute(
        "SELECT model_name, fingerprint, release_id, schema_key FROM analytics_physical._environments"
    ).fetchall()
    assert rows == [("orders", "fp2", "rel_2", None)]


def test_duckdb_merge_batch_replaces_existing_and_adds_new_rows() -> None:
    con, harness = _duckdb_registry()
    con.execute(harness.call_macro("upsert_environment_pointers_sql", "analytics_physical", "staging", _rows(2)))
    batch = [{**r, "fingerprint": "new"} for r in _rows(4)]
    con.execute(harness.call_macro("upsert_environment_pointers_sql", "analytics_physical", "staging", batch))
    rows = con.execute(
        "SELECT model_name, fingerprint FROM analytics_physical._environments ORDER BY 1"
    ).fetchall()
    assert rows == [(f"m{i}", "new") for i in range(4)]
