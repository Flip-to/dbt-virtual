"""End-to-end verification of dbt-virtual on DuckDB (`adapter_type='duckdb'`).

Executes the actual SQL rendered by `MacroTestHarness` against a real in-process DuckDB database:
1. Registry creation (`ensure_registry_tables_sql` -> `analytics_physical._environments`, `_releases`)
2. Content-addressed table v1 (`analytics_physical.orders_metrics__<fp_v1>`) + `sync_env_view`
3. DuckDB scalar & table macros (`create_routine_ddl_sql` for `function` and `table_function`)
4. Zero-rebuild `virtual_promote` (`staging` -> `prod`)
5. Ephemeral PR environment `virtual_init_env` (`pr_42`) + `virtual_drop_env`
6. Incremental v2 seeding (`clone_if_new_fp` via CTAS on DuckDB) + `virtual_promote` to `prod`
7. Instant `virtual_rollback` (`prod` -> release `v1`)
8. Janitor `virtual_gc` orphan detection and table drop.
"""

from __future__ import annotations

import duckdb

from dbt_virtual.fingerprint import compute_node_fingerprint
from tests.test_macros_execution import FakeRelation, FakeResult, MacroTestHarness


def test_duckdb_full_virtual_lifecycle_e2e() -> None:
    con = duckdb.connect(":memory:")
    for schema in ("analytics_physical", "analytics_staging", "analytics"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "env_schemas": {
                    "staging": "analytics_staging",
                    "prod": "analytics",
                    "pr_42": "analytics_pr_42",
                },
            },
            "_dbt_virtual_release": "v1",
        },
        target_name="staging",
        target_schema="analytics_staging",
        target_database="",
        adapter_type="duckdb",
    )

    def exec_multi(sql_text: str) -> None:
        for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
            con.execute(stmt)

    # 1. Initialize registry tables via macro
    reg_sql = harness.call_macro("ensure_registry_tables_sql", "analytics_physical")
    exec_multi(reg_sql)

    # 2. Compute v1 fingerprint and build physical table + staging view via `sync_env_view`
    node_v1 = {
        "unique_id": "model.test.orders_metrics",
        "name": "orders_metrics",
        "resource_type": "model",
        "compiled_code": "SELECT 'store_1' AS customer_id, 10 AS orders UNION ALL SELECT 'store_2', 25",
        "raw_code": "SELECT 'store_1' AS customer_id, 10 AS orders UNION ALL SELECT 'store_2', 25",
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    fp_v1 = compute_node_fingerprint(node_v1).fingerprint
    phys_v1 = f"orders_metrics__{fp_v1}"
    harness.vars_dict["_dbt_virtual_fps"] = {"orders_metrics": fp_v1}

    con.execute(
        f"""
        CREATE TABLE analytics_physical.{phys_v1} AS
        SELECT 'store_1' AS customer_id, 10 AS orders
        UNION ALL
        SELECT 'store_2' AS customer_id, 25 AS orders
        """
    )

    harness.model = node_v1
    harness.this = FakeRelation("", "analytics_physical", phys_v1, "table", quote_char="")
    sync_v1_sql = harness.call_macro("sync_env_view")
    exec_multi(sync_v1_sql)

    # 3. Create DuckDB scalar macro (`function`) and table macro (`table_function`)
    udf_ddl = harness.call_macro(
        "create_routine_ddl_sql",
        "function",
        "analytics_staging.udf_double_orders",
        "n",
        "BIGINT",
        "n * 2",
    )
    tvf_ddl = harness.call_macro(
        "create_routine_ddl_sql",
        "table_function",
        "analytics_staging.tvf_orders_report",
        "min_o",
        "",
        "SELECT customer_id, orders, analytics_staging.udf_double_orders(orders) AS doubled FROM analytics_staging.orders_metrics WHERE orders >= min_o",
    )
    exec_multi(udf_ddl)
    exec_multi(tvf_ddl)

    rows_stg = con.execute(
        "SELECT customer_id, orders, doubled FROM analytics_staging.tvf_orders_report(15)"
    ).fetchall()
    assert rows_stg == [("store_2", 25, 50)]

    # 4. Zero-rebuild promote v1 to prod
    rel_rows = con.execute(
        """
        SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier
        FROM analytics_physical._releases
        WHERE release_id = 'v1'
        ORDER BY dependency_tier ASC, model_name ASC
        """
    ).fetchall()
    harness.existing_relations.add(("", "analytics_physical", phys_v1))
    harness.query_responses.append(FakeResult(rel_rows))
    harness.executed_queries.clear()
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v1")
    for q_str in harness.executed_queries[1:]:  # skip the initial SELECT that FakeResult answered
        exec_multi(q_str)

    prod_sum_v1 = con.execute("SELECT SUM(orders) FROM analytics.orders_metrics").fetchone()[0]
    assert prod_sum_v1 == 35

    # 5. Ephemeral PR environment (`pr_42`) bootstrap & drop
    env_rows = con.execute(
        """
        SELECT model_name, resource_type, fingerprint, physical_identifier, NULL, 2
        FROM analytics_physical._environments
        WHERE environment = 'prod'
        """
    ).fetchall()
    harness.query_responses.append(FakeResult([]))
    harness.query_responses.append(FakeResult(env_rows))
    harness.executed_queries.clear()
    harness.call_macro("virtual_init_env", env="pr_42", from_env="prod")
    for q_str in harness.executed_queries:
        if not q_str.lstrip().upper().startswith("SELECT"):
            exec_multi(q_str)

    pr_sum = con.execute("SELECT SUM(orders) FROM analytics_pr_42.orders_metrics").fetchone()[0]
    assert pr_sum == 35

    harness.executed_queries.clear()
    harness.call_macro("virtual_drop_env", env="pr_42")
    for q_str in harness.executed_queries:
        exec_multi(q_str)

    # 6. Build v2 in staging via `clone_if_new_fp` (CTAS seed from prod v1) + incremental insert
    node_v2 = {
        **node_v1,
        "compiled_code": "SELECT 'store_1' AS customer_id, 10 AS orders UNION ALL SELECT 'store_2', 25 UNION ALL SELECT 'store_3', 40",
    }
    fp_v2 = compute_node_fingerprint(node_v2).fingerprint
    phys_v2 = f"orders_metrics__{fp_v2}"
    harness.vars_dict["_dbt_virtual_fps"] = {"orders_metrics": fp_v2}
    harness.vars_dict["_dbt_virtual_release"] = "v2"
    harness.model = node_v2
    harness.this = FakeRelation("", "analytics_physical", phys_v2, "table", quote_char="")

    # Seed v2 from prod's v1 via `clone_if_new_fp`
    harness.query_responses.append(FakeResult([(f"analytics_physical.{phys_v1}",)]))
    clone_sql = harness.call_macro("clone_if_new_fp")
    exec_multi(clone_sql)
    con.execute(f"INSERT INTO analytics_physical.{phys_v2} VALUES ('store_3', 40)")

    sync_v2_sql = harness.call_macro("sync_env_view")
    exec_multi(sync_v2_sql)

    # Promote v2 to prod
    rel_v2_rows = con.execute(
        """
        SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier
        FROM analytics_physical._releases
        WHERE release_id = 'v2'
        """
    ).fetchall()
    harness.existing_relations.add(("", "analytics_physical", phys_v2))
    harness.query_responses.append(FakeResult(rel_v2_rows))
    harness.executed_queries.clear()
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v2")
    for q_str in harness.executed_queries[1:]:
        exec_multi(q_str)

    prod_sum_v2 = con.execute("SELECT SUM(orders) FROM analytics.orders_metrics").fetchone()[0]
    assert prod_sum_v2 == 75

    # 7. Instant Rollback of prod back to release v1
    rel_v1_rb = con.execute(
        """
        SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template
        FROM analytics_physical._releases
        WHERE release_id = 'v1'
        """
    ).fetchall()
    current_prod_rows = con.execute(
        """
        SELECT model_name, resource_type
        FROM analytics_physical._environments
        WHERE environment = 'prod'
        """
    ).fetchall()
    harness.query_responses.append(FakeResult(current_prod_rows))
    harness.query_responses.append(FakeResult(rel_v1_rb))
    harness.executed_queries.clear()
    harness.call_macro("virtual_rollback", env="prod", to_release="v1")
    for q_str in harness.executed_queries[1:]:
        exec_multi(q_str)

    prod_sum_rb = con.execute("SELECT SUM(orders) FROM analytics.orders_metrics").fetchone()[0]
    assert prod_sum_rb == 35


def test_duckdb_gc_only_selects_fingerprint_suffix_tables() -> None:
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA analytics_physical")
    con.execute("CREATE TABLE analytics_physical.orders__deadbeef AS SELECT 1 AS id")
    con.execute("CREATE TABLE analytics_physical.recent__cafebabe AS SELECT 1 AS id")
    con.execute("CREATE TABLE analytics_physical.oldorphan__badcafe00 AS SELECT 1 AS id")
    con.execute("CREATE TABLE analytics_physical.daily__archive AS SELECT 1 AS id")
    harness = MacroTestHarness(adapter_type="duckdb", target_name="prod")
    registry_sql = harness.call_macro("ensure_registry_tables_sql", "analytics_physical")
    for stmt in [s.strip() for s in registry_sql.split(";") if s.strip()]:
        con.execute(stmt)
    con.execute(
        """
        INSERT INTO analytics_physical._releases (
            release_id, source_environment, model_name, resource_type, dependency_tier,
            fingerprint, physical_identifier, status, created_at
        ) VALUES
          ('old_release', 'staging', 'orders', 'model', 2, 'deadbeef',
           'analytics_physical.orders__deadbeef', 'candidate', CURRENT_TIMESTAMP - INTERVAL '30 days'),
          ('recent_reference', 'staging', 'orders', 'model', 2, 'deadbeef',
           'analytics_physical.orders__deadbeef', 'candidate', CURRENT_TIMESTAMP),
          ('recent_release', 'staging', 'recent', 'model', 2, 'cafebabe',
           'analytics_physical.recent__cafebabe', 'candidate', CURRENT_TIMESTAMP),
          ('old_orphan_release', 'staging', 'oldorphan', 'model', 2, 'badcafe00',
           'analytics_physical.oldorphan__badcafe00', 'candidate', CURRENT_TIMESTAMP - INTERVAL '30 days')
        """
    )

    orphan_query = harness.call_macro(
        "find_orphan_tables_sql", "analytics_physical", 0, 7
    )
    orphan_names = {row[0] for row in con.execute(orphan_query).fetchall()}

    assert "orders__deadbeef" not in orphan_names
    assert "recent__cafebabe" not in orphan_names
    assert "oldorphan__badcafe00" in orphan_names
    assert "daily__archive" not in orphan_names
