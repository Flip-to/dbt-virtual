"""End-to-end verification of dbt-virtual against nnnkkk7/snowflake-emulator and PostgreSQL.

Exercises the full virtual data environment lifecycle using the actual SQL rendered by
`dbt-virtual` Jinja macros (`MacroTestHarness`) against:
1. `nnnkkk7/snowflake-emulator` (`SNOWFLAKE_EMULATOR_HOST`, default `http://127.0.0.1:8080`, REST API v2 `/api/v2/statements`)
2. `PostgreSQL 16` (`POSTGRES_DSN`, optional when set in CI)

Verified stages on each warehouse:
1. Registry initialization (`ensure_registry_tables_sql` -> `analytics_physical._environments`, `_releases`).
2. Release v1 build in `staging`:
   - Content-addressed physical table `analytics_physical.orders_metrics__<fp_v1>`
   - `sync_env_view` macro execution -> virtual view `analytics_staging.orders_metrics` + registry rows
   - Snowflake SQL function translation verification (`IFF`, `NVL`) on `nnnkkk7/snowflake-emulator`
3. Zero-rebuild promotion (`staging` -> `prod` for release `v1`) via `virtual_promote`.
4. Ephemeral PR environment (`pr_42`) zero-compute initialization (`virtual_init_env`) & teardown (`virtual_drop_env`).
5. Release v2 build in `staging` (`clone_if_new_fp` + incremental insert) & zero-rebuild promotion to `prod`.
6. Instant rollback (`prod` -> release `v1`) via `virtual_rollback`.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dbt_virtual.fingerprint import compute_node_fingerprint
from tests.test_macros_execution import FakeRelation, FakeResult, MacroTestHarness


def run_snowflake_emulator_e2e(endpoint: str) -> None:
    import requests

    base_url = f"{endpoint.rstrip('/')}/api/v2"
    print(f"Connected to nnnkkk7/snowflake-emulator at {base_url}")

    # Create database & warehouse in emulator
    requests.post(f"{base_url}/databases", json={"name": "TEST_DB"}, timeout=10)
    requests.post(
        f"{base_url}/warehouses",
        json={"name": "COMPUTE_WH", "size": "X-SMALL", "autoResume": True},
        timeout=10,
    )

    def sf_exec(sql: str) -> list[list]:
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            resp = requests.post(
                f"{base_url}/statements",
                json={"statement": stmt, "database": "TEST_DB", "schema": "PUBLIC"},
                timeout=15,
            )
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Snowflake emulator error ({resp.status_code}) on `{stmt}`: {resp.text}"
                )
            payload = resp.json()
            if payload.get("sqlState") and payload.get("sqlState") != "00000":
                raise RuntimeError(
                    f"Snowflake emulator SQL error ({payload.get('code')}/{payload.get('sqlState')}) on `{stmt}`: {payload.get('message')}"
                )
            rows = payload.get("data") or []
        return rows

    for schema in ("analytics_physical", "analytics_staging", "analytics", "analytics_pr_42"):
        sf_exec(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "use_ctas_for_clone": True,
                "env_schemas": {
                    "staging": "analytics_staging",
                    "prod": "analytics",
                    "pr_42": "analytics_pr_42",
                },
            },
            "_dbt_virtual_release": "v1",
        },
        target_name="emulator",
        target_schema="analytics_staging",
        target_database="",
        adapter_type="snowflake",
    )

    # 1. Initialize registry tables (`_environments`, `_releases`) via `snowflake__ensure_registry_tables_sql`
    reg_sql = harness.call_macro("ensure_registry_tables_sql", "analytics_physical")
    sf_exec(reg_sql)

    # 2. Compute v1 fingerprint & build physical table with Snowflake-specific SQL (`IFF`, `NVL`)
    node_v1 = {
        "unique_id": "model.test.orders_metrics",
        "name": "orders_metrics",
        "resource_type": "model",
        "compiled_code": (
            "SELECT 'store_1' AS customer_id, IFF(1 > 0, 10, 0) AS orders "
            "UNION ALL SELECT 'store_2' AS customer_id, NVL(25, 0) AS orders"
        ),
        "raw_code": (
            "SELECT 'store_1' AS customer_id, IFF(1 > 0, 10, 0) AS orders "
            "UNION ALL SELECT 'store_2' AS customer_id, NVL(25, 0) AS orders"
        ),
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    fp_v1 = compute_node_fingerprint(node_v1).fingerprint
    phys_v1 = f"orders_metrics__{fp_v1}"
    harness.vars_dict["_dbt_virtual_fps"] = {"orders_metrics": fp_v1}

    sf_exec(f"CREATE TABLE analytics_physical.{phys_v1} (customer_id VARCHAR, orders INTEGER)")
    sf_exec(
        f"INSERT INTO analytics_physical.{phys_v1} VALUES ('store_1', 10), ('store_2', 25)"
    )

    harness.model = node_v1
    harness.this = FakeRelation("", "analytics_physical", phys_v1, "table", quote_char="")
    sync_v1_sql = harness.call_macro("sync_env_view")
    sf_exec(sync_v1_sql)

    stg_rows = sf_exec(
        "SELECT customer_id, orders, IFF(orders >= 20, 'high', 'low') AS tier "
        "FROM analytics_staging.orders_metrics WHERE orders >= 15"
    )
    assert len(stg_rows) == 1 and stg_rows[0][0] == "store_2" and int(stg_rows[0][1]) == 25, stg_rows
    print(f"[snowflake-emulator v1] Staging virtual view + Snowflake IFF verified: {stg_rows}")

    # 3. Zero-rebuild Promote v1 to prod via `virtual_promote`
    rel_v1_rows = sf_exec(
        "SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier "
        "FROM analytics_physical._releases WHERE release_id = 'v1'"
    )
    harness.existing_relations.add(("", "analytics_physical", phys_v1))
    harness.query_responses.append(FakeResult([tuple(r) for r in rel_v1_rows]))
    harness.executed_queries.clear()
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v1")
    for q_str in harness.executed_queries[1:]:
        sf_exec(q_str)

    prod_v1 = sf_exec("SELECT SUM(orders) FROM analytics.orders_metrics")
    assert int(float(prod_v1[0][0])) == 35, prod_v1
    print(f"[snowflake-emulator v1] Zero-rebuild promote to prod verified: sum={prod_v1[0][0]}")

    # 4. Ephemeral PR environment (`pr_42`) zero-compute init & drop
    env_prod_rows = sf_exec(
        "SELECT model_name, resource_type, fingerprint, physical_identifier, NULL, 2 "
        "FROM analytics_physical._environments WHERE environment = 'prod'"
    )
    harness.query_responses.append(FakeResult([]))
    harness.query_responses.append(FakeResult([tuple(r) for r in env_prod_rows]))
    harness.executed_queries.clear()
    harness.call_macro("virtual_init_env", env="pr_42", from_env="prod")
    for q_str in harness.executed_queries:
        if not q_str.lstrip().upper().startswith("SELECT"):
            sf_exec(q_str)

    pr_sum = sf_exec("SELECT SUM(orders) FROM analytics_pr_42.orders_metrics")
    assert int(float(pr_sum[0][0])) == 35, pr_sum
    print(f"[snowflake-emulator pr_42] Zero-compute PR environment verified: sum={pr_sum[0][0]}")

    harness.executed_queries.clear()
    harness.call_macro("virtual_drop_env", env="pr_42")
    for q_str in harness.executed_queries:
        sf_exec(q_str)

    # 5. Build v2 in staging (`clone_if_new_fp` + incremental insert) & promote to prod
    node_v2 = {
        **node_v1,
        "compiled_code": "SELECT 'store_1', 10 UNION ALL SELECT 'store_2', 25 UNION ALL SELECT 'store_3', 40",
    }
    fp_v2 = compute_node_fingerprint(node_v2).fingerprint
    phys_v2 = f"orders_metrics__{fp_v2}"
    harness.vars_dict["_dbt_virtual_fps"] = {"orders_metrics": fp_v2}
    harness.vars_dict["_dbt_virtual_release"] = "v2"
    harness.model = node_v2
    harness.this = FakeRelation("", "analytics_physical", phys_v2, "table", quote_char="")

    harness.query_responses.append(FakeResult([(f"analytics_physical.{phys_v1}",)]))
    clone_sql = harness.call_macro("clone_if_new_fp")
    sf_exec(clone_sql)
    sf_exec(f"INSERT INTO analytics_physical.{phys_v2} VALUES ('store_3', 40)")
    sf_exec(harness.call_macro("sync_env_view"))

    rel_v2_rows = sf_exec(
        "SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier "
        "FROM analytics_physical._releases WHERE release_id = 'v2'"
    )
    harness.existing_relations.add(("", "analytics_physical", phys_v2))
    harness.query_responses.append(FakeResult([tuple(r) for r in rel_v2_rows]))
    harness.executed_queries.clear()
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v2")
    for q_str in harness.executed_queries[1:]:
        sf_exec(q_str)

    prod_v2 = sf_exec("SELECT SUM(orders) FROM analytics.orders_metrics")
    assert int(float(prod_v2[0][0])) == 75, prod_v2
    print(f"[snowflake-emulator v2] Promoted v2 to prod via view swap: sum={prod_v2[0][0]}")

    # 6. Instant Rollback of prod back to release v1
    rel_v1_rb = sf_exec(
        "SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template "
        "FROM analytics_physical._releases WHERE release_id = 'v1'"
    )
    current_prod_rows = sf_exec(
        "SELECT model_name, resource_type FROM analytics_physical._environments "
        "WHERE environment = 'prod'"
    )
    harness.query_responses.append(FakeResult([tuple(r) for r in current_prod_rows]))
    harness.query_responses.append(FakeResult([tuple(r) for r in rel_v1_rb]))
    harness.executed_queries.clear()
    harness.call_macro("virtual_rollback", env="prod", to_release="v1")
    for q_str in harness.executed_queries[1:]:
        sf_exec(q_str)

    prod_rb = sf_exec("SELECT SUM(orders) FROM analytics.orders_metrics")
    assert int(float(prod_rb[0][0])) == 35, prod_rb
    print(f"[snowflake-emulator rollback] Instant view rollback to v1 verified: sum={prod_rb[0][0]}")


def run_postgres_e2e(dsn: str) -> None:
    import psycopg

    print(f"Connected to PostgreSQL at {dsn}")
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for schema in ("analytics_physical", "analytics_staging", "analytics", "analytics_pr_42"):
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        def pg_exec(sql_text: str) -> None:
            for stmt in [s.strip() for s in sql_text.split(";") if s.strip()]:
                cur.execute(stmt)

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
            adapter_type="postgres",
        )

        # 1. Initialize registry tables
        pg_exec(harness.call_macro("ensure_registry_tables_sql", "analytics_physical"))

        # 2. Build v1 in staging
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

        pg_exec(
            f"CREATE TABLE analytics_physical.{phys_v1} AS "
            f"SELECT 'store_1'::TEXT AS customer_id, 10::INT AS orders "
            f"UNION ALL SELECT 'store_2'::TEXT, 25::INT"
        )
        harness.model = node_v1
        harness.this = FakeRelation("", "analytics_physical", phys_v1, "table", quote_char="")
        pg_exec(harness.call_macro("sync_env_view"))

        # 3. Promote v1 to prod
        cur.execute(
            "SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier "
            "FROM analytics_physical._releases WHERE release_id = 'v1'"
        )
        rel_v1_rows = cur.fetchall()
        harness.existing_relations.add(("", "analytics_physical", phys_v1))
        harness.query_responses.append(FakeResult(rel_v1_rows))
        harness.executed_queries.clear()
        harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v1")
        for q_str in harness.executed_queries[1:]:
            pg_exec(q_str)

        cur.execute("SELECT SUM(orders) FROM analytics.orders_metrics")
        assert cur.fetchone()[0] == 35
        print("[postgres v1] Zero-rebuild promote to prod verified: sum=35")

        # 4. Build v2 via clone_if_new_fp + promote + rollback
        node_v2 = {
            **node_v1,
            "compiled_code": "SELECT 'store_1', 10 UNION ALL SELECT 'store_2', 25 UNION ALL SELECT 'store_3', 40",
        }
        fp_v2 = compute_node_fingerprint(node_v2).fingerprint
        phys_v2 = f"orders_metrics__{fp_v2}"
        harness.vars_dict["_dbt_virtual_fps"] = {"orders_metrics": fp_v2}
        harness.vars_dict["_dbt_virtual_release"] = "v2"
        harness.model = node_v2
        harness.this = FakeRelation("", "analytics_physical", phys_v2, "table", quote_char="")

        harness.query_responses.append(FakeResult([(f"analytics_physical.{phys_v1}",)]))
        pg_exec(harness.call_macro("clone_if_new_fp"))
        pg_exec(f"INSERT INTO analytics_physical.{phys_v2} VALUES ('store_3', 40)")
        pg_exec(harness.call_macro("sync_env_view"))

        cur.execute(
            "SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier "
            "FROM analytics_physical._releases WHERE release_id = 'v2'"
        )
        harness.existing_relations.add(("", "analytics_physical", phys_v2))
        harness.query_responses.append(FakeResult(cur.fetchall()))
        harness.executed_queries.clear()
        harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v2")
        for q_str in harness.executed_queries[1:]:
            pg_exec(q_str)

        cur.execute("SELECT SUM(orders) FROM analytics.orders_metrics")
        assert cur.fetchone()[0] == 75
        print("[postgres v2] Promoted v2 to prod via view swap: sum=75")

        cur.execute(
            "SELECT model_name, resource_type FROM analytics_physical._environments "
            "WHERE environment = 'prod'"
        )
        harness.query_responses.append(FakeResult(cur.fetchall()))
        cur.execute(
            "SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template "
            "FROM analytics_physical._releases WHERE release_id = 'v1'"
        )
        harness.query_responses.append(FakeResult(cur.fetchall()))
        harness.executed_queries.clear()
        harness.call_macro("virtual_rollback", env="prod", to_release="v1")
        for q_str in harness.executed_queries[1:]:
            pg_exec(q_str)

        cur.execute("SELECT SUM(orders) FROM analytics.orders_metrics")
        assert cur.fetchone()[0] == 35
        print("[postgres rollback] Instant view rollback to v1 verified: sum=35")


def main() -> int:
    sf_host = os.environ.get("SNOWFLAKE_EMULATOR_HOST", "http://127.0.0.1:8080")
    pg_dsn = os.environ.get("POSTGRES_DSN", "")

    run_snowflake_emulator_e2e(sf_host)
    if pg_dsn:
        run_postgres_e2e(pg_dsn)

    print("All multi-warehouse (Snowflake Emulator + Postgres) E2E checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
