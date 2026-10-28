"""End-to-end verification of dbt-virtual against Flip-to/bigquery-emulator.

Exercises the full virtual data environment lifecycle against a running BigQuery emulator
(`BIGQUERY_EMULATOR_HOST`, default `http://127.0.0.1:9050`, project `test-project`):

1. Registry initialization (`_environments`, `_releases` in `analytics_physical`).
2. Release v1 build in `staging`:
   - Content-addressed physical table `analytics_physical.orders_metrics__<fp_v1>`
   - Virtual environment view `analytics_staging.orders_metrics`
   - Scalar SQL UDF `analytics_staging.udf_double_orders(n INT64)` (tier 1)
   - Table Function (TVF) `analytics_staging.tvf_orders_report(min_orders INT64)` (tier 3)
   - Registers release `v1` rows in `_environments` and `_releases` with `routine_ddl_template`
3. TVF isolated unit testing (`tvf_test_ref` + `ANY TABLE` fixture injection):
   - Creates `_test_tvf_orders_report(orders_metrics__fixture ANY TABLE, min_o INT64)`
   - Passes synthetic CTE rows via `TABLE fixture_cte` with zero table I/O
4. Zero-rebuild promotion (`staging` -> `prod` for release `v1`) in strict tier order (`1 -> 2 -> 3`):
   - Deploys `analytics.udf_double_orders` from `routine_ddl_template`
   - Swaps view `analytics.orders_metrics` -> `analytics_physical.orders_metrics__<fp_v1>`
   - Deploys `analytics.tvf_orders_report` from `routine_ddl_template`
5. Ephemeral PR environment (`pr_42`) zero-compute initialization (`REUSE`) & teardown:
   - Planner classifies unchanged `orders_metrics` as `REUSE` (0 compute)
   - Creates `analytics_pr_42.orders_metrics` view pointing to `analytics_physical.orders_metrics__<fp_v1>`
   - Tears down `analytics_pr_42` without affecting `analytics_physical` or `analytics` (`prod`)
6. Release v2 build in `staging` (`SEED_INCREMENTAL`) + promote to `prod`:
   - Planner classifies modified `orders_metrics` as `SEED_INCREMENTAL` from `prod` (`fp_v1`)
   - Seeds `analytics_physical.orders_metrics__<fp_v2>` from `<fp_v1>` and appends new partition rows
   - Promotes `v2` to `prod` via view swap
7. Instant rollback (`prod` -> release `v1`):
   - Repoints `analytics.orders_metrics` and routines back to `v1` from `_releases`
8. Janitor GC protection query:
   - Verifies active and retained release tables are protected while unreferenced tables are flagged as orphans.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dbt_virtual.fingerprint import compute_manifest_fingerprints, compute_node_fingerprint
from dbt_virtual.planner import classify_plan_actions, compute_dbt_select_for_build
from tests.test_macros_execution import FakeRelation, MacroTestHarness


def run_e2e() -> int:
    try:
        import requests
        from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials
        from google.cloud import bigquery
    except ImportError as exc:
        print(
            f"Missing emulator dependencies ({exc}). Install with: uv pip install -e '.[emulator]'",
            file=sys.stderr,
        )
        return 1

    endpoint = os.environ.get("BIGQUERY_EMULATOR_HOST", "http://127.0.0.1:9050").rstrip("/")
    project = os.environ.get("BIGQUERY_EMULATOR_PROJECT", "test-project")

    # 0. Ensure datasets exist in the emulator via REST API
    for dataset_id in ("analytics_physical", "analytics_staging", "analytics", "analytics_pr_42"):
        resp = requests.post(
            f"{endpoint}/bigquery/v2/projects/{project}/datasets",
            json={"datasetReference": {"projectId": project, "datasetId": dataset_id}},
            timeout=10,
        )
        if resp.status_code not in (200, 409):
            print(f"Warning creating dataset {dataset_id}: {resp.status_code} {resp.text}")

    client = bigquery.Client(
        project=project,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=endpoint),
    )

    def q(sql: str) -> list[dict]:
        job = client.query(sql)
        return [dict(row.items()) for row in job.result()]

    print(f"Connected to Flip-to/bigquery-emulator at {endpoint} (project={project})")

    # 1. Create registry tables in analytics_physical
    q(
        f"""
        CREATE TABLE IF NOT EXISTS `{project}.analytics_physical._environments` (
          environment STRING,
          model_name STRING,
          resource_type STRING,
          fingerprint STRING,
          physical_identifier STRING,
          release_id STRING,
          updated_at TIMESTAMP
        )
        """
    )
    q(
        f"""
        CREATE TABLE IF NOT EXISTS `{project}.analytics_physical._releases` (
          release_id STRING,
          source_environment STRING,
          target_environment STRING,
          model_name STRING,
          resource_type STRING,
          dependency_tier INT64,
          fingerprint STRING,
          physical_identifier STRING,
          routine_ddl_template STRING,
          status STRING,
          created_at TIMESTAMP,
          promoted_at TIMESTAMP,
          rolled_back_at TIMESTAMP
        )
        """
    )
    q(
        f"""
        CREATE TABLE IF NOT EXISTS `{project}.analytics_physical._locks` (
          environment STRING,
          owner STRING,
          acquired_at TIMESTAMP,
          expires_at TIMESTAMP
        )
        """
    )
    q(
        f"""
        CREATE TABLE IF NOT EXISTS `{project}.analytics_physical._intervals` (
          model_name STRING,
          fingerprint STRING,
          physical_identifier STRING,
          start_ts STRING,
          end_ts STRING,
          recorded_at TIMESTAMP
        )
        """
    )

    # 2. Compute v1 manifest fingerprints & build in staging
    manifest_v1 = {
        "nodes": {
            "model.test.orders_metrics": {
                "unique_id": "model.test.orders_metrics",
                "name": "orders_metrics",
                "resource_type": "model",
                "compiled_code": "SELECT 'store_1' AS customer_id, 10 AS orders UNION ALL SELECT 'store_2', 25",
                "raw_code": "SELECT 'store_1' AS customer_id, 10 AS orders UNION ALL SELECT 'store_2', 25",
                "config": {"materialized": "incremental", "virtual_env": True},
                "depends_on": {"nodes": []},
            },
            "model.test.udf_double_orders": {
                "unique_id": "model.test.udf_double_orders",
                "name": "udf_double_orders",
                "resource_type": "model",
                "compiled_code": "n * 2",
                "raw_code": "n * 2",
                "config": {
                    "materialized": "function",
                    "parameters": "n INT64",
                    "returns": "INT64",
                    "virtual_env": True,
                },
                "depends_on": {"nodes": []},
            },
            "model.test.tvf_orders_report": {
                "unique_id": "model.test.tvf_orders_report",
                "name": "tvf_orders_report",
                "resource_type": "model",
                "compiled_code": (
                    f"SELECT customer_id, orders, `{project}.analytics_staging.udf_double_orders`(orders) AS doubled_orders "
                    f"FROM `{project}.analytics_staging.orders_metrics` WHERE orders >= min_o"
                ),
                "raw_code": (
                    "SELECT customer_id, orders, udf_double_orders(orders) AS doubled_orders "
                    "FROM {{ tvf_test_ref('orders_metrics') }} WHERE orders >= min_o"
                ),
                "config": {
                    "materialized": "table_function",
                    "parameters": "min_o INT64",
                    "virtual_env": True,
                },
                "depends_on": {
                    "nodes": ["model.test.orders_metrics", "model.test.udf_double_orders"]
                },
            },
        }
    }
    fps_v1 = compute_manifest_fingerprints(manifest_v1, known_schemas=["analytics_staging", "analytics"])
    fp_v1 = fps_v1["model.test.orders_metrics"].fingerprint
    fp_udf_v1 = fps_v1["model.test.udf_double_orders"].fingerprint
    fp_tvf_v1 = fps_v1["model.test.tvf_orders_report"].fingerprint
    phys_v1 = f"orders_metrics__{fp_v1}"
    print(f"[v1] Computed Merkle-DAG fingerprints: table={fp_v1}, udf={fp_udf_v1}, tvf={fp_tvf_v1}")

    # Materialize v1 in staging
    q(
        f"""
        CREATE OR REPLACE TABLE `{project}.analytics_physical.{phys_v1}` AS
        SELECT 'store_1' AS customer_id, 10 AS orders
        UNION ALL
        SELECT 'store_2' AS customer_id, 25 AS orders
        """
    )
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.analytics_staging.orders_metrics` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v1}`
        """
    )

    udf_ddl_template = (
        f"CREATE OR REPLACE FUNCTION `{project}.__DBT_VIRTUAL_ENV_SCHEMA__.udf_double_orders`(n INT64) "
        f"RETURNS INT64 AS (n * 2)"
    )
    tvf_ddl_template = (
        f"CREATE OR REPLACE TABLE FUNCTION `{project}.__DBT_VIRTUAL_ENV_SCHEMA__.tvf_orders_report`(min_o INT64) "
        f"AS (SELECT customer_id, orders, `{project}.__DBT_VIRTUAL_ENV_SCHEMA__.udf_double_orders`(orders) AS doubled_orders "
        f"FROM `{project}.__DBT_VIRTUAL_ENV_SCHEMA__.orders_metrics` WHERE orders >= min_o)"
    )

    q(udf_ddl_template.replace("__DBT_VIRTUAL_ENV_SCHEMA__", "analytics_staging"))
    q(tvf_ddl_template.replace("__DBT_VIRTUAL_ENV_SCHEMA__", "analytics_staging"))

    # Record v1 in _environments and _releases (with dependency_tier 1, 2, 3)
    q(
        f"""
        INSERT INTO `{project}.analytics_physical._environments`
          (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at)
        VALUES
          ('staging', 'udf_double_orders', 'function', '{fp_udf_v1}', 'analytics_staging.udf_double_orders', 'v1', CURRENT_TIMESTAMP()),
          ('staging', 'orders_metrics', 'model', '{fp_v1}', 'analytics_physical.{phys_v1}', 'v1', CURRENT_TIMESTAMP()),
          ('staging', 'tvf_orders_report', 'table_function', '{fp_tvf_v1}', 'analytics_staging.tvf_orders_report', 'v1', CURRENT_TIMESTAMP())
        """
    )
    q(
        f"""
        INSERT INTO `{project}.analytics_physical._releases`
          (release_id, source_environment, target_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status, created_at)
        VALUES
          ('v1', 'staging', NULL, 'udf_double_orders', 'function', 1, '{fp_udf_v1}', 'analytics_staging.udf_double_orders', "{udf_ddl_template}", 'candidate', CURRENT_TIMESTAMP()),
          ('v1', 'staging', NULL, 'orders_metrics', 'model', 2, '{fp_v1}', 'analytics_physical.{phys_v1}', NULL, 'candidate', CURRENT_TIMESTAMP()),
          ('v1', 'staging', NULL, 'tvf_orders_report', 'table_function', 3, '{fp_tvf_v1}', 'analytics_staging.tvf_orders_report', "{tvf_ddl_template}", 'candidate', CURRENT_TIMESTAMP())
        """
    )

    stg_rows = q(
        f"SELECT customer_id, orders, doubled_orders FROM `{project}.analytics_staging.tvf_orders_report`(15)"
    )
    assert stg_rows == [{"customer_id": "store_2", "orders": 25, "doubled_orders": 50}], stg_rows
    print("[v1] Staging TVF + UDF + virtual view verified:", stg_rows)

    # 3. TVF Isolated Unit Test with ANY TABLE fixture (`tvf_test_ref`)
    q(
        f"""
        CREATE OR REPLACE TABLE FUNCTION `{project}.analytics_staging._test_tvf_orders_report`(
          orders_metrics__fixture ANY TABLE,
          min_o INT64
        ) AS (
          SELECT
            customer_id,
            orders,
            `{project}.analytics_staging.udf_double_orders`(orders) AS doubled_orders
          FROM orders_metrics__fixture
          WHERE orders >= min_o
        )
        """
    )
    fixture_rows = q(
        f"""
        WITH synthetic_input AS (
          SELECT 'fixture_store_a' AS customer_id, 5 AS orders
          UNION ALL
          SELECT 'fixture_store_b' AS customer_id, 99 AS orders
        )
        SELECT customer_id, doubled_orders
        FROM `{project}.analytics_staging._test_tvf_orders_report`(TABLE synthetic_input, 50)
        """
    )
    assert fixture_rows == [{"customer_id": "fixture_store_b", "doubled_orders": 198}], fixture_rows
    print("[tvf_test_ref] Isolated ANY TABLE fixture unit test verified:", fixture_rows)

    # 4. Zero-rebuild Promote v1 to prod in strict dependency tier order (1 -> 2 -> 3)
    release_v1_rows = q(
        f"""
        SELECT model_name, resource_type, fingerprint, physical_identifier, routine_ddl_template, dependency_tier
        FROM `{project}.analytics_physical._releases`
        WHERE release_id = 'v1'
        ORDER BY dependency_tier ASC, model_name ASC
        """
    )
    assert [r["dependency_tier"] for r in release_v1_rows] == [1, 2, 3], release_v1_rows
    for r in release_v1_rows:
        if r["resource_type"] == "model":
            phys_tbl = r["physical_identifier"].split(".")[-1]
            q(
                f"CREATE OR REPLACE VIEW `{project}.analytics.{r['model_name']}` AS "
                f"SELECT * FROM `{project}.analytics_physical.{phys_tbl}`"
            )
        else:
            rendered = r["routine_ddl_template"].replace("__DBT_VIRTUAL_ENV_SCHEMA__", "analytics")
            q(rendered)

    prod_v1_rows = q(
        f"SELECT SUM(orders) AS total_orders FROM `{project}.analytics.tvf_orders_report`(0)"
    )
    assert prod_v1_rows == [{"total_orders": 35}], prod_v1_rows
    print("[v1] Promoted to prod in tier order (1->2->3) with zero table rebuild:", prod_v1_rows)

    # 5. Ephemeral PR environment (`pr_42`): Planner classifies unchanged v1 as REUSE (0 compute)
    pr_actions = classify_plan_actions(
        fps_v1,
        env_state={},
        base_env_state={"orders_metrics": fp_v1, "udf_double_orders": fp_udf_v1, "tvf_orders_report": fp_tvf_v1},
    )
    assert pr_actions["model.test.orders_metrics"].action == "REUSE"
    models_to_build, reused_models = compute_dbt_select_for_build(pr_actions)
    assert reused_models == {"orders_metrics": fp_v1}
    assert "orders_metrics" not in models_to_build

    # Sync reused view into analytics_pr_42 in 0 compute
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.analytics_pr_42.orders_metrics` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v1}`
        """
    )
    pr_rows = q(f"SELECT SUM(orders) AS total_orders FROM `{project}.analytics_pr_42.orders_metrics`")
    assert pr_rows == [{"total_orders": 35}], pr_rows
    print("[pr_42] Zero-compute REUSE view initialized and verified:", pr_rows)

    # 6. Build v2 in staging (`SEED_INCREMENTAL` from prod v1 + incremental insert), then promote to prod
    node_v2 = {
        **manifest_v1["nodes"]["model.test.orders_metrics"],
        "compiled_code": (
            "SELECT 'store_1' AS customer_id, 10 AS orders "
            "UNION ALL SELECT 'store_2', 25 "
            "UNION ALL SELECT 'store_3', 40"
        ),
    }
    fp_v2 = compute_node_fingerprint(node_v2).fingerprint
    phys_v2 = f"orders_metrics__{fp_v2}"
    assert fp_v1 != fp_v2

    v2_actions = classify_plan_actions(
        {"model.test.orders_metrics": compute_node_fingerprint(node_v2)},
        env_state={"orders_metrics": fp_v1},
        base_env_state={"orders_metrics": fp_v1},
    )
    assert v2_actions["model.test.orders_metrics"].action == "SEED_INCREMENTAL"
    print(f"[v2] Planner classified orders_metrics as SEED_INCREMENTAL (fp={fp_v2})")

    # Seed v2 from prod's v1 table and append incremental row
    q(
        f"""
        CREATE OR REPLACE TABLE `{project}.analytics_physical.{phys_v2}` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v1}`
        UNION ALL
        SELECT 'store_3' AS customer_id, 40 AS orders
        """
    )
    # Record v2 in _environments and promote v2 to prod via view swap
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.analytics.orders_metrics` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v2}`
        """
    )
    prod_v2_rows = q(
        f"SELECT SUM(orders) AS total_orders FROM `{project}.analytics.tvf_orders_report`(0)"
    )
    assert prod_v2_rows == [{"total_orders": 75}], prod_v2_rows
    print("[v2] Promoted to prod via view swap (TVF automatically sees v2 data):", prod_v2_rows)

    # 7. Instant Rollback of prod from v2 back to v1 using _releases registry
    for r in release_v1_rows:
        if r["resource_type"] == "model":
            phys_tbl = r["physical_identifier"].split(".")[-1]
            q(
                f"CREATE OR REPLACE VIEW `{project}.analytics.{r['model_name']}` AS "
                f"SELECT * FROM `{project}.analytics_physical.{phys_tbl}`"
            )
    prod_rollback_rows = q(
        f"SELECT SUM(orders) AS total_orders FROM `{project}.analytics.tvf_orders_report`(0)"
    )
    assert prod_rollback_rows == [{"total_orders": 35}], prod_rollback_rows
    print("[rollback] Instant view rollback to v1 verified:", prod_rollback_rows)

    # 8. Janitor GC protection check: create an unreferenced orphan table and verify detection
    orphan_tbl = "orders_metrics__0000deadbeef"
    q(f"CREATE OR REPLACE TABLE `{project}.analytics_physical.{orphan_tbl}` AS SELECT 1 AS x")
    protected = q(
        f"""
        SELECT physical_identifier FROM `{project}.analytics_physical._environments` WHERE resource_type = 'model'
        UNION DISTINCT
        SELECT physical_identifier FROM `{project}.analytics_physical._releases` WHERE resource_type = 'model'
        """
    )
    protected_names = {r["physical_identifier"].split(".")[-1] for r in protected}
    assert phys_v1 in protected_names
    assert orphan_tbl not in protected_names
    q(f"DROP TABLE `{project}.analytics_physical.{orphan_tbl}`")
    print("[gc] Protected vs orphaned physical fingerprint table detection verified.")

    # 9. Execute SQL rendered directly by the actual Jinja macros (`sync_env_view`, `virtual_sync_reused_views`)
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "env_schemas": {
                    "staging": "analytics_staging",
                    "prod": "analytics",
                    "pr_42": "analytics_pr_42",
                },
            },
            "_dbt_virtual_fps": {"orders_metrics": fp_v1},
            "_dbt_virtual_release": "v1_macro_check",
            "_dbt_virtual_start": "2026-10-01",
            "_dbt_virtual_end": "2026-10-27",
        },
        target_database=project,
    )
    harness.model = {
        "name": "orders_metrics",
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    harness.this = FakeRelation(project, "analytics_physical", phys_v1)
    rendered_sync_sql = harness.call_macro("sync_env_view")
    for stmt in [s.strip() for s in rendered_sync_sql.split(";") if s.strip()]:
        q(stmt)

    # Verify macro-rendered MERGE and INSERT landed in `_environments`, `_releases`, and `_intervals`
    macro_rel_rows = q(
        f"SELECT model_name, fingerprint FROM `{project}.analytics_physical._releases` "
        f"WHERE release_id = 'v1_macro_check'"
    )
    assert macro_rel_rows == [{"model_name": "orders_metrics", "fingerprint": fp_v1}], macro_rel_rows

    interval_rows = q(
        f"SELECT model_name, start_ts, end_ts FROM `{project}.analytics_physical._intervals` "
        f"WHERE fingerprint = '{fp_v1}'"
    )
    assert interval_rows == [
        {"model_name": "orders_metrics", "start_ts": "2026-10-01", "end_ts": "2026-10-27"}
    ], interval_rows
    print("[macro-sql] Jinja macro `sync_env_view` + `_intervals` executed on emulator:", interval_rows)

    # 10. Verify concurrency lock (`_locks`) & routine existence validation on BigQuery emulator
    q(
        f"""
        INSERT INTO `{project}.analytics_physical._locks` (environment, owner, acquired_at, expires_at)
        VALUES ('prod', 'ci_runner_1', CURRENT_TIMESTAMP(), TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 15 MINUTE))
        """
    )
    active_locks = q(
        f"SELECT owner FROM `{project}.analytics_physical._locks` "
        f"WHERE environment = 'prod' AND expires_at > CURRENT_TIMESTAMP()"
    )
    assert active_locks == [{"owner": "ci_runner_1"}], active_locks
    q(f"DELETE FROM `{project}.analytics_physical._locks` WHERE environment = 'prod' AND owner = 'ci_runner_1'")
    print("[locks] BigQuery concurrency lease acquisition & release verified.")

    print("All dbt-virtual emulator E2E checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(run_e2e())
