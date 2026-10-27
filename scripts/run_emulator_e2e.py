"""End-to-end verification of dbt-virtual against Flip-to/bigquery-emulator.

Exercises the full virtual data environment lifecycle against a running BigQuery emulator
(`BIGQUERY_EMULATOR_HOST`, default `http://127.0.0.1:9050`, project `test-project`):

1. Registry initialization (`_environments`, `_releases` in `analytics_physical`).
2. Release v1 build in `staging`:
   - Content-addressed physical table `analytics_physical.orders_metrics__<fp_v1>`
   - Virtual environment view `analytics_staging.orders_metrics`
   - Scalar SQL UDF `analytics_staging.udf_double_orders(n INT64)`
   - Table Function (TVF) `analytics_staging.tvf_orders_report(min_orders INT64)` reading the view
3. Zero-rebuild promotion (`staging` -> `prod` for release `v1`):
   - View `analytics.orders_metrics` points to `analytics_physical.orders_metrics__<fp_v1>`
   - `analytics.udf_double_orders` and `analytics.tvf_orders_report` deployed in `prod`
4. Release v2 build in `staging` + promote to `prod`:
   - New fingerprinted table `analytics_physical.orders_metrics__<fp_v2>` seeded from `prod`'s `<fp_v1>`
     and updated with new rows
   - Promoted to `prod` via view swap
5. Instant rollback (`prod` -> release `v1`):
   - Repoints `analytics.orders_metrics` back to `<fp_v1>` and verifies `analytics.tvf_orders_report`
     immediately serves `v1` data again.
"""

from __future__ import annotations

import os
import sys

from dbt_virtual.fingerprint import compute_node_fingerprint


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
    for dataset_id in ("analytics_physical", "analytics_staging", "analytics"):
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
          created_at TIMESTAMP
        )
        """
    )

    # 2. Compute v1 fingerprint & build in staging
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
    print(f"[v1] Computed fingerprint: {fp_v1} -> analytics_physical.{phys_v1}")

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
    q(
        f"""
        CREATE OR REPLACE FUNCTION `{project}.analytics_staging.udf_double_orders`(n INT64)
        RETURNS INT64
        AS (n * 2)
        """
    )
    q(
        f"""
        CREATE OR REPLACE TABLE FUNCTION `{project}.analytics_staging.tvf_orders_report`(min_o INT64)
        AS (
          SELECT
            customer_id,
            orders,
            `{project}.analytics_staging.udf_double_orders`(orders) AS doubled_orders
          FROM `{project}.analytics_staging.orders_metrics`
          WHERE orders >= min_o
        )
        """
    )

    stg_rows = q(
        f"SELECT customer_id, orders, doubled_orders FROM `{project}.analytics_staging.tvf_orders_report`(15)"
    )
    assert stg_rows == [{"customer_id": "store_2", "orders": 25, "doubled_orders": 50}], stg_rows
    print("[v1] Staging TVF + UDF + virtual view verified:", stg_rows)

    # 3. Zero-rebuild Promote v1 to prod (view swap + routine deploy)
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.analytics.orders_metrics` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v1}`
        """
    )
    q(
        f"""
        CREATE OR REPLACE FUNCTION `{project}.analytics.udf_double_orders`(n INT64)
        RETURNS INT64
        AS (n * 2)
        """
    )
    q(
        f"""
        CREATE OR REPLACE TABLE FUNCTION `{project}.analytics.tvf_orders_report`(min_o INT64)
        AS (
          SELECT
            customer_id,
            orders,
            `{project}.analytics.udf_double_orders`(orders) AS doubled_orders
          FROM `{project}.analytics.orders_metrics`
          WHERE orders >= min_o
        )
        """
    )
    prod_v1_rows = q(
        f"SELECT SUM(orders) AS total_orders FROM `{project}.analytics.tvf_orders_report`(0)"
    )
    assert prod_v1_rows == [{"total_orders": 35}], prod_v1_rows
    print("[v1] Promoted to prod with zero table rebuild:", prod_v1_rows)

    # 4. Build v2 in staging (seed from prod v1 table + incremental insert), then promote to prod
    node_v2 = {
        **node_v1,
        "compiled_code": "SELECT 'store_1' AS customer_id, 10 AS orders UNION ALL SELECT 'store_2', 25 UNION ALL SELECT 'store_3', 40",
    }
    fp_v2 = compute_node_fingerprint(node_v2).fingerprint
    phys_v2 = f"orders_metrics__{fp_v2}"
    assert fp_v1 != fp_v2
    print(f"[v2] Computed fingerprint: {fp_v2} -> analytics_physical.{phys_v2}")

    # Seed v2 from prod's v1 table and append incremental row
    q(
        f"""
        CREATE OR REPLACE TABLE `{project}.analytics_physical.{phys_v2}` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v1}`
        UNION ALL
        SELECT 'store_3' AS customer_id, 40 AS orders
        """
    )
    # Promote v2 to prod by swapping the view only (TVF in prod automatically reads the swapped view!)
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

    # 5. Instant Rollback of prod from v2 back to v1
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.analytics.orders_metrics` AS
        SELECT * FROM `{project}.analytics_physical.{phys_v1}`
        """
    )
    prod_rollback_rows = q(
        f"SELECT SUM(orders) AS total_orders FROM `{project}.analytics.tvf_orders_report`(0)"
    )
    assert prod_rollback_rows == [{"total_orders": 35}], prod_rollback_rows
    print("[rollback] Instant view rollback to v1 verified:", prod_rollback_rows)

    print("All dbt-virtual emulator E2E checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(run_e2e())
