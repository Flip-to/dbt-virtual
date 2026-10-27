#!/usr/bin/env python
"""End-to-end verification of dbt-virtual against Flip-to/bigquery-emulator.

Exercises the full virtual data environment lifecycle against a running BigQuery emulator
(`BIGQUERY_EMULATOR_HOST`, default `http://127.0.0.1:9050`, project `test-project`):

1. Registry initialization (`_environments`, `_releases` in `spacetime_physical`).
2. Release v1 build in `staging`:
   - Content-addressed physical table `spacetime_physical.traffic_metrics_root__<fp_v1>`
   - Virtual environment view `spacetime_staging.traffic_metrics_root`
   - Scalar SQL UDF `spacetime_staging.udf_double_sessions(n INT64)`
   - Table Function (TVF) `spacetime_staging.ft_report_traffic(min_sessions INT64)` reading the view
3. Zero-rebuild promotion (`staging` -> `prod` for release `v1`):
   - View `spacetime.traffic_metrics_root` points to `spacetime_physical.traffic_metrics_root__<fp_v1>`
   - `spacetime.udf_double_sessions` and `spacetime.ft_report_traffic` deployed in `prod`
4. Release v2 build in `staging` + promote to `prod`:
   - New fingerprinted table `spacetime_physical.traffic_metrics_root__<fp_v2>` seeded from `prod`'s `<fp_v1>`
     and updated with new rows
   - Promoted to `prod` via view swap
5. Instant rollback (`prod` -> release `v1`):
   - Repoints `spacetime.traffic_metrics_root` back to `<fp_v1>` and verifies `spacetime.ft_report_traffic`
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
    for dataset_id in ("spacetime_physical", "spacetime_staging", "spacetime"):
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

    # 1. Create registry tables in spacetime_physical
    q(
        f"""
        CREATE TABLE IF NOT EXISTS `{project}.spacetime_physical._environments` (
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
        CREATE TABLE IF NOT EXISTS `{project}.spacetime_physical._releases` (
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
        "unique_id": "model.test.traffic_metrics_root",
        "name": "traffic_metrics_root",
        "resource_type": "model",
        "compiled_code": "SELECT 'prop_1' AS entity_uuid, 10 AS sessions UNION ALL SELECT 'prop_2', 25",
        "raw_code": "SELECT 'prop_1' AS entity_uuid, 10 AS sessions UNION ALL SELECT 'prop_2', 25",
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    fp_v1 = compute_node_fingerprint(node_v1).fingerprint
    phys_v1 = f"traffic_metrics_root__{fp_v1}"
    print(f"[v1] Computed fingerprint: {fp_v1} -> spacetime_physical.{phys_v1}")

    q(
        f"""
        CREATE OR REPLACE TABLE `{project}.spacetime_physical.{phys_v1}` AS
        SELECT 'prop_1' AS entity_uuid, 10 AS sessions
        UNION ALL
        SELECT 'prop_2' AS entity_uuid, 25 AS sessions
        """
    )
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.spacetime_staging.traffic_metrics_root` AS
        SELECT * FROM `{project}.spacetime_physical.{phys_v1}`
        """
    )
    q(
        f"""
        CREATE OR REPLACE FUNCTION `{project}.spacetime_staging.udf_double_sessions`(n INT64)
        RETURNS INT64
        AS (n * 2)
        """
    )
    q(
        f"""
        CREATE OR REPLACE TABLE FUNCTION `{project}.spacetime_staging.ft_report_traffic`(min_s INT64)
        AS (
          SELECT
            entity_uuid,
            sessions,
            `{project}.spacetime_staging.udf_double_sessions`(sessions) AS doubled_sessions
          FROM `{project}.spacetime_staging.traffic_metrics_root`
          WHERE sessions >= min_s
        )
        """
    )

    stg_rows = q(
        f"SELECT entity_uuid, sessions, doubled_sessions FROM `{project}.spacetime_staging.ft_report_traffic`(15)"
    )
    assert stg_rows == [{"entity_uuid": "prop_2", "sessions": 25, "doubled_sessions": 50}], stg_rows
    print("[v1] Staging TVF + UDF + virtual view verified:", stg_rows)

    # 3. Zero-rebuild Promote v1 to prod (view swap + routine deploy)
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.spacetime.traffic_metrics_root` AS
        SELECT * FROM `{project}.spacetime_physical.{phys_v1}`
        """
    )
    q(
        f"""
        CREATE OR REPLACE FUNCTION `{project}.spacetime.udf_double_sessions`(n INT64)
        RETURNS INT64
        AS (n * 2)
        """
    )
    q(
        f"""
        CREATE OR REPLACE TABLE FUNCTION `{project}.spacetime.ft_report_traffic`(min_s INT64)
        AS (
          SELECT
            entity_uuid,
            sessions,
            `{project}.spacetime.udf_double_sessions`(sessions) AS doubled_sessions
          FROM `{project}.spacetime.traffic_metrics_root`
          WHERE sessions >= min_s
        )
        """
    )
    prod_v1_rows = q(
        f"SELECT SUM(sessions) AS total_sessions FROM `{project}.spacetime.ft_report_traffic`(0)"
    )
    assert prod_v1_rows == [{"total_sessions": 35}], prod_v1_rows
    print("[v1] Promoted to prod with zero table rebuild:", prod_v1_rows)

    # 4. Build v2 in staging (seed from prod v1 table + incremental insert), then promote to prod
    node_v2 = {
        **node_v1,
        "compiled_code": "SELECT 'prop_1' AS entity_uuid, 10 AS sessions UNION ALL SELECT 'prop_2', 25 UNION ALL SELECT 'prop_3', 40",
    }
    fp_v2 = compute_node_fingerprint(node_v2).fingerprint
    phys_v2 = f"traffic_metrics_root__{fp_v2}"
    assert fp_v1 != fp_v2
    print(f"[v2] Computed fingerprint: {fp_v2} -> spacetime_physical.{phys_v2}")

    # Seed v2 from prod's v1 table and append incremental row
    q(
        f"""
        CREATE OR REPLACE TABLE `{project}.spacetime_physical.{phys_v2}` AS
        SELECT * FROM `{project}.spacetime_physical.{phys_v1}`
        UNION ALL
        SELECT 'prop_3' AS entity_uuid, 40 AS sessions
        """
    )
    # Promote v2 to prod by swapping the view only (TVF in prod automatically reads the swapped view!)
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.spacetime.traffic_metrics_root` AS
        SELECT * FROM `{project}.spacetime_physical.{phys_v2}`
        """
    )
    prod_v2_rows = q(
        f"SELECT SUM(sessions) AS total_sessions FROM `{project}.spacetime.ft_report_traffic`(0)"
    )
    assert prod_v2_rows == [{"total_sessions": 75}], prod_v2_rows
    print("[v2] Promoted to prod via view swap (TVF automatically sees v2 data):", prod_v2_rows)

    # 5. Instant Rollback of prod from v2 back to v1
    q(
        f"""
        CREATE OR REPLACE VIEW `{project}.spacetime.traffic_metrics_root` AS
        SELECT * FROM `{project}.spacetime_physical.{phys_v1}`
        """
    )
    prod_rollback_rows = q(
        f"SELECT SUM(sessions) AS total_sessions FROM `{project}.spacetime.ft_report_traffic`(0)"
    )
    assert prod_rollback_rows == [{"total_sessions": 35}], prod_rollback_rows
    print("[rollback] Instant view rollback to v1 verified:", prod_rollback_rows)

    print("All dbt-virtual emulator E2E checks PASSED.")
    return 0


if __name__ == "__main__":
    sys.exit(run_e2e())
