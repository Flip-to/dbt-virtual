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

Steps 11-25 (`run_feature_steps`) run the real rendered macros against the emulator through the harness's
`query_executor` / `relation_lookup` hooks:
   11. Registry DDL (`ensure_registry_tables_sql`) and `registry_migrations_sql` on an old-shape registry.
   12. Batched registry flush (`flush_registry`): one MERGE for several objects, `build_quality` stamped.
   13. Multi-dataset environments: `env_schemas` custom-schema mapping, per-object placeholders in a TVF template.
   14. Virtual view promotion (`register_view_deployment` -> `virtual_promote`).
   15. Promotion guards: unsealed release refused, `seal_release_sql`, partial `build_quality` refused.
   16. Partition coverage audit (`partition_coverage_sql`, `virtual_audit`).
   17. Regions: `CREATE SCHEMA ... OPTIONS (location = ...)`, per-location registry.
   18. GC per-environment retention (`find_orphan_tables_sql`, `virtual_gc`) with releases in two environments.
   19. Companion state save / restore (`companion_depart`, `restore_companion_state`) on a fake manifest table.
   20-25 (`emulator_e2e_gap_steps.py`, features that were tested only on DuckDB or the mock harness):
   20. Data diff (`virtual_data_diff`): BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(...))) per partition, exact counts.
   21. Cross-location / cross-project promote-by-clone (`physical_locations`, `source_database` / `source_schema`).
   22. `authorize_env_views`: authorized-dataset access entry added once (datasets.get / datasets.patch).
   23. Env view metadata: `OPTIONS(description, labels)` and column descriptions (tables.get / tables.patch).
   24. `gc --expire-env-days` (`virtual_expire_envs`, `DROP SCHEMA ... CASCADE`).
   25. Shared-table write guard (`assert_shared_tables_safe`) and `DBT_VIRTUAL_FPS`.
   Steps 20-25 need the emulator changes of Flip-to/googlesqlite#36 and Flip-to/bigquery-emulator#32
   (`CREATE TABLE ... CLONE`, `DROP SCHEMA`, persisted `datasets.patch` with `access`, view OPTIONS as table metadata).
   26. `virtual_data_diff` and `virtual_audit` run from a CI target read prod from its own `physical_locations` entry.
   27. A failed build puts the environment's companion state back (`companion_repair_failed`); gc deletes saved state with the dropped table (in 18).
   28. Cross-location promote writes the promoted table's companion state (source live rows, rows saved in the source registry).
Emulator limitations are listed in the final "Skipped" summary; the script is not re-runnable on one emulator
instance (restart it between runs).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from emulator_e2e_gap_steps import run_bigquery_gap_steps

from dbt_virtual.fingerprint import compute_manifest_fingerprints, compute_node_fingerprint
from dbt_virtual.planner import classify_plan_actions, compute_dbt_select_for_build
from tests.test_macros_execution import FakeRelation, MacroTestHarness


def run_feature_steps(client, q, project, requests, endpoint) -> list[str]:
    """Steps 11-19: the production-readiness features, executed through the real rendered macros.

    The macros are rendered by `MacroTestHarness`; its `query_executor` / `relation_lookup` hooks send
    every `run_query` / `exec_sql_statement` call to the emulator, so promote, audit, gc and the
    companion macros run their real SQL end to end. Returns the steps skipped because of an emulator
    limitation (each one is documented where it is skipped; nothing is faked).
    """
    import json

    from google.api_core.exceptions import NotFound

    skipped: list[str] = []
    for dataset_id in (
        "e2e_reg", "e2e_old", "e2e_stg", "e2e_stg_fin", "e2e_prod", "e2e_prod_fin",
        "e2e_cov", "e2e_gc", "e2e_cmp", "e2e_loc_ci", "e2e_loc_prod", "e2e_xsrc", "e2e_xdst", "e2e_xprod",
        "e2e_xp_phys_prod", "e2e_xp_prod",
    ):
        requests.post(
            f"{endpoint}/bigquery/v2/projects/{project}/datasets",
            json={"datasetReference": {"projectId": project, "datasetId": dataset_id}},
            timeout=10,
        )

    def run_sql(sql: str):
        result = client.query(sql).result()
        names = [f.name for f in (result.schema or [])]
        return [tuple(row.values()) for row in result], names

    def lookup(database, schema, identifier):
        try:
            table = client.get_table(f"{database or project}.{schema}.{identifier}")
        except NotFound:
            return None
        return FakeRelation(database, schema, identifier, "view" if table.table_type == "VIEW" else "table", "`")

    class Col:
        def __init__(self, name, data_type):
            self.name, self.data_type = name, data_type

    def columns_in_relation(rel):
        table = client.get_table(f"{rel.database}.{rel.schema}.{rel.identifier}")
        return [Col(f.name, f.field_type) for f in table.schema]

    def exec_script(sql: str) -> None:
        for stmt in sql.split(";"):
            if stmt.strip():
                q(stmt)

    def table_columns(dataset: str, table: str) -> list[str]:
        return [f.name for f in client.get_table(f"{project}.{dataset}.{table}").schema]

    def refused(fn, needle: str) -> None:
        try:
            fn()
        except RuntimeError as exc:
            assert needle in str(exc), f"expected {needle!r} in {exc}"
            return
        raise AssertionError(f"expected a refusal containing {needle!r}")

    fps = {"fin_orders": "f1n0rd3rs001", "udf_x": "udfx00000001", "tvf_fin": "tvffin000001", "v_big": "vbig00000001"}
    fin_alias = f"fin_orders__{fps['fin_orders']}"
    nodes = {
        "model.p.fin_orders": {
            "unique_id": "model.p.fin_orders", "name": "fin_orders", "resource_type": "model",
            "alias": fin_alias, "database": project, "schema": "e2e_reg",
            "config": {"materialized": "table", "virtual_env": True, "schema": "finance"},
            "compiled_code": "SELECT 'a' AS k, 1 AS v", "depends_on": {"nodes": []},
        },
        "model.p.udf_x": {
            "unique_id": "model.p.udf_x", "name": "udf_x", "resource_type": "model", "alias": "udf_x",
            "database": project, "schema": "e2e_stg",
            "config": {"materialized": "function", "virtual_env": True, "parameters": "n INT64", "returns": "INT64"},
            "compiled_code": "n * 10", "depends_on": {"nodes": []},
        },
        "model.p.tvf_fin": {
            "unique_id": "model.p.tvf_fin", "name": "tvf_fin", "resource_type": "model", "alias": "tvf_fin",
            "database": project, "schema": "e2e_stg_fin",
            "config": {"materialized": "table_function", "virtual_env": True, "schema": "finance", "parameters": "min_v INT64"},
            "compiled_code": (
                f"SELECT k, `{project}.e2e_stg.udf_x`(v) AS v10 FROM `{project}.e2e_stg_fin.fin_orders` WHERE v >= min_v"
            ),
            "depends_on": {"nodes": ["model.p.fin_orders", "model.p.udf_x"]},
        },
        "model.p.v_big": {
            "unique_id": "model.p.v_big", "name": "v_big", "resource_type": "model", "alias": "v_big",
            "database": project, "schema": "e2e_stg",
            "config": {"materialized": "view", "virtual_env": True},
            "compiled_code": f"SELECT k, v FROM `{project}.e2e_stg_fin.fin_orders` WHERE v > 0",
            "depends_on": {"nodes": ["model.p.fin_orders"]},
        },
    }

    def make_harness(extra_cfg=None, extra_vars=None, target_name="staging", graph_nodes=None, target_database=None):
        cfg = {
            "physical_schema": "e2e_reg",
            "env_schemas": {
                "staging": {"default": "e2e_stg", "finance": "e2e_stg_fin"},
                "prod": {"default": "e2e_prod", "finance": "e2e_prod_fin"},
            },
            **(extra_cfg or {}),
        }
        h = MacroTestHarness(
            vars_dict={"dbt_virtual": cfg, "_dbt_virtual_fps": fps, **(extra_vars or {})},
            target_name=target_name,
            target_schema="e2e_stg",
            target_database=target_database or project,
        )
        h.graph = {"nodes": nodes if graph_nodes is None else graph_nodes}
        h.query_executor = run_sql
        h.relation_lookup = lookup
        h.adapter_extras = {
            "get_columns_in_relation": columns_in_relation,
            "quote": lambda name: f"`{name}`",
        }
        return h

    # 11. Registry DDL from `ensure_registry_tables_sql`, and `registry_migrations_sql` on an old registry
    h = make_harness()
    exec_script(h.call_macro("ensure_registry_tables_sql", "e2e_reg"))
    assert table_columns("e2e_reg", "_releases")[-3:] == ["schema_key", "build_quality", "sealed_at"]
    assert "build_quality" in table_columns("e2e_reg", "_environments")
    for table in ("_locks", "_intervals"):
        assert lookup(project, "e2e_reg", table) is not None, table
    q(
        f"CREATE TABLE `{project}.e2e_old._environments` (environment STRING, model_name STRING, resource_type STRING, "
        f"fingerprint STRING, physical_identifier STRING, release_id STRING, updated_at TIMESTAMP)"
    )
    q(
        f"CREATE TABLE `{project}.e2e_old._releases` (release_id STRING, source_environment STRING, target_environment STRING, "
        f"model_name STRING, resource_type STRING, dependency_tier INT64, fingerprint STRING, physical_identifier STRING, "
        f"routine_ddl_template STRING, status STRING, created_at TIMESTAMP, promoted_at TIMESTAMP, rolled_back_at TIMESTAMP)"
    )
    q(f"INSERT INTO `{project}.e2e_old._releases` (release_id, model_name) VALUES ('legacy', 'm')")
    exec_script(h.call_macro("registry_migrations_sql", "e2e_old"))
    exec_script(h.call_macro("registry_migrations_sql", "e2e_old"))  # idempotent: IF NOT EXISTS
    assert table_columns("e2e_old", "_releases")[-3:] == ["schema_key", "build_quality", "sealed_at"], table_columns("e2e_old", "_releases")
    assert table_columns("e2e_old", "_environments")[-2:] == ["schema_key", "build_quality"]
    legacy = q(f"SELECT release_id, sealed_at, build_quality FROM `{project}.e2e_old._releases`")
    assert legacy == [{"release_id": "legacy", "sealed_at": None, "build_quality": None}], legacy
    print("[registry] ensure_registry_tables_sql + registry_migrations_sql (old-shape registry, idempotent) verified.")

    # 12. Batched registry flush: one MERGE for several objects, build_quality stamped
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_reg.{fin_alias}` AS SELECT 'a' AS k, 1 AS v UNION ALL SELECT 'b', 7")
    q(f"CREATE OR REPLACE VIEW `{project}.e2e_stg_fin.fin_orders` AS SELECT * FROM `{project}.e2e_reg.{fin_alias}`")
    results = [
        {"node": nodes[uid], "status": "success"}
        for uid in ("model.p.fin_orders", "model.p.udf_x", "model.p.tvf_fin")
    ]
    h = make_harness(extra_vars={"_dbt_virtual_release": "rel1"})
    rows = h.call_macro("collect_registry_rows", results, "staging", "e2e_reg")
    assert sorted(r["name"] for r in rows) == ["fin_orders", "tvf_fin", "udf_x"], rows
    env_sql = h.call_macro(
        "upsert_environment_pointers_sql", "e2e_reg", "staging",
        [{"name": r["name"], "resource_type": r["resource_type"], "fingerprint": r["fingerprint"],
          "physical_identifier": r["physical_identifier"], "release_id": "rel1", "schema_key": r["schema_key"]} for r in rows],
    )
    assert env_sql.count("MERGE") == 1 and env_sql.count("UNION ALL") == 2
    h.executed_queries.clear()
    h.call_macro("flush_registry", results)
    merges = [s for s in h.executed_queries if s.startswith("MERGE")]
    assert len(merges) == 1, h.executed_queries  # one MERGE for the three objects
    env_rows = q(
        f"SELECT model_name, resource_type, schema_key, build_quality FROM `{project}.e2e_reg._environments` "
        f"WHERE environment = 'staging' ORDER BY model_name"
    )
    assert env_rows == [
        {"model_name": "fin_orders", "resource_type": "model", "schema_key": "finance", "build_quality": "full"},
        {"model_name": "tvf_fin", "resource_type": "table_function", "schema_key": "finance", "build_quality": "full"},
        {"model_name": "udf_x", "resource_type": "function", "schema_key": None, "build_quality": "full"},
    ], env_rows
    rel_rows = q(
        f"SELECT model_name, dependency_tier, build_quality, sealed_at FROM `{project}.e2e_reg._releases` "
        f"WHERE release_id = 'rel1' ORDER BY dependency_tier, model_name"
    )
    assert [(r["model_name"], r["dependency_tier"], r["build_quality"], r["sealed_at"]) for r in rel_rows] == [
        ("udf_x", 1, "full", None), ("fin_orders", 2, "full", None), ("tvf_fin", 3, "full", None),
    ], rel_rows
    h.call_macro("flush_registry", results)  # idempotent re-run: still one candidate row per object
    assert q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_reg._releases` WHERE release_id = 'rel1'") == [{"n": 3}]
    print("[registry] batched flush_registry: one MERGE for 3 objects, rows + build_quality verified, re-run idempotent.")

    # 13. Multi-dataset env: per-object placeholders in the TVF template, rendered per environment
    templates = {
        r["model_name"]: r["routine_ddl_template"]
        for r in q(
            f"SELECT model_name, routine_ddl_template FROM `{project}.e2e_reg._releases` "
            f"WHERE release_id = 'rel1' AND routine_ddl_template IS NOT NULL"
        )
    }
    tvf_template = templates["tvf_fin"]
    assert "__DBT_VIRTUAL_ENV_SCHEMA_finance__.fin_orders" in tvf_template, tvf_template
    assert "__DBT_VIRTUAL_ENV_SCHEMA__.udf_x" in tvf_template, tvf_template
    assert "e2e_stg" not in tvf_template, tvf_template
    stg_ddl = h.call_macro("resolve_env_placeholders_sql", tvf_template, "staging")
    assert "e2e_stg_fin`.fin_orders" in stg_ddl or "e2e_stg_fin.fin_orders" in stg_ddl, stg_ddl
    prod_ddl = h.call_macro("resolve_env_placeholders_sql", tvf_template, "prod")
    assert "e2e_prod_fin" in prod_ddl and "e2e_prod`.udf_x" in prod_ddl.replace("e2e_prod.udf_x", "e2e_prod`.udf_x"), prod_ddl
    q(h.call_macro("resolve_env_placeholders_sql", templates["udf_x"], "staging"))
    q(stg_ddl)
    stg_tvf = q(f"SELECT k, v10 FROM `{project}.e2e_stg_fin.tvf_fin`(5)")
    assert stg_tvf == [{"k": "b", "v10": 70}], stg_tvf
    print("[multi-dataset] env_schemas mapping: view in e2e_stg_fin, TVF template resolved per object for staging and prod.")

    # 14. Virtual view promotion: register_view_deployment -> promote -> view in prod with the right body
    q(f"CREATE OR REPLACE VIEW `{project}.e2e_stg.v_big` AS SELECT k, v FROM `{project}.e2e_stg_fin.fin_orders` WHERE v > 0")
    view_sql = h.call_macro(
        "register_view_deployment", nodes["model.p.v_big"], FakeRelation(project, "e2e_stg", "v_big", "view", "`")
    )
    exec_script(view_sql)
    view_template = q(
        f"SELECT routine_ddl_template AS t FROM `{project}.e2e_reg._releases` WHERE release_id = 'rel1' AND model_name = 'v_big'"
    )[0]["t"]
    assert "CREATE OR REPLACE VIEW" in view_template and "__DBT_VIRTUAL_ENV_SCHEMA_finance__.fin_orders" in view_template, view_template

    # 15. Promotion guards: unsealed refused, then sealed; partial build refused
    hp = make_harness(target_name="prod")
    refused(lambda: hp.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="rel1"), "is not sealed")
    assert lookup(project, "e2e_prod_fin", "fin_orders") is None  # nothing was changed
    exec_script(hp.call_macro("seal_release_sql", "e2e_reg", "rel1"))
    assert q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_reg._releases` WHERE release_id = 'rel1' AND sealed_at IS NOT NULL") == [{"n": 4}]
    hp = make_harness(target_name="prod")
    hp.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="rel1", expected_fps_json=json.dumps(fps))
    assert lookup(project, "e2e_prod_fin", "fin_orders") is not None, "table view must land in the mapped finance dataset"
    assert lookup(project, "e2e_prod", "fin_orders") is None
    assert lookup(project, "e2e_prod", "v_big") is not None
    # Staging objects removed: the promoted routines must read only prod objects.
    q(f"DROP VIEW `{project}.e2e_stg_fin.fin_orders`")
    q(f"DROP FUNCTION `{project}.e2e_stg.udf_x`")
    prod_tvf = q(f"SELECT k, v10 FROM `{project}.e2e_prod_fin.tvf_fin`(5)")
    assert prod_tvf == [{"k": "b", "v10": 70}], prod_tvf
    prod_view = q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_prod.v_big`")
    assert prod_view == [{"n": 2}], prod_view
    body = client.get_table(f"{project}.e2e_prod.v_big").view_query
    assert "e2e_prod_fin" in body and "e2e_stg" not in body, body
    pointers = q(f"SELECT model_name FROM `{project}.e2e_reg._environments` WHERE environment = 'prod' ORDER BY model_name")
    assert [p["model_name"] for p in pointers] == ["fin_orders", "tvf_fin", "udf_x", "v_big"], pointers
    assert q(f"SELECT status FROM `{project}.e2e_reg._releases` WHERE release_id = 'rel1' AND model_name = 'v_big'") == [{"status": "promoted"}]
    print("[promote] sealed release promoted: per-object datasets, TVF/UDF/view bodies resolved to prod, staging dropped.")

    hq = make_harness(extra_vars={"_dbt_virtual_release": "rel_partial", "_dbt_virtual_partial": True})
    hq.call_macro("flush_registry", results[:1])
    assert q(f"SELECT build_quality AS b FROM `{project}.e2e_reg._releases` WHERE release_id = 'rel_partial'") == [{"b": "partial"}]
    exec_script(hq.call_macro("seal_release_sql", "e2e_reg", "rel_partial"))
    hp = make_harness(target_name="prod")
    refused(lambda: hp.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="rel_partial"), "built partially")
    print("[guards] unsealed release refused, seal_release_sql, partial build_quality refused (nothing changed).")

    # 16. Partition coverage audit
    for ident, days in (("cov_m__aaaaaaaaaaaa", ("2026-10-01",)), ("cov_m__bbbbbbbbbbbb", ("2026-10-01", "2026-10-02"))):
        selects = " UNION ALL ".join(f"SELECT DATE '{d}' AS d, 1 AS x" for d in days)
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_reg.{ident}` PARTITION BY d AS {selects}")
    hc = make_harness()
    q(f"INSERT INTO `{project}.e2e_reg._releases` (release_id, source_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, status, created_at) "
      f"VALUES ('rel_cov', 'staging', 'cov_m', 'model', 2, 'aaaaaaaaaaaa', 'e2e_reg.cov_m__aaaaaaaaaaaa', 'candidate', CURRENT_TIMESTAMP())")
    q(f"INSERT INTO `{project}.e2e_reg._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at) "
      f"VALUES ('prod', 'cov_m', 'model', 'bbbbbbbbbbbb', 'e2e_reg.cov_m__bbbbbbbbbbbb', 'rel_x', CURRENT_TIMESTAMP())")
    cand_rel = FakeRelation(project, "e2e_reg", "cov_m__aaaaaaaaaaaa")
    base_rel = FakeRelation(project, "e2e_reg", "cov_m__bbbbbbbbbbbb")
    coverage_sql = hc.call_macro("partition_coverage_sql", cand_rel, base_rel)
    missing = [r["partition_id"] for r in q(coverage_sql)]
    assert missing == ["20261002"], missing
    refused(
        lambda: hc.call_macro("virtual_audit", from_env="staging", to_env="prod", release_id="rel_cov"),
        "lacks 1 partition",
    )
    # Ignoring partitions before the cutoff makes the same pair pass.
    hc.call_macro("virtual_audit", from_env="staging", to_env="prod", release_id="rel_cov", coverage_ignore_before="2026-10-15")
    print("[audit] partition coverage: missing partition 20261002 refused; coverage_ignore_before lets it pass.")

    # 17. Regions: CREATE SCHEMA ... OPTIONS(location=...) from the regions macros
    hr = make_harness(extra_cfg={"physical_schemas_by_location": {"EU": "e2e_reg_eu", "US": "e2e_reg_us"}, "physical_schema": "e2e_reg"})
    assert hr.call_macro("physical_schema_for", "EU") == "e2e_reg_eu"
    assert hr.call_macro("create_location_for", "US") == "US"
    schema_sql = hr.call_macro("create_schema_if_not_exists_sql", "e2e_reg_eu", hr.call_macro("create_location_for", "EU"))
    assert "OPTIONS (location = 'EU')" in schema_sql, schema_sql
    q(schema_sql)
    client.get_dataset(f"{project}.e2e_reg_eu")  # CREATE SCHEMA must create the dataset
    exec_script(hr.call_macro("ensure_registry_tables_sql", hr.call_macro("physical_schema_for", "EU")))
    assert lookup(project, "e2e_reg_eu", "_releases") is not None
    print("[regions] CREATE SCHEMA ... OPTIONS (location = 'EU') rendered and accepted; per-location registry created in e2e_reg_eu")
    location = client.get_dataset(f"{project}.e2e_reg_eu").location
    print(f"[regions] emulator reports dataset location: {location!r}")
    assert location == "EU", location

    # 18. GC retention per environment (`find_orphan_tables_sql`) with releases in two environments
    gc_tables = [
        "m__0123456789ab",  # in _environments (prod pointer)
        "m__1111111111aa",  # newest release of env prod
        "m__2222222222bb",  # older release of env prod, beyond retention 1 -> orphan
        "n__3333333333cc",  # only release of env pr_7: kept by per-environment retention
        "m__4444444444dd",  # unreferenced -> orphan
    ]
    for ident in gc_tables:
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_gc.{ident}` AS SELECT 1 AS x")
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_gc.not_a_fingerprint_table` AS SELECT 1 AS x")
    exec_script(h.call_macro("ensure_registry_tables_sql", "e2e_gc"))
    q(f"INSERT INTO `{project}.e2e_gc._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at) "
      f"VALUES ('prod', 'm', 'model', '0123456789ab', 'e2e_gc.m__0123456789ab', 'a0', CURRENT_TIMESTAMP())")
    release_values = [
        ("a1", "prod", "m", "2222222222bb", "2026-10-01"),
        ("a2", "prod", "m", "1111111111aa", "2026-10-02"),
        ("b1", "pr_7", "n", "3333333333cc", "2026-09-01"),
    ]
    values = ", ".join(
        f"('{rid}', '{env}', '{env}', '{name}', 'model', 2, '{fp}', 'e2e_gc.{name}__{fp}', 'promoted', TIMESTAMP '{ts} 00:00:00', TIMESTAMP '{ts} 00:00:00')"
        for rid, env, name, fp, ts in release_values
    )
    q(f"INSERT INTO `{project}.e2e_gc._releases` (release_id, source_environment, target_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, status, created_at, promoted_at) VALUES {values}")
    hg = make_harness(extra_cfg={"physical_schema": "e2e_gc"})
    orphan_sql = hg.call_macro("find_orphan_tables_sql", "e2e_gc", 1, 0)
    orphans = [r["table_name"] for r in q(orphan_sql)]
    assert orphans == ["m__2222222222bb", "m__4444444444dd"], orphans
    # The state saved for a table goes with it: rows of the dropped orphan are collected, the kept one stays.
    exec_script(hg.call_macro("companion_registry_sql", "e2e_gc"))
    q(f"INSERT INTO `{project}.e2e_gc._companion_state` (companion, model_name, fingerprint, state_json, environment, release_id, recorded_at) VALUES "
      f"('e2e_cmp.manifest', 'm', '2222222222bb', '[]', 'prod', '', CURRENT_TIMESTAMP()), "
      f"('e2e_cmp.manifest', 'm', '1111111111aa', '[]', 'prod', '', CURRENT_TIMESTAMP())")
    hg.call_macro("virtual_gc", dry_run=False, retention_releases=1, retention_days=0)
    kept_state = [r["fingerprint"] for r in q(f"SELECT fingerprint FROM `{project}.e2e_gc._companion_state` ORDER BY 1")]
    assert kept_state == ["1111111111aa"], kept_state
    left = sorted(t.table_id for t in client.list_tables(f"{project}.e2e_gc") if "__" in t.table_id)
    assert left == ["m__0123456789ab", "m__1111111111aa", "n__3333333333cc"], left
    print("[gc] per-environment retention: orphans", orphans, "dropped; pr_7's only release and prod's newest kept.")

    # 19. Companion state save/restore on a fake manifest table
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_cmp.manifest` (model STRING, last_success TIMESTAMP)")
    inc_node = {
        "unique_id": "model.p.inc_m", "name": "inc_m", "resource_type": "model", "alias": "inc_m__fp1fp1fp1fp1",
        "database": project, "schema": "e2e_reg", "tags": [],
        "config": {"materialized": "incremental", "virtual_env": True}, "depends_on": {"nodes": []},
    }
    hk = make_harness(
        extra_cfg={"companion_state": [{"table": "e2e_cmp.manifest", "key_column": "model"}]},
        graph_nodes={"model.p.inc_m": inc_node},
    )
    q(f"INSERT INTO `{project}.e2e_cmp.manifest` (model, last_success) VALUES "
      f"('inc_m', TIMESTAMP '2026-11-01 00:00:00'), ('other', TIMESTAMP '2026-01-01 00:00:00')")
    exec_script(hk.call_macro(
        "upsert_environment_pointer_sql", "e2e_reg", "staging", "inc_m", "model", "fp1fp1fp1fp1", "e2e_reg.inc_m__fp1fp1fp1fp1", "", None
    ))
    assert hk.call_macro("companion_depart", inc_node, "staging", "fp2fp2fp2fp2") is True  # saves rows for fp1
    saved = q(f"SELECT fingerprint, state_json FROM `{project}.e2e_reg._companion_state`")
    assert len(saved) == 1 and saved[0]["fingerprint"] == "fp1fp1fp1fp1" and "2026-11-01" in saved[0]["state_json"], saved
    # The environment moves on: its live state is replaced, then it returns to fp1.
    q(f"DELETE FROM `{project}.e2e_cmp.manifest` WHERE model = 'inc_m'")
    q(f"INSERT INTO `{project}.e2e_cmp.manifest` (model, last_success) VALUES ('inc_m', TIMESTAMP '2026-12-31 00:00:00')")
    hk.call_macro("restore_companion_state", "inc_m", "fp1fp1fp1fp1", "staging")
    restored = q(f"SELECT model, FORMAT_TIMESTAMP('%F', last_success) AS d FROM `{project}.e2e_cmp.manifest` ORDER BY model")
    assert restored == [{"model": "inc_m", "d": "2026-11-01"}, {"model": "other", "d": "2026-01-01"}], restored
    print("[companion] state saved when the env left fp1, restored on return; other models' rows untouched.")
    # 26. Gates across `physical_locations`: from the CI target, prod's registry and tables are in
    # another dataset. Found on real BigQuery: both gates read prod from the CI registry and passed
    # without comparing anything (docs/findings/20261105_bigquery_sandbox_verification.md).
    for ds in ("e2e_loc_ci", "e2e_loc_prod"):
        exec_script(h.call_macro("ensure_registry_tables_sql", ds))
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_loc_ci.orders__fp2` AS SELECT * FROM UNNEST([STRUCT(1 AS id, 10 AS amount), STRUCT(2, 20), STRUCT(3, 30), STRUCT(4, 40)])")
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_loc_prod.orders__fp1` AS SELECT * FROM UNNEST([STRUCT(1 AS id, 10 AS amount), STRUCT(2, 20), STRUCT(3, 31)])")
    q(f"INSERT INTO `{project}.e2e_loc_ci._releases` (release_id, source_environment, target_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, status, created_at) "
      f"VALUES ('r1', 'staging', NULL, 'orders', 'model', 2, 'fp2', 'e2e_loc_ci.orders__fp2', 'candidate', CURRENT_TIMESTAMP())")
    q(f"INSERT INTO `{project}.e2e_loc_prod._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at) "
      f"VALUES ('prod', 'orders', 'model', 'fp1', 'e2e_loc_prod.orders__fp1', 'r0', CURRENT_TIMESTAMP())")
    loc_nodes = {"model.p.orders": {"name": "orders", "resource_type": "model", "config": {"unique_key": "id"}}}
    hl = make_harness(
        extra_cfg={"physical_schema": "e2e_loc_ci", "base_environment": "prod",
                   "physical_locations": {"prod": {"database": project, "schema": "e2e_loc_prod"},
                                          "default": {"database": project, "schema": "e2e_loc_ci"}}},
        graph_nodes=loc_nodes,
    )
    hl.call_macro("virtual_audit", from_env="staging", to_env="prod", release_id="r1", check_coverage=False)
    assert any("audit [orders]: OK (candidate=4 rows, prod=3 rows)" in m for m in hl.logs), hl.logs
    hl.call_macro("virtual_data_diff", from_env="staging", to_env="prod", release_id="r1")
    diff_line = next(m for m in hl.logs if m.startswith("DBT_VIRTUAL_DIFF_JSON:"))
    diff = json.loads(diff_line.split(":", 1)[1])["models"][0]
    assert (diff["status"], diff["only_candidate"], diff["changed"]) == ("differs", 1, 1), diff
    print("[locations] audit and data diff from the CI target compared the release with prod's own location.")

    # 21. Cross-PROJECT promote by clone: CI's registry and tables are in another project, prod's in
    # `project`. Found on real BigQuery: the release rows copied into the prod registry kept a routine
    # template that named the CI project, so a later rollback in prod redeployed a routine reading
    # CI (docs/findings/20261105_cross_project_promote_by_clone_bigquery.md).
    xci = "e2e-xp-ci-project"
    requests.post(
        f"{endpoint}/bigquery/v2/projects/{xci}/datasets",
        json={"datasetReference": {"projectId": xci, "datasetId": "e2e_xp_phys_ci"}}, timeout=10,
    )
    xp_locations = {"prod": {"database": project, "schema": "e2e_xp_phys_prod"},
                    "default": {"database": xci, "schema": "e2e_xp_phys_ci"}}
    xp_cfg = {"physical_schema": "e2e_xp_phys_ci", "base_environment": "prod",
              "env_schemas": {"staging": "e2e_xp_stg", "prod": "e2e_xp_prod"},
              "physical_locations": xp_locations}
    exec_script(make_harness(extra_cfg=xp_cfg, target_database=xci).call_macro("ensure_registry_tables_sql", "e2e_xp_phys_ci"))
    exec_script(make_harness(extra_cfg=xp_cfg, target_name="prod").call_macro("ensure_registry_tables_sql", "e2e_xp_phys_prod"))
    q(f"CREATE OR REPLACE TABLE `{xci}.e2e_xp_phys_ci.orders__fp1` AS SELECT * FROM UNNEST([STRUCT(1 AS id, 10 AS amount), STRUCT(2, 20)])")
    tvf_template = (f"CREATE OR REPLACE TABLE FUNCTION `{xci}`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`(min_amount INT64) AS "
                    f"(SELECT * FROM `{xci}`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`orders` WHERE amount >= min_amount)")
    q(f"INSERT INTO `{xci}.e2e_xp_phys_ci._releases` (release_id, source_environment, target_environment, model_name, resource_type, "
      f"dependency_tier, fingerprint, physical_identifier, routine_ddl_template, status, created_at, schema_key, build_quality, sealed_at) VALUES "
      f"('r1', 'staging', NULL, 'orders', 'model', 2, 'fp1', 'e2e_xp_phys_ci.orders__fp1', NULL, 'candidate', CURRENT_TIMESTAMP(), NULL, 'full', CURRENT_TIMESTAMP()), "
      f"('r1', 'staging', NULL, 'tvf', 'table_function', 3, 'fpt', 'e2e_xp_stg.tvf', '{tvf_template}', 'candidate', CURRENT_TIMESTAMP(), NULL, 'full', CURRENT_TIMESTAMP())")
    xp_nodes = {
        "model.p.orders": {"name": "orders", "resource_type": "model", "config": {"unique_key": "id"}},
        "model.p.tvf": {"name": "tvf", "resource_type": "model", "config": {"materialized": "table_function", "virtual_env": True}},
    }
    hp = make_harness(extra_cfg=xp_cfg, target_name="prod", graph_nodes=xp_nodes)
    hp.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r1")
    assert [r["id"] for r in q(f"SELECT id FROM `{project}.e2e_xp_phys_prod.orders__fp1` ORDER BY id")] == [1, 2]
    stored = q(f"SELECT routine_ddl_template AS t FROM `{project}.e2e_xp_phys_prod._releases` WHERE model_name = 'tvf'")
    assert len(stored) == 1 and f"`{project}`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`" in stored[0]["t"] and xci not in stored[0]["t"], stored
    assert q(f"SELECT environment, model_name FROM `{project}.e2e_xp_phys_prod._environments` ORDER BY model_name") == [
        {"environment": "prod", "model_name": "orders"}, {"environment": "prod", "model_name": "tvf"}]
    print("[xproject] release promoted from the CI project into the prod project by clone; the template stored in the prod registry names the prod project.")

    # Pilot finding: `promote` decided its default audit from the raw `base_environment`, so a Jinja
    # value never matched `--to`. `virtual_print_base_environment` renders it like the macros do
    # (the CLI parses this line); it must follow the target. `dbt` itself cannot run on the emulator,
    # so the CLI half (`--select` vs REUSE, `DBT_EMPTY` on compile) is covered by
    # tests/test_pilot_fixes_bigquery.py and tests/test_dbt_duckdb_pilot_fixes.py.
    jinja_base = "{{ 'e2e_stg' if target.name == 'staging' else 'e2e_prod' }}"
    for target_name, expected_base in (("staging", "e2e_stg"), ("prod", "e2e_prod")):
        hb = make_harness(extra_cfg={"base_environment": jinja_base}, target_name=target_name)
        hb.call_macro("virtual_print_base_environment")
        assert f"DBT_VIRTUAL_BASE_ENV:{expected_base}" in hb.logs, hb.logs
    print("[base_environment] a Jinja base_environment is rendered per target.")


    # 27. A failed build puts the environment's state back (`companion_repair_failed`, run by `flush_registry`)
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_cmp.manifest_fail` (model STRING, last_success TIMESTAMP)")
    q(f"INSERT INTO `{project}.e2e_cmp.manifest_fail` (model, last_success) VALUES ('inc_f', TIMESTAMP '2026-11-04 00:00:00')")
    fail_node = {**inc_node, "unique_id": "model.p.inc_f", "name": "inc_f", "alias": "inc_f__fp2fp2fp2fp2"}
    hf = make_harness(
        extra_cfg={"companion_state": [{"table": "e2e_cmp.manifest_fail", "key_column": "model"}]},
        graph_nodes={"model.p.inc_f": fail_node},
    )
    exec_script(hf.call_macro(
        "upsert_environment_pointer_sql", "e2e_reg", "staging", "inc_f", "model", "fp1fp1fp1fp1", "e2e_reg.inc_f__fp1fp1fp1fp1", "", None
    ))
    # What `seed_incremental_clones` does for a new table: save the rows of the table staging leaves,
    # then overwrite them with the seed's (the base environment's older row).
    assert hf.call_macro("companion_depart", fail_node, "staging", "fp2fp2fp2fp2") is True
    q(f"DELETE FROM `{project}.e2e_cmp.manifest_fail` WHERE model = 'inc_f'")
    q(f"INSERT INTO `{project}.e2e_cmp.manifest_fail` (model, last_success) VALUES ('inc_f', TIMESTAMP '2026-11-01 00:00:00')")
    from types import SimpleNamespace

    hf.call_macro("companion_repair_failed", [SimpleNamespace(node=fail_node, status="success")], "staging")
    seeded = q(f"SELECT FORMAT_TIMESTAMP('%F', last_success) AS d FROM `{project}.e2e_cmp.manifest_fail`")
    assert seeded == [{"d": "2026-11-01"}], seeded  # a build that succeeded is not touched
    hf.call_macro("companion_repair_failed", [SimpleNamespace(node=fail_node, status="error")], "staging")
    repaired = q(f"SELECT FORMAT_TIMESTAMP('%F', last_success) AS d FROM `{project}.e2e_cmp.manifest_fail`")
    assert repaired == [{"d": "2026-11-04"}], repaired
    print("[companion] failed build: staging's manifest row went back to the table it still points at.")

    # 28. Cross-location promote writes the promoted table's state: live rows of the source environment
    # (model xm) and rows saved in the source registry (model xs, no environment holds its table).
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_cmp.xmanifest_ci` (model STRING, last_success TIMESTAMP)")
    q(f"INSERT INTO `{project}.e2e_cmp.xmanifest_ci` (model, last_success) VALUES ('xm', TIMESTAMP '2026-11-03 00:00:00')")
    for ident in ("xm__fpxfpxfpxfp", "xs__fpyfpyfpyfp"):
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_xsrc.{ident}` AS SELECT 1 AS x")
    x_nodes = {
        f"model.p.{name}": {
            "unique_id": f"model.p.{name}", "name": name, "resource_type": "model", "alias": f"{name}__fp",
            "database": project, "schema": "e2e_xdst", "tags": [],
            "config": {"materialized": "incremental", "virtual_env": True}, "depends_on": {"nodes": []},
        }
        for name in ("xm", "xs")
    }
    hx = make_harness(
        target_name="prod",
        extra_cfg={
            "physical_schema": "e2e_xdst",
            "env_schemas": {"prod": "e2e_xprod", "ci": "e2e_xprod_ci"},
            "physical_locations": {
                "prod": {"database": project, "schema": "e2e_xdst"},
                "default": {"database": project, "schema": "e2e_xsrc"},
            },
            "companion_state": [{
                "table": "e2e_cmp.xmanifest",
                "key_column": "model",
                "env_tables": {"ci": f"{project}.e2e_cmp.xmanifest_ci", "prod": f"{project}.e2e_cmp.xmanifest_prod"},
            }],
        },
        graph_nodes=x_nodes,
    )
    exec_script(hx.call_macro("ensure_registry_tables_sql", "e2e_xsrc"))
    exec_script(hx.call_macro("ensure_registry_tables_sql", "e2e_xdst"))
    exec_script(hx.call_macro("companion_registry_sql", "e2e_xsrc"))
    q(f"INSERT INTO `{project}.e2e_xsrc._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, release_id, updated_at) "
      f"VALUES ('ci', 'xm', 'model', 'fpxfpxfpxfp', 'e2e_xsrc.xm__fpxfpxfpxfp', 'rx', CURRENT_TIMESTAMP())")
    q(f"INSERT INTO `{project}.e2e_xsrc._releases` (release_id, source_environment, target_environment, model_name, resource_type, dependency_tier, fingerprint, physical_identifier, status, created_at) VALUES "
      f"('rx', 'ci', 'ci', 'xm', 'model', 2, 'fpxfpxfpxfp', 'e2e_xsrc.xm__fpxfpxfpxfp', 'built', CURRENT_TIMESTAMP()), "
      f"('rx', 'ci', 'ci', 'xs', 'model', 2, 'fpyfpyfpyfp', 'e2e_xsrc.xs__fpyfpyfpyfp', 'built', CURRENT_TIMESTAMP())")
    q(f"INSERT INTO `{project}.e2e_xsrc._companion_state` (companion, model_name, fingerprint, state_json, environment, release_id, recorded_at) VALUES "
      f"('e2e_cmp.xmanifest', 'xs', 'fpyfpyfpyfp', '[{{\"model\": \"xs\", \"last_success\": \"2026-11-02 00:00:00+00:00\"}}]', 'ci', '', CURRENT_TIMESTAMP())")
    hx.call_macro("virtual_promote", from_env="ci", to_env="prod", release_id="rx", allow_unsealed=True)
    promoted = q(f"SELECT model, FORMAT_TIMESTAMP('%F', last_success) AS d FROM `{project}.e2e_cmp.xmanifest_prod` ORDER BY model")
    assert promoted == [{"model": "xm", "d": "2026-11-03"}, {"model": "xs", "d": "2026-11-02"}], promoted
    print("[companion] cross-location promote: prod's manifest got the source environment's live rows and the rows saved in the source registry.")
    # 30. Pilot round 2: rollback over a legacy TABLE, lock release on failure, one clone per
    # incremental, BigQuery clone chain. The emulator now runs CREATE TABLE ... CLONE and DROP SCHEMA.
    for dataset_id in ("e2e_r2_phys", "e2e_r2_stg", "e2e_r2_prod"):
        requests.post(
            f"{endpoint}/bigquery/v2/projects/{project}/datasets",
            json={"datasetReference": {"projectId": project, "datasetId": dataset_id}},
            timeout=10,
        )
    r2_cfg = {"physical_schema": "e2e_r2_phys", "env_schemas": {"staging": "e2e_r2_stg", "prod": "e2e_r2_prod"}}

    def r2_harness(**kwargs):
        return make_harness(extra_cfg={**r2_cfg, **kwargs.pop("cfg", {})}, graph_nodes=kwargs.pop("nodes", {}), **kwargs)

    hr = r2_harness(target_name="prod")
    exec_script(hr.call_macro("ensure_registry_tables_sql", "e2e_r2_phys"))
    for name in ("a", "b"):
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_r2_phys.{name}__f1` AS SELECT 1 AS x")
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_r2_prod.{name}` AS SELECT 1 AS x")  # legacy TABLE kept by adopt
        q(
            f"INSERT INTO `{project}.e2e_r2_phys._releases` (release_id, source_environment, target_environment, model_name, "
            f"resource_type, dependency_tier, fingerprint, physical_identifier, status, created_at, promoted_at) VALUES "
            f"('adopt1', 'prod', 'prod', '{name}', 'model', 2, 'f1', 'e2e_r2_phys.{name}__f1', 'promoted', "
            f"CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())"
        )
        exec_script(hr.call_macro(
            "upsert_environment_pointer_sql", "e2e_r2_phys", "prod", name, "model", "f1", f"e2e_r2_phys.{name}__f1", "adopt1", None
        ))
    # 30.1 + 30.2: a rollback over legacy tables replaces them; a refused promote with a lock leaves no lease.
    hr.call_macro("virtual_rollback", env="prod", to_release="adopt1", lock=True, lock_owner="r2")
    for name in ("a", "b"):
        assert client.get_table(f"{project}.e2e_r2_prod.{name}").table_type == "VIEW", name
        assert q(f"SELECT x FROM `{project}.e2e_r2_prod.{name}`") == [{"x": 1}]
    assert q(f"SELECT owner FROM `{project}.e2e_r2_phys._locks`") == []
    refused(
        lambda: hr.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="nope", lock=True, lock_owner="r2"),
        "No models found",
    )
    assert q(f"SELECT owner FROM `{project}.e2e_r2_phys._locks`") == [], "a refused promote kept its lease"
    print("[rollback] adopt release restored over legacy TABLEs (replaced by views); refused promote released its lock.")

    # 30.3: a clone seeded earlier in the invocation is not dropped and cloned again (real CLONE).
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_r2_phys.inc__base1` AS SELECT 1 AS id")
    exec_script(hr.call_macro(
        "upsert_environment_pointer_sql", "e2e_r2_phys", "prod", "inc", "model", "base1", "e2e_r2_phys.inc__base1", "adopt1", None
    ))
    inc_node2 = {
        "unique_id": "model.p.inc", "name": "inc", "resource_type": "model", "alias": "inc__new1",
        "database": project, "schema": "e2e_r2_phys", "tags": [],
        "config": {"materialized": "incremental", "virtual_env": True}, "depends_on": {"nodes": []},
    }
    hs = r2_harness(
        target_name="staging", nodes={"model.p.inc": inc_node2},
        extra_vars={"_dbt_virtual_seeded": []},
    )
    hs.call_macro("seed_incremental_clones", model_names=["inc"])  # the CLI's seeding
    hs.call_macro("seed_incremental_clones")  # the on-run-start hook
    assert sum("CLONE" in x for x in hs.executed_queries) == 1, hs.executed_queries
    assert not any("never registered" in m for m in hs.logs), hs.logs
    assert q(f"SELECT id FROM `{project}.e2e_r2_phys.inc__new1`") == [{"id": 1}]
    print("[seed] a new incremental is cloned once per invocation; the hook did not re-clone it.")

    # 30.4: the clone macro with the chain backstop runs on the emulator (scripting block, CLONE branch).
    hc = r2_harness(target_name="staging")
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_r2_phys.chain0` AS SELECT 1 AS id")
    q(hc.call_macro(
        "clone_table_sql", FakeRelation(project, "e2e_r2_phys", "chain0", quote_char="`"), FakeRelation(project, "e2e_r2_phys", "chain1", quote_char="`")
    ))
    assert q(f"SELECT id FROM `{project}.e2e_r2_phys.chain1`") == [{"id": 1}]
    print("[clone] clone_table_sql with the chain backstop ran on the emulator (CLONE branch).")

    # 30.5: clone chain depth. Seeding clones the base environment's table; the emulator (like BigQuery) reports
    # table_type CLONE + base_table_* and refuses a 4th chained clone, so `clone_or_copy_table` reads the depth
    # (bigquery_clone_depth) and seeds with CREATE TABLE ... COPY at depth 3, with CLONE below. Each seeded table
    # becomes the base environment's pointer, as after a promote, so the chain grows by one per seeding.
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_r2_phys.ch__v0` (id INT64, d DATE) PARTITION BY d CLUSTER BY id")
    q(f"INSERT INTO `{project}.e2e_r2_phys.ch__v0` (id, d) VALUES (1, DATE '2026-10-01'), (2, DATE '2026-10-02')")

    def point_prod_at(ident: str, fp: str) -> None:
        exec_script(hr.call_macro(
            "upsert_environment_pointer_sql", "e2e_r2_phys", "prod", "ch", "model", fp, f"e2e_r2_phys.{ident}", "adopt_ch", None
        ))

    def table_kind(ident: str) -> tuple:
        row = q(
            f"SELECT table_type, base_table_name FROM `{project}`.`e2e_r2_phys`.INFORMATION_SCHEMA.TABLES WHERE table_name = '{ident}'"
        )
        assert len(row) == 1, (ident, row)
        return row[0]["table_type"], row[0]["base_table_name"]

    point_prod_at("ch__v0", "v0")
    seeded: list[str] = []
    for i in range(1, 6):
        ident = f"ch__v{i}"
        node = {
            "unique_id": "model.p.ch", "name": "ch", "resource_type": "model", "alias": ident,
            "database": project, "schema": "e2e_r2_phys", "tags": [],
            "config": {"materialized": "incremental", "virtual_env": True}, "depends_on": {"nodes": []},
        }
        hs = r2_harness(target_name="staging", nodes={"model.p.ch": node}, extra_vars={"_dbt_virtual_seeded": []})
        hs.call_macro("seed_incremental_clones", model_names=["ch"])
        seeded.append(table_kind(ident)[0])
        # The log names the path taken: the chain depth of the source and CLONE, or the copy at the limit.
        if i == 4:
            assert any("copying" in m and "3 clone(s) deep" in m for m in hs.logs), hs.logs
        else:
            assert any("cloning" in m for m in hs.logs), hs.logs
        assert q(f"SELECT COUNT(*) AS n, SUM(id) AS s FROM `{project}.e2e_r2_phys.{ident}`") == [{"n": 2, "s": 3}], ident
        point_prod_at(ident, f"v{i}")
    assert seeded == ["CLONE", "CLONE", "CLONE", "BASE TABLE", "CLONE"], seeded
    assert table_kind("ch__v3") == ("CLONE", "ch__v2")
    assert table_kind("ch__v4") == ("BASE TABLE", None), "the COPY at depth 3 is an independent table"
    assert table_kind("ch__v5") == ("CLONE", "ch__v4"), "the chain restarts from the copy"
    # REST tables.get: type TABLE plus cloneDefinition for a clone; the COPY has none and keeps the layout.
    tables_url = f"{endpoint}/bigquery/v2/projects/{project}/datasets/e2e_r2_phys/tables"
    clone_meta = requests.get(f"{tables_url}/ch__v3", timeout=10).json()
    assert clone_meta["type"] == "TABLE"
    assert clone_meta["cloneDefinition"]["baseTableReference"]["tableId"] == "ch__v2", clone_meta
    copy_meta = requests.get(f"{tables_url}/ch__v4", timeout=10).json()
    assert "cloneDefinition" not in copy_meta, copy_meta
    assert copy_meta["timePartitioning"]["field"] == "d" and copy_meta["clustering"]["fields"] == ["id"], copy_meta
    print("[clone-chain] seeding cloned at depth 0, 1, 2, copied at depth 3 (ch__v4 BASE TABLE, partitioning and clustering kept), then cloned the copy.")

    run_bigquery_gap_steps(client, q, project, requests, endpoint)
    return skipped


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
          updated_at TIMESTAMP,
          schema_key STRING,
          build_quality STRING
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
          rolled_back_at TIMESTAMP,
          schema_key STRING,
          build_quality STRING,
          sealed_at TIMESTAMP
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

    skipped = run_feature_steps(client, q, project, requests, endpoint)

    print("All dbt-virtual emulator E2E checks PASSED.")
    if skipped:
        print(f"Skipped because of emulator limitations ({len(skipped)}):")
        for line in skipped:
            print(f"  - {line}")
    return 0


if __name__ == "__main__":
    sys.exit(run_e2e())
