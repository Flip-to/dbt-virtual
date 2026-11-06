"""BigQuery emulator E2E steps 20-25: features that were tested only on DuckDB or the mock harness.

Called from `run_emulator_e2e.run_feature_steps`. The macros are the real rendered package macros
(`MacroTestHarness`); every `run_query` / `exec_sql_statement` goes to the emulator through the
harness's `query_executor` / `relation_lookup` hooks, and the BigQuery-only adapter calls
(`grant_access_to`, `update_columns`) are replaced by stubs that perform the same REST calls
dbt-bigquery makes (`datasets.get` + `datasets.patch`, `tables.patch`) against the emulator.

20. Data diff (`virtual_data_diff`): partitioned tables with injected differences, exact counts.
21. Cross-location promote-by-clone (`virtual_promote` with `physical_locations` / `source_*`).
22. `authorize_env_views`: authorized-dataset access entry, added once, visible through the REST API.
23. Env view metadata: view `OPTIONS(description, labels)` and column descriptions.
24. `gc --expire-env-days` (`virtual_expire_envs`).
25. Shared-table write guard (`assert_shared_tables_safe`) and `DBT_VIRTUAL_FPS`.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from tests.test_macros_execution import FakeRelation, MacroTestHarness


class _Io:
    """Warehouse access shared by the steps: queries, relation lookup and the REST API."""

    def __init__(self, client, q, project, requests, endpoint) -> None:
        self.client, self.q, self.project, self.requests, self.endpoint = client, q, project, requests, endpoint

    # -- SQL -------------------------------------------------------------------------------------
    def run_sql(self, sql: str):
        result = self.client.query(sql).result()
        names = [f.name for f in (result.schema or [])]
        return [tuple(row.values()) for row in result], names

    def lookup(self, database, schema, identifier):
        from google.api_core.exceptions import NotFound

        try:
            table = self.client.get_table(f"{database or self.project}.{schema}.{identifier}")
        except NotFound:
            return None
        return FakeRelation(database, schema, identifier, "view" if table.table_type == "VIEW" else "table", "`")

    def columns_in_relation(self, rel):
        class Col:
            def __init__(self, name, data_type):
                self.name, self.data_type = name, data_type

        table = self.client.get_table(f"{rel.database}.{rel.schema}.{rel.identifier}")
        return [Col(f.name, f.field_type) for f in table.schema]

    def exec_script(self, sql: str) -> None:
        for stmt in sql.split(";"):
            if stmt.strip():
                self.q(stmt)

    def refused(self, fn, needle: str) -> None:
        try:
            fn()
        except RuntimeError as exc:
            assert needle in str(exc), f"expected {needle!r} in {exc}"
            return
        raise AssertionError(f"expected a refusal containing {needle!r}")

    def create_datasets(self, *dataset_ids: str, project: str | None = None) -> None:
        project = project or self.project
        for dataset_id in dataset_ids:
            resp = self.requests.post(
                f"{self.endpoint}/bigquery/v2/projects/{project}/datasets",
                json={"datasetReference": {"projectId": project, "datasetId": dataset_id}},
                timeout=10,
            )
            assert resp.status_code in (200, 409), (dataset_id, resp.status_code, resp.text)

    # -- REST ------------------------------------------------------------------------------------
    def dataset_url(self, dataset: str) -> str:
        return f"{self.endpoint}/bigquery/v2/projects/{self.project}/datasets/{dataset}"

    def rest_get(self, url: str) -> dict[str, Any]:
        resp = self.requests.get(url, timeout=10)
        assert resp.status_code == 200, (url, resp.status_code, resp.text)
        return resp.json()

    def rest_patch(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = self.requests.patch(url, json=body, timeout=10)
        assert resp.status_code == 200, (url, resp.status_code, resp.text)
        return resp.json()

    # -- harness ---------------------------------------------------------------------------------
    def harness(self, cfg, *, nodes=None, target_name="staging", target_schema="e2e_stg", extra_vars=None):
        h = MacroTestHarness(
            vars_dict={"dbt_virtual": cfg, **(extra_vars or {})},
            target_name=target_name,
            target_schema=target_schema,
            target_database=self.project,
        )
        h.graph = {"nodes": nodes or {}}
        h.query_executor = self.run_sql
        h.relation_lookup = self.lookup
        h.adapter_extras = {
            "get_columns_in_relation": self.columns_in_relation,
            "quote": lambda name: f"`{name}`",
            "grant_access_to": self.grant_access_to,
            "update_columns": self.update_columns,
        }
        return h

    def grant_access_to(self, entity, entity_type, role, grant_target_dict) -> None:
        """What dbt-bigquery's `grant_access_to` does: read the dataset, add the entry if absent, update."""
        assert entity_type == "dataset", entity_type
        url = self.dataset_url(grant_target_dict["dataset"])
        access = self.rest_get(url).get("access") or []
        entry = {"dataset": entity}  # AccessEntry(None, "dataset", entity).to_api_repr()
        if entry not in access:
            self.rest_patch(url, {"access": [*access, entry]})

    def update_columns(self, relation, columns) -> None:
        """What dbt-bigquery's `update_columns` does: `tables.patch` the schema with the descriptions."""
        url = f"{self.dataset_url(relation.schema)}/tables/{relation.identifier}"
        schema = self.rest_get(url)["schema"]["fields"]
        fields = [
            {**field, "description": columns[field["name"]]["description"]} if field["name"] in columns else field
            for field in schema
        ]
        self.rest_patch(url, {"schema": {"fields": fields}})


def run_bigquery_gap_steps(client, q, project, requests, endpoint) -> None:
    io = _Io(client, q, project, requests, endpoint)
    for step in (step_data_diff, step_promote_clone, step_env_view_metadata, step_expire_envs, step_shared_table_guard):
        started = time.monotonic()
        step(io)
        print(f"[timing] {step.__name__}: {time.monotonic() - started:.1f}s")


def _registry(io: _Io, h: MacroTestHarness, schema: str) -> None:
    io.exec_script(h.call_macro("ensure_registry_tables_sql", schema))


def step_data_diff(io: _Io) -> None:
    """20. `virtual_data_diff`: BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))) checksums per partition."""
    project, q = io.project, io.q
    io.create_datasets("e2e_dd")
    node = {
        "unique_id": "model.p.dd_events", "name": "dd_events", "resource_type": "model",
        "config": {"unique_key": "id", "partition_by": {"field": "d", "data_type": "date"}},
    }
    cfg = {"physical_schema": "e2e_dd", "env_schemas": {"staging": "e2e_dd_stg", "prod": "e2e_dd_prod"}}
    h = io.harness(cfg, nodes={"model.p.dd_events": node})
    _registry(io, h, "e2e_dd")

    # Four partitions of 12 rows. 2026-10-01 and 2026-10-02 are identical on both sides.
    # 2026-10-03: ids 100 and 101 are only in the candidate, id 200 only in the target, ids 27 and 28
    #   changed `v`. 2026-10-04: id 48 changed `amt` (a second column), nothing else.
    def build(table: str, mutate: bool) -> None:
        rows = []
        for day, base in (("2026-10-01", 0), ("2026-10-02", 12), ("2026-10-03", 24), ("2026-10-04", 36)):
            for i in range(12):
                rows.append((base + i + 1, day, f"v{base + i + 1}", float(i)))
        out = []
        for rid, day, v, amt in rows:
            if mutate:
                if rid in (27, 28):  # changed `v` (2026-10-03)
                    v = v + "_new"
                if rid == 48:  # changed `amt` (2026-10-04)
                    amt = amt + 0.5
            out.append((rid, day, v, amt))
        if mutate:
            out.append((100, "2026-10-03", "extra_a", 1.0))  # only in the candidate
            out.append((101, "2026-10-03", "extra_b", 2.0))  # only in the candidate
        else:
            out.append((200, "2026-10-03", "gone", 3.0))  # only in the target
        values = ", ".join(f"({rid}, DATE '{day}', '{v}', {amt})" for rid, day, v, amt in out)
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_dd.{table}` (id INT64, d DATE, v STRING, amt FLOAT64) PARTITION BY d")
        q(f"INSERT INTO `{project}.e2e_dd.{table}` (id, d, v, amt) VALUES {values}")

    build("dd_events__aaaaaaaaaaaa", mutate=True)
    build("dd_events__bbbbbbbbbbbb", mutate=False)
    q(
        f"INSERT INTO `{project}.e2e_dd._releases` (release_id, source_environment, model_name, resource_type, dependency_tier, "
        f"fingerprint, physical_identifier, status, created_at) VALUES ('rel_dd', 'staging', 'dd_events', 'model', 2, "
        f"'aaaaaaaaaaaa', 'e2e_dd.dd_events__aaaaaaaaaaaa', 'candidate', CURRENT_TIMESTAMP())"
    )
    q(
        f"INSERT INTO `{project}.e2e_dd._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, "
        f"release_id, updated_at) VALUES ('prod', 'dd_events', 'model', 'bbbbbbbbbbbb', 'e2e_dd.dd_events__bbbbbbbbbbbb', 'rel_prod', "
        f"CURRENT_TIMESTAMP())"
    )

    # Expected: candidate 50 rows, target 49 rows; only_candidate 2, only_target 1, changed 3.
    def diff(**kwargs):
        h.logs.clear()
        h.executed_queries.clear()
        summary = h.call_macro("virtual_data_diff", from_env="staging", to_env="prod", release_id="rel_dd", **kwargs)
        line = next(m for m in h.logs if m.startswith("DBT_VIRTUAL_DIFF_JSON:"))
        assert json.loads(line.split(":", 1)[1]) == json.loads(json.dumps(summary))
        return summary["models"][0], list(h.executed_queries)

    model, queries = diff()
    assert (model["only_candidate"], model["only_target"], model["changed"]) == (2, 1, 3), model
    assert (model["candidate_rows"], model["target_rows"]) == (50, 49), model
    assert model["status"] == "differs" and model["drilled"] is True and model["segment_kind"] == "date", model
    assert model["segments_total"] == 4 and model["segments_identical"] == 2, model
    assert model["column_mismatches"] == {"d": 0, "v": 2, "amt": 1}, model
    leaves = {leaf["segment"]: leaf for leaf in model["leaf_segments"]}
    assert sorted(leaves) == ["partition=2026-10-03", "partition=2026-10-04"], leaves
    assert (leaves["partition=2026-10-03"]["only_candidate"], leaves["partition=2026-10-03"]["only_target"], leaves["partition=2026-10-03"]["changed"]) == (2, 1, 2), leaves
    assert (leaves["partition=2026-10-04"]["only_candidate"], leaves["partition=2026-10-04"]["only_target"], leaves["partition=2026-10-04"]["changed"]) == (0, 0, 1), leaves
    # Identical partitions are never drilled: no leaf query mentions them, only the two checksum queries do.
    joins = [s for s in queries if "FULL OUTER JOIN" in s]
    assert joins and not any("2026-10-01" in s or "2026-10-02" in s for s in joins), joins
    assert model["queries"] == {"checksum_queries": 2, "join_queries": 2, "sample_queries": 2}, model["queries"]
    assert "BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(" in queries[2], queries[2]
    sample = {(s["kind"], tuple(s["key"].values()), tuple(s["changed_columns"])) for s in model["sample"]}
    assert sample == {
        ("only_candidate", ("100",), ()), ("only_candidate", ("101",), ()), ("only_target", ("200",), ()),
        ("changed", ("27",), ("v",)), ("changed", ("28",), ("v",)), ("changed", ("48",), ("amt",)),
    }, sample
    assert abs(model["mismatch_pct"] - 6 * 100.0 / 50) < 1e-6, model["mismatch_pct"]

    # A low threshold bisects the 13-row partition by key range; the totals must not change.
    bis, bis_queries = diff(threshold=4, branching=4)
    assert (bis["only_candidate"], bis["only_target"], bis["changed"]) == (2, 1, 3), bis
    assert bis["segments_split"] >= 1 and any(leaf["depth"] >= 1 for leaf in bis["leaf_segments"]), bis
    assert not any("2026-10-01" in s or "2026-10-02" in s for s in bis_queries if "FULL OUTER JOIN" in s)

    # max_mismatch_pct gate: 12% mismatch passes a 15% gate and fails a 5% gate (after logging the summary).
    ok, _ = diff(max_mismatch_pct=15)
    assert ok["mismatch_pct"] == 12.0
    h.logs.clear()
    io.refused(lambda: h.call_macro("virtual_data_diff", from_env="staging", to_env="prod", release_id="rel_dd", max_mismatch_pct=5), "exceeds max_mismatch_pct=5")
    assert any(m.startswith("DBT_VIRTUAL_DIFF_JSON:") and '"passed": false' in m for m in h.logs), h.logs

    # The gate also guards promote(data_diff=True): refused before any object changes.
    hp = io.harness(cfg, nodes={"model.p.dd_events": node}, target_name="prod", target_schema="e2e_dd_prod")
    io.exec_script(hp.call_macro("seal_release_sql", "e2e_dd", "rel_dd"))
    io.refused(
        lambda: hp.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="rel_dd", data_diff=True, data_diff_max_mismatch_pct=5),
        "exceeds max_mismatch_pct=5",
    )
    assert io.lookup(project, "e2e_dd_prod", "dd_events") is None, "a refused promote must not create the view"

    # Identical tables cost one checksum query per side and no join.
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_dd.dd_events__cccccccccccc` PARTITION BY d AS SELECT * FROM `{project}.e2e_dd.dd_events__bbbbbbbbbbbb`")
    q(
        f"UPDATE `{project}.e2e_dd._releases` SET physical_identifier = 'e2e_dd.dd_events__cccccccccccc', fingerprint = 'cccccccccccc' "
        f"WHERE release_id = 'rel_dd'"
    )
    same, same_queries = diff()
    assert same["status"] == "identical" and same["queries"] == {"checksum_queries": 2, "join_queries": 0, "sample_queries": 0}, same
    assert (same["only_candidate"], same["only_target"], same["changed"], same["segments_identical"]) == (0, 0, 0, 4), same
    assert not any("FULL OUTER JOIN" in s for s in same_queries)
    print(
        "[data-diff] BigQuery checksums: 2 differing partitions drilled (only-in-candidate 2, only-in-target 1, changed 3), "
        "2 identical partitions never joined; bisection totals equal; max_mismatch_pct 5 refused / 15 passed; promote gate refused; identical tables 0 joins."
    )


OTHER_PROJECT = "e2e-source-project"  # a second project in the same emulator: BigQuery creates projects implicitly


def _seed_release(io: _Io, h: MacroTestHarness, *, database: str, schema: str, release_id: str, model: str, fp: str, rows_sql: str) -> None:
    """A sealed single-model release and its physical table in `database.schema` (the source location)."""
    io.exec_script(h.call_macro("ensure_registry_tables_sql", schema))
    io.q(f"CREATE OR REPLACE TABLE `{database}.{schema}.{model}__{fp}` AS {rows_sql}")
    io.q(
        f"INSERT INTO `{database}.{schema}._releases` (release_id, source_environment, model_name, resource_type, dependency_tier, "
        f"fingerprint, physical_identifier, status, created_at, build_quality, sealed_at) VALUES ('{release_id}', 'staging', "
        f"'{model}', 'model', 2, '{fp}', '{schema}.{model}__{fp}', 'candidate', CURRENT_TIMESTAMP(), 'full', CURRENT_TIMESTAMP())"
    )


def step_promote_clone(io: _Io) -> None:
    """21. Promote by CLONE across locations: `physical_locations`, and `source_database` / `source_schema`."""
    project, q = io.project, io.q
    rows = "SELECT 1 AS id, 'a' AS v UNION ALL SELECT 2, 'b' UNION ALL SELECT 3, 'c'"
    node = {
        "unique_id": "model.p.xm", "name": "xm", "resource_type": "model", "alias": "xm",
        "config": {"materialized": "table", "virtual_env": True},
    }

    def view_query(dataset: str, name: str, database: str = project) -> str:
        return io.client.get_table(f"{database}.{dataset}.{name}").view_query

    # A. Two datasets as two locations: staging's physical tables in e2e_loc_phys_stg, prod's in e2e_loc_phys_prod.
    io.create_datasets("e2e_loc_phys_stg", "e2e_loc_phys_prod", "e2e_loc_prod")
    cfg_a = {
        # The emulator has no INFORMATION_SCHEMA.TABLES.base_table_* (the clone-depth check) nor COPY.
        "clone_chain_fallback": False,
        "physical_schema": "e2e_loc_phys_prod",
        "env_schemas": {"staging": "e2e_loc_stg", "prod": "e2e_loc_prod"},
        "physical_locations": {"staging": {"schema": "e2e_loc_phys_stg"}, "prod": {"schema": "e2e_loc_phys_prod"}},
    }
    h_stg = io.harness(cfg_a, nodes={"model.p.xm": node}, target_name="staging", target_schema="e2e_loc_stg")
    assert h_stg.call_macro("physical_location", "staging") == {"database": project, "schema": "e2e_loc_phys_stg"}
    _seed_release(io, h_stg, database=project, schema="e2e_loc_phys_stg", release_id="rel_a", model="xm", fp="aaaaaaaaaaaa", rows_sql=rows)
    h_prod = io.harness(cfg_a, nodes={"model.p.xm": node}, target_name="prod", target_schema="e2e_loc_prod")
    io.exec_script(h_prod.call_macro("ensure_registry_tables_sql", "e2e_loc_phys_prod"))  # the target registry (`statement` is a no-op in the harness)
    source = h_prod.call_macro("promotion_source", "staging", "prod", "rel_a", "", "", "e2e_loc_phys_prod")
    assert source == {"cross": True, "database": project, "schema": "e2e_loc_phys_stg"}, source

    h_prod.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="rel_a")
    clone = io.client.get_table(f"{project}.e2e_loc_phys_prod.xm__aaaaaaaaaaaa")
    assert clone.table_type == "TABLE"
    assert q(f"SELECT COUNT(*) AS n, SUM(id) AS s FROM `{project}.e2e_loc_phys_prod.xm__aaaaaaaaaaaa`") == [{"n": 3, "s": 6}]
    body = view_query("e2e_loc_prod", "xm")
    assert "e2e_loc_phys_prod" in body and "e2e_loc_phys_stg" not in body, body
    pointer = q(f"SELECT physical_identifier, fingerprint FROM `{project}.e2e_loc_phys_prod._environments` WHERE environment = 'prod'")
    assert pointer == [{"physical_identifier": "e2e_loc_phys_prod.xm__aaaaaaaaaaaa", "fingerprint": "aaaaaaaaaaaa"}], pointer
    assert q(f"SELECT status, target_environment FROM `{project}.e2e_loc_phys_prod._releases` WHERE release_id = 'rel_a'") == [
        {"status": "promoted", "target_environment": "prod"}
    ], "the release is recorded in the target registry"
    # A second clone of an existing table is skipped, not repeated.
    h_prod.logs.clear()
    h_prod.call_macro("clone_into_location", project, "e2e_loc_phys_stg", FakeRelation(project, "e2e_loc_phys_prod", "xm__aaaaaaaaaaaa"))
    assert any("already exists; not cloning" in m for m in h_prod.logs), h_prod.logs

    # The source location goes away: the promoted view keeps working (it reads the clone).
    q(f"DROP TABLE `{project}.e2e_loc_phys_stg.xm__aaaaaaaaaaaa`")
    assert io.lookup(project, "e2e_loc_phys_stg", "xm__aaaaaaaaaaaa") is None
    assert q(f"SELECT COUNT(*) AS n, MAX(v) AS m FROM `{project}.e2e_loc_prod.xm`") == [{"n": 3, "m": "c"}]
    resp = io.requests.delete(io.dataset_url("e2e_loc_phys_stg") + "?deleteContents=true", timeout=600)
    assert resp.status_code in (200, 204), (resp.status_code, resp.text)
    assert q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_loc_prod.xm`") == [{"n": 3}]

    # B. Another project, named by `source_database` / `source_schema` (no `physical_locations`).
    io.create_datasets("e2e_src_phys", project=OTHER_PROJECT)
    io.create_datasets("e2e_loc_phys_b", "e2e_loc_prod_b")
    cfg_src = {"physical_schema": "e2e_src_phys", "physical_locations": {"default": {"database": OTHER_PROJECT, "schema": "e2e_src_phys"}}}
    h_src = io.harness(cfg_src, nodes={"model.p.xm": node}, target_name="staging", target_schema="e2e_loc_stg")
    assert h_src.call_macro("active_database") == OTHER_PROJECT
    _seed_release(io, h_src, database=OTHER_PROJECT, schema="e2e_src_phys", release_id="rel_b", model="xm", fp="bbbbbbbbbbbb", rows_sql=rows)
    cfg_b = {"clone_chain_fallback": False, "physical_schema": "e2e_loc_phys_b", "env_schemas": {"prod": "e2e_loc_prod_b"}}
    h_b = io.harness(cfg_b, nodes={"model.p.xm": node}, target_name="prod", target_schema="e2e_loc_prod_b")
    io.exec_script(h_b.call_macro("ensure_registry_tables_sql", "e2e_loc_phys_b"))
    h_b.call_macro(
        "virtual_promote", from_env="staging", to_env="prod", release_id="rel_b",
        source_database=OTHER_PROJECT, source_schema="e2e_src_phys",
    )
    assert q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_loc_phys_b.xm__bbbbbbbbbbbb`") == [{"n": 3}], "cloned across projects"
    q(f"DROP TABLE `{OTHER_PROJECT}.e2e_src_phys.xm__bbbbbbbbbbbb`")
    assert q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_loc_prod_b.xm`") == [{"n": 3}]
    body_b = view_query("e2e_loc_prod_b", "xm")
    assert "e2e_loc_phys_b" in body_b and OTHER_PROJECT not in body_b, body_b
    # Without a release the clone has nothing to register: refused, nothing changed.
    io.refused(
        lambda: h_b.call_macro("virtual_promote", from_env="staging", to_env="prod", source_database=OTHER_PROJECT, source_schema="e2e_src_phys"),
        "needs a release",
    )
    print(
        "[promote-clone] CREATE TABLE ... CLONE across datasets (physical_locations) and across projects (source_database): "
        "rows cloned, view reads the clone, source table/dataset dropped and the view still works, re-clone skipped."
    )


def _authorized_entries(io: _Io, dataset: str) -> list[dict[str, Any]]:
    """The authorized-dataset entries of `dataset`, as REST returns them."""
    return [entry for entry in (io.rest_get(io.dataset_url(dataset)).get("access") or []) if "dataset" in entry]


def step_env_view_metadata(io: _Io) -> None:
    """22-23. Env views promoted with `authorize_env_views` and persisted docs.

    22. `authorize_env_views`: ONE authorized-dataset entry on the physical dataset for the env dataset,
        not duplicated on a rerun, other access entries kept.
    23. Env view metadata: `CREATE OR REPLACE VIEW ... OPTIONS(description, labels)` and column descriptions
        (`adapter.update_columns` -> `tables.patch`) visible through `tables.get`.
    """
    project, q = io.project, io.q
    io.create_datasets("e2e_meta_phys", "e2e_meta_prod", "e2e_meta_prod2")
    description = "Gross orders: \"quoted\"; it's per order"  # `;`, quotes and an apostrophe must survive the OPTIONS literal
    nodes = {
        "model.p.meta_orders": {
            "unique_id": "model.p.meta_orders", "name": "meta_orders", "resource_type": "model", "alias": "meta_orders",
            "description": description,
            "config": {
                "materialized": "table", "virtual_env": True, "persist_docs": {"relation": True, "columns": True},
                "labels": {"team": "data", "tier": "gold"},
            },
            "columns": {
                "id": {"name": "id", "description": "Order id"},
                "amount": {"name": "amount", "description": "Amount; in cents"},
            },
        },
        "model.p.meta_plain": {
            "unique_id": "model.p.meta_plain", "name": "meta_plain", "resource_type": "model", "alias": "meta_plain",
            "description": "not persisted",
            "config": {"materialized": "table", "virtual_env": True},
        },
    }
    cfg = {
        "physical_schema": "e2e_meta_phys",
        "env_schemas": {"staging": "e2e_meta_stg", "prod": "e2e_meta_prod"},
        "authorize_env_views": True,
    }
    h = io.harness(cfg, nodes=nodes, target_name="prod", target_schema="e2e_meta_prod")
    io.exec_script(h.call_macro("ensure_registry_tables_sql", "e2e_meta_phys"))
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_meta_phys.meta_orders__aaaaaaaaaaaa` AS SELECT 1 AS id, 10.5 AS amount, 'x' AS note")
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_meta_phys.meta_plain__bbbbbbbbbbbb` AS SELECT 1 AS id")
    for model, fp in (("meta_orders", "aaaaaaaaaaaa"), ("meta_plain", "bbbbbbbbbbbb")):
        q(
            f"INSERT INTO `{project}.e2e_meta_phys._releases` (release_id, source_environment, model_name, resource_type, dependency_tier, "
            f"fingerprint, physical_identifier, status, created_at, build_quality, sealed_at) VALUES ('rel_meta', 'staging', "
            f"'{model}', 'model', 2, '{fp}', 'e2e_meta_phys.{model}__{fp}', 'candidate', CURRENT_TIMESTAMP(), 'full', CURRENT_TIMESTAMP())"
        )
    # An access entry that is not ours must survive the authorization.
    reader = {"role": "READER", "userByEmail": "analyst@example.com"}
    io.rest_patch(io.dataset_url("e2e_meta_phys"), {"access": [reader]})

    h.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="rel_meta")

    # 22. Authorization: one entry for the env dataset covering its views, added once.
    expected = {"dataset": {"dataset": {"projectId": project, "datasetId": "e2e_meta_prod"}, "targetTypes": ["VIEWS"]}}
    access = io.rest_get(io.dataset_url("e2e_meta_phys")).get("access")
    assert access == [reader, expected], access
    assert not io.rest_get(io.dataset_url("e2e_meta_prod")).get("access"), "the env dataset itself is not changed"
    # A rerun (new run, empty per-run list) finds the entry and does not duplicate it.
    view_rel = FakeRelation(project, "e2e_meta_prod", "meta_orders", "view", "`")
    h.call_macro("finalize_env_view", view_rel, nodes["model.p.meta_orders"], "e2e_meta_phys", [])
    h.call_macro("authorize_env_dataset", project, "e2e_meta_prod", "e2e_meta_phys")
    assert io.rest_get(io.dataset_url("e2e_meta_phys")).get("access") == [reader, expected]
    # Another env dataset gets its own entry; the first stays.
    h.call_macro("authorize_env_dataset", project, "e2e_meta_prod2", "e2e_meta_phys")
    assert len(_authorized_entries(io, "e2e_meta_phys")) == 2
    # Nothing to authorize when the env dataset is the physical dataset itself.
    h.call_macro("authorize_env_dataset", project, "e2e_meta_phys", "e2e_meta_phys")
    assert len(_authorized_entries(io, "e2e_meta_phys")) == 2
    # Without `authorize_env_views` the physical dataset is left alone.
    h_off = io.harness({**cfg, "authorize_env_views": False}, nodes=nodes, target_name="prod", target_schema="e2e_meta_prod")
    before = io.rest_get(io.dataset_url("e2e_meta_phys")).get("access")
    h_off.call_macro("finalize_env_view", FakeRelation(project, "e2e_meta_prod3", "v", "view", "`"), None, "e2e_meta_phys", [])
    assert io.rest_get(io.dataset_url("e2e_meta_phys")).get("access") == before
    print("[authorize_env_views] one authorized-dataset entry (targetTypes VIEWS) added by promote through datasets.patch, not duplicated on rerun, other entries kept.")

    # 23. View metadata, read back through the REST API.
    view = io.rest_get(f"{io.dataset_url('e2e_meta_prod')}/tables/meta_orders")
    assert view["type"] == "VIEW", view
    assert view.get("description") == description, view.get("description")
    assert view.get("labels") == {"team": "data", "tier": "gold"}, view.get("labels")
    fields = {f["name"]: f.get("description") for f in view["schema"]["fields"]}
    assert fields == {"id": "Order id", "amount": "Amount; in cents", "note": None}, fields
    plain = io.rest_get(f"{io.dataset_url('e2e_meta_prod')}/tables/meta_plain")
    assert plain.get("description") in (None, "") and not plain.get("labels"), plain
    assert all(not f.get("description") for f in plain["schema"]["fields"]), plain["schema"]
    # The view still reads the physical table, and the description did not break its SQL.
    assert q(f"SELECT id, amount FROM `{project}.e2e_meta_prod.meta_orders`") == [{"id": 1, "amount": 10.5}]
    # update_columns keeps the other columns' metadata when it runs again for a subset.
    io.update_columns(view_rel, {"note": {"name": "note", "description": "Free text"}})
    fields = {f["name"]: f.get("description") for f in io.rest_get(f"{io.dataset_url('e2e_meta_prod')}/tables/meta_orders")["schema"]["fields"]}
    assert fields == {"id": "Order id", "amount": "Amount; in cents", "note": "Free text"}, fields
    print("[env-view-metadata] view OPTIONS(description, labels) and column descriptions (update_columns -> tables.patch) read back through tables.get.")


def step_expire_envs(io: _Io) -> None:
    """24. `gc --expire-env-days` (`virtual_expire_envs`): abandoned environments are dropped, base / mapped kept."""
    project, q = io.project, io.q
    datasets = ("e2e_exp", "e2e_exp_stg", "e2e_exp_prod", "e2e_exp_prod_pr_old", "e2e_exp_prod_pr_new", "e2e_exp_prod_pr_mixed")
    io.create_datasets(*datasets)
    cfg = {"physical_schema": "e2e_exp", "env_schemas": {"staging": "e2e_exp_stg", "prod": "e2e_exp_prod"}, "base_environment": "prod"}
    h = io.harness(cfg, target_name="prod", target_schema="e2e_exp_prod")
    _registry(io, h, "e2e_exp")
    assert h.call_macro("env_schema_for", "pr_old") == "e2e_exp_prod_pr_old"

    # (environment, model, age in days): pr_mixed has one old and one fresh pointer, so it is NOT abandoned.
    pointers = [
        ("prod", "m_prod", 90), ("staging", "m_stg", 120),
        ("pr_old", "m_old", 40), ("pr_new", "m_new", 1),
        ("pr_mixed", "m_mix_a", 60), ("pr_mixed", "m_mix_b", 2),
    ]
    for env, model, age in pointers:
        fp = f"{abs(hash(model)) % 10**12:012d}"[:12]
        fp = (model.replace("_", "") + "000000000000")[:12]
        q(f"CREATE OR REPLACE TABLE `{project}.e2e_exp.{model}__{fp}` AS SELECT 1 AS x")
        q(
            f"INSERT INTO `{project}.e2e_exp._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, "
            f"release_id, updated_at) VALUES ('{env}', '{model}', 'model', '{fp}', 'e2e_exp.{model}__{fp}', 'r_{env}', "
            f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {age} DAY))"
        )
        schema = h.call_macro("env_schema_for", env)
        q(f"CREATE OR REPLACE VIEW `{project}.{schema}.{model}` AS SELECT * FROM `{project}.e2e_exp.{model}__{fp}`")
    # Releases: a candidate nobody promoted per abandoned/live environment, and a promoted one of the abandoned env.
    releases = [
        ("c_old", "pr_old", None, "candidate"), ("c_new", "pr_new", None, "candidate"),
        ("c_stg", "staging", None, "candidate"), ("p_old", "pr_old", "prod", "promoted"),
    ]
    for rid, source, target, status in releases:
        q(
            f"INSERT INTO `{project}.e2e_exp._releases` (release_id, source_environment, target_environment, model_name, resource_type, "
            f"dependency_tier, fingerprint, physical_identifier, status, created_at) VALUES ('{rid}', '{source}', "
            f"{'NULL' if target is None else repr(target)}, 'm_old', 'model', 2, 'mold00000000', 'e2e_exp.m_old__mold00000000', "
            f"'{status}', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 50 DAY))"
        )

    def envs() -> list[str]:
        return [r["environment"] for r in q(f"SELECT DISTINCT environment FROM `{project}.e2e_exp._environments` ORDER BY 1")]

    def release_status() -> dict[str, str]:
        return {r["release_id"]: r["status"] for r in q(f"SELECT release_id, status FROM `{project}.e2e_exp._releases`")}

    # The expiry query alone: every environment older than 30 days, including the base and mapped ones.
    old = [r["environment"] for r in q(h.call_macro("expired_environments_sql", "e2e_exp", 30))]
    assert old == ["pr_old", "prod", "staging"], old

    # Dry run: reports the abandoned environment, changes nothing.
    assert h.call_macro("virtual_expire_envs", 30, dry_run=True) == ["pr_old"]
    assert envs() == ["pr_mixed", "pr_new", "pr_old", "prod", "staging"]
    assert io.lookup(project, "e2e_exp_prod_pr_old", "m_old") is not None
    assert release_status()["c_old"] == "candidate"

    # Without an age nothing expires (gc with no --expire-env-days and no config).
    h.call_macro("virtual_gc", dry_run=False, retention_releases=5, retention_days=0)
    assert envs() == ["pr_mixed", "pr_new", "pr_old", "prod", "staging"]

    # Real run: pr_old's dataset and pointers go; base, mapped and fresh environments stay.
    h.call_macro("virtual_gc", dry_run=False, retention_releases=5, retention_days=0, expire_env_days=30)
    assert envs() == ["pr_mixed", "pr_new", "prod", "staging"], envs()
    assert io.lookup(project, "e2e_exp_prod_pr_old", "m_old") is None
    from google.api_core.exceptions import NotFound

    try:
        io.client.get_dataset(f"{project}.e2e_exp_prod_pr_old")
    except NotFound:
        pass
    else:
        raise AssertionError("the expired environment's dataset still exists")
    for kept in ("e2e_exp_prod", "e2e_exp_stg", "e2e_exp_prod_pr_new", "e2e_exp_prod_pr_mixed"):
        io.client.get_dataset(f"{project}.{kept}")
    assert io.lookup(project, "e2e_exp_prod", "m_prod") is not None and io.lookup(project, "e2e_exp_prod_pr_mixed", "m_mix_a") is not None
    # Candidate releases of the expired environment are marked; promoted ones and other environments' are not.
    assert release_status() == {"c_old": "expired", "c_new": "candidate", "c_stg": "candidate", "p_old": "promoted"}, release_status()
    # The shared physical tables are untouched (GC decides about them, not the expiry).
    assert io.lookup(project, "e2e_exp", "m_old__mold00000000") is not None

    # `expire_env_days` from the project config does the same, and the base environment is never expired.
    q(f"UPDATE `{project}.e2e_exp._environments` SET updated_at = TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY) WHERE environment = 'pr_new'")
    h_cfg = io.harness({**cfg, "expire_env_days": 30}, target_name="prod", target_schema="e2e_exp_prod")
    h_cfg.call_macro("virtual_gc", dry_run=False, retention_releases=5, retention_days=0)
    assert envs() == ["pr_mixed", "prod", "staging"], envs()
    assert release_status()["c_new"] == "expired" and release_status()["c_stg"] == "candidate"
    print(
        "[expire-envs] gc --expire-env-days: dry run reports pr_old only; real run drops its dataset and pointers, keeps prod/staging "
        "(old but base/mapped) and an env with a fresh pointer; candidates of dropped envs marked expired; config expire_env_days works."
    )


def step_shared_table_guard(io: _Io) -> None:
    """25. `assert_shared_tables_safe` (write guard) and the `DBT_VIRTUAL_FPS` precomputed fingerprints, at the macro level."""
    project, q = io.project, io.q
    io.create_datasets("e2e_grd")
    cfg = {"physical_schema": "e2e_grd", "env_schemas": {"staging": "e2e_grd_stg", "prod": "e2e_grd_prod"}}
    up = {
        "unique_id": "model.p.grd_up", "name": "grd_up", "resource_type": "model", "alias": "grd_up",
        "raw_code": "select 1 as id", "config": {"materialized": "table", "virtual_env": True}, "depends_on": {"nodes": []},
    }
    dn = {
        "unique_id": "model.p.grd_dn", "name": "grd_dn", "resource_type": "model",
        "raw_code": "select * from {{ ref('grd_up') }}", "config": {"materialized": "table", "virtual_env": True},
        "depends_on": {"nodes": ["model.p.grd_up"]},
    }
    nodes = {"model.p.grd_up": up, "model.p.grd_dn": dn}

    def harness(extra_cfg=None, extra_vars=None, which="run", target_name="staging"):
        h = io.harness({**cfg, **(extra_cfg or {})}, nodes=nodes, target_name=target_name, target_schema="e2e_grd_stg", extra_vars=extra_vars)
        h.flags.WHICH = which
        return h

    h = harness()
    _registry(io, h, "e2e_grd")
    # Parse-time naming ignores the parent: the alias carries grd_dn's own fingerprint only.
    self_fp = h.call_macro("compute_node_self_payload", dn)
    runtime_fp = h.call_macro("get_fingerprint", dn)
    assert self_fp != runtime_fp, "a parent changes the Merkle fingerprint"
    dn["alias"] = f"grd_dn__{self_fp}"  # what a bare `dbt run` would write
    up["alias"] = f"grd_up__{h.call_macro('get_fingerprint', up)}"
    phys = f"e2e_grd.{dn['alias']}"
    q(f"CREATE OR REPLACE TABLE `{project}.e2e_grd.{dn['alias']}` AS SELECT 1 AS id")

    def register(env: str, fp: str) -> None:
        q(f"DELETE FROM `{project}.e2e_grd._environments` WHERE environment = '{env}' AND model_name = 'grd_dn'")
        q(
            f"INSERT INTO `{project}.e2e_grd._environments` (environment, model_name, resource_type, fingerprint, physical_identifier, "
            f"release_id, updated_at) VALUES ('{env}', 'grd_dn', 'model', '{fp}', '{phys}', 'r_{env}', CURRENT_TIMESTAMP())"
        )

    # 1. prod serves this physical table under another fingerprint (built when the parent had a different version):
    #    a bare run in staging would rewrite it in place -> refused before anything runs.
    register("prod", "oldfp0000000")
    h = harness()
    io.refused(lambda: h.call_macro("assert_shared_tables_safe"), "refusing to rewrite physical table(s) another environment points at")
    try:
        h.call_macro("on_run_start")
    except RuntimeError as exc:
        assert f"environment prod registered fingerprint oldfp0000000, this run computes {runtime_fp}" in str(exc), exc
    else:
        raise AssertionError("on_run_start must run the guard")
    # `compile` / `run-operation` (flags.WHICH other than run/build/retry) are never refused.
    harness(which="compile").call_macro("assert_shared_tables_safe")
    # `guard_shared_tables: false` turns the guard off (and 'false'-like strings too).
    harness({"guard_shared_tables": False}).call_macro("assert_shared_tables_safe")
    harness({"guard_shared_tables": "false"}).call_macro("assert_shared_tables_safe")

    # 2. Same fingerprint as the run computes: nothing to protect.
    register("prod", runtime_fp)
    harness().call_macro("assert_shared_tables_safe")

    # 3. A pointer of the CURRENT environment (staging) never blocks the rewrite.
    register("prod", "oldfp0000000")
    q(f"DELETE FROM `{project}.e2e_grd._environments` WHERE environment = 'prod'")
    register("staging", "oldfp0000000")
    harness().call_macro("assert_shared_tables_safe")
    register("prod", "oldfp0000000")

    # 4. Precomputed fingerprints (var `_dbt_virtual_fps`, as `dbt-virtual build` passes them) skip the guard:
    #    the table names already carry the parent fingerprints.
    harness(extra_vars={"_dbt_virtual_fps": {"grd_dn": "ffffffffffff", "grd_up": "eeeeeeeeeeee"}}).call_macro("assert_shared_tables_safe")

    # 5. The DBT_VIRTUAL_FPS environment variable gives the same fingerprints to a plain `dbt run`.
    fps = {"grd_dn": "aaaaaaaaaaaa", "grd_up": "bbbbbbbbbbbb"}
    saved = os.environ.get("DBT_VIRTUAL_FPS")
    try:
        os.environ["DBT_VIRTUAL_FPS"] = json.dumps(fps)
        h = harness()
        assert h.call_macro("get_precomputed_fps") == fps
        assert h.call_macro("get_fingerprint", dn) == "aaaaaaaaaaaa"
        assert h.call_macro("runtime_fingerprint", dn) == "aaaaaaaaaaaa"
        h.call_macro("assert_shared_tables_safe")  # precomputed: no run-time recomputation, no conflict
        # The var wins over the variable.
        h_var = harness(extra_vars={"_dbt_virtual_fps": {"grd_dn": "cccccccccccc"}})
        assert h_var.call_macro("get_fingerprint", dn) == "cccccccccccc"
        # A table not covered by the precomputed set is still checked at run time.
        os.environ["DBT_VIRTUAL_FPS"] = json.dumps({"grd_up": "bbbbbbbbbbbb"})
        io.refused(lambda: harness().call_macro("assert_shared_tables_safe"), "grd_dn")
        # require_cli_fingerprints: met by the variable, not met without it.
        os.environ["DBT_VIRTUAL_FPS"] = json.dumps(fps)
        harness({"require_cli_fingerprints": True}).call_macro("assert_cli_fingerprints")
        del os.environ["DBT_VIRTUAL_FPS"]
        io.refused(lambda: harness({"require_cli_fingerprints": True}).call_macro("assert_cli_fingerprints"), "require_cli_fingerprints is true")
        # A value that is not a JSON object is rejected with a pointer to `plan --print-fps`.
        os.environ["DBT_VIRTUAL_FPS"] = "[1, 2]"
        io.refused(lambda: harness().call_macro("get_precomputed_fps"), "DBT_VIRTUAL_FPS must be a JSON object")
    finally:
        if saved is None:
            os.environ.pop("DBT_VIRTUAL_FPS", None)
        else:
            os.environ["DBT_VIRTUAL_FPS"] = saved
    # The refused run changed nothing: prod still points at its table, which still has its row.
    assert q(f"SELECT fingerprint FROM `{project}.e2e_grd._environments` WHERE environment = 'prod'") == [{"fingerprint": "oldfp0000000"}]
    assert q(f"SELECT COUNT(*) AS n FROM `{project}.e2e_grd.{dn['alias']}`") == [{"n": 1}]
    print(
        "[shared-table-guard] a bare run is refused when another env serves the table under another fingerprint (conflict text, on_run_start); "
        "allowed for the current env, same fingerprint, compile, guard off; DBT_VIRTUAL_FPS / _dbt_virtual_fps skip it."
    )
