"""SQL shape of `virtual_data_diff` on the BigQuery adapter (mock dbt runtime, no warehouse)."""

from __future__ import annotations

import json
from types import SimpleNamespace

from tests.test_macros_execution import FakeResult, _bq_harness


def test_bigquery_data_diff_sql_shape_checksums_then_leaf_join() -> None:
    """Segment checksums use BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...)))); only differing leaves are joined."""
    harness = _bq_harness()
    harness.graph["nodes"]["model.p.events"] = {
        "name": "events",
        "resource_type": "model",
        "config": {
            "unique_key": "id",
            "partition_by": {"field": "ts", "data_type": "timestamp"},
            "diff_exclude_columns": ["loaded_at"],
        },
    }
    cols = [SimpleNamespace(name=n) for n in ("id", "ts", "v", "loaded_at")]
    harness.adapter_extras["get_columns_in_relation"] = lambda rel: cols
    harness.adapter_extras["quote"] = lambda name: f"`{name}`"
    harness.query_responses.extend(
        [
            FakeResult([("events", "analytics_physical.events__fp2")]),
            FakeResult([("events", "analytics_physical.events__fp1")]),
            FakeResult([("2024-01-01", 5, 111, 1, 5), ("2024-01-02", 4, 222, 6, 9)]),  # candidate
            FakeResult([("2024-01-01", 5, 111, 1, 5), ("2024-01-02", 4, 999, 6, 9)]),  # target
            FakeResult([(0, 0, 1, 0, 1)]),  # leaf aggregate for 2024-01-02: `v` changed
            FakeResult([("changed", 7, 0, 1)]),  # sample
        ]
    )
    harness.call_macro("virtual_data_diff", from_env="staging", to_env="prod", threshold=10)
    q = harness.executed_queries
    assert len(q) == 6
    assert "BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(`id`, `ts`, `v`))))" in q[2]
    assert "loaded_at" not in q[2]
    assert "GROUP BY 1" in q[2] and "DATE(`ts`) AS dv_seg" in q[2]
    assert "`test_proj`.`analytics_physical`.`events__fp2`" in q[2]
    assert "`test_proj`.`analytics_physical`.`events__fp1`" in q[3]
    # Only the differing partition is joined; the identical one never is.
    assert sum("FULL OUTER JOIN" in x for x in q) == 2  # aggregate + sample, one leaf
    assert "DATE(`ts`) = DATE '2024-01-02'" in q[4]
    assert "2024-01-01" not in q[4]
    assert "TO_JSON_STRING(dv_c.`v`) IS DISTINCT FROM TO_JSON_STRING(dv_t.`v`)" in q[4]
    line = next(m for m in harness.logs if m.startswith("DBT_VIRTUAL_DIFF_JSON:"))
    model = json.loads(line.split(":", 1)[1])["models"][0]
    assert (model["changed"], model["only_candidate"], model["only_target"]) == (1, 0, 0)
    assert model["segments_identical"] == 1
    assert model["column_mismatches"] == {"ts": 0, "v": 1}
