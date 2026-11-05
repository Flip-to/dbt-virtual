"""Row-level data diff (`virtual_data_diff`, `dbt-virtual diff-data`) on real dbt-core + dbt-duckdb.

Prod holds 200 rows over 4 daily partitions. Staging differs by: 3 changed values (all in the
2024-01-01 partition), 1 row missing (2024-01-04) and 1 extra row (2024-01-02); the partition
2024-01-03 is identical. `loaded_at` differs on every row and is excluded by config.
"""

from __future__ import annotations

import json

import pytest

from tests.test_dbt_duckdb_regressions import Project

MARKER = "DBT_VIRTUAL_DIFF_JSON:"

PROD_SQL = """{{ config(materialized='table', unique_key='id',
   meta={'diff_partition_by': {'field': 'dt', 'data_type': 'date'}}, diff_exclude_columns=['loaded_at']) }}
select i as id, date '2024-01-01' + cast(i % 4 as integer) as dt, 'v' || i as v, 'a' as loaded_at
from range(1, 201) t(i)
"""

# 3 changed values (ids 4, 8, 12 -> 2024-01-01), id 3 removed (2024-01-04), id 201 added (2024-01-02).
STAGING_SQL = """{{ config(materialized='table', unique_key='id',
   meta={'diff_partition_by': {'field': 'dt', 'data_type': 'date'}}, diff_exclude_columns=['loaded_at']) }}
select i as id, date '2024-01-01' + cast(i % 4 as integer) as dt,
  case when i in (4, 8, 12) then 'changed' else 'v' || i end as v, 'b' as loaded_at
from range(1, 202) t(i)
where i != 3
"""

# Same rows as prod, written differently: a new fingerprint but no data difference.
SAME_SQL = PROD_SQL.replace("select i as id", "select i + 0 as id")


@pytest.fixture
def prod_and_staging(tmp_path):
    proj = Project(tmp_path, {"events": PROD_SQL})
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    proj.operation("virtual_promote", {"from_env": "staging", "to_env": "prod", "release_id": "r1"})
    return proj


def _diff(proj: Project, **args) -> dict:
    op = {"from_env": "staging", "to_env": "prod", "threshold": 10, "branching": 4, **args}
    res = proj.operation("virtual_data_diff", op, expect_ok=args.get("max_mismatch_pct") is None)
    for line in res.stdout.splitlines():
        if MARKER in line:
            return json.loads(line.split(MARKER, 1)[1])
    raise AssertionError(f"no {MARKER} line:\n{res.stdout}\n{res.stderr}")


def test_diff_finds_exact_rows_and_drills_only_differing_segments(prod_and_staging) -> None:
    proj = prod_and_staging
    proj.write_models({"events": STAGING_SQL})
    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0

    model = _diff(proj)["models"][0]
    assert (model["only_candidate"], model["only_target"], model["changed"]) == (1, 1, 3)
    assert model["column_mismatches"] == {"dt": 0, "v": 3}  # loaded_at is excluded
    assert model["excluded_columns"] == ["loaded_at"]
    assert model["candidate_rows"] == 200 and model["target_rows"] == 200
    # 4 partitions: 2024-01-03 is identical and never joined.
    assert model["segments_total"] == 4 and model["segments_identical"] == 1
    drilled = {leaf["segment"].split()[0] for leaf in model["leaf_segments"]}
    assert drilled == {"partition=2024-01-01", "partition=2024-01-02", "partition=2024-01-04"}
    assert all("partition=2024-01-03" not in leaf["segment"] for leaf in model["leaf_segments"])
    # 50 rows per partition > threshold 10: ranges were split, and only small ranges were joined.
    assert model["segments_split"] >= 3
    assert all(max(leaf["candidate_rows"], leaf["target_rows"]) <= 10 for leaf in model["leaf_segments"])
    assert model["queries"]["join_queries"] == len(model["leaf_segments"])
    assert {s["kind"] for s in model["sample"]} == {"changed", "only_candidate", "only_target"}
    assert sorted(s["key"]["id"] for s in model["sample"] if s["kind"] == "changed") == ["12", "4", "8"]
    assert model["mismatch_pct"] == pytest.approx(5 * 100.0 / 200, abs=1e-3)


def test_identical_tables_do_no_join(prod_and_staging) -> None:
    proj = prod_and_staging
    proj.write_models({"events": SAME_SQL})
    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0
    summary = _diff(proj)
    model = summary["models"][0]
    assert model["model"] == "events" and model["status"] == "identical"
    assert (model["only_candidate"], model["only_target"], model["changed"]) == (0, 0, 0)
    assert model["queries"] == {"checksum_queries": 2, "join_queries": 0, "sample_queries": 0}
    assert summary["passed"] is True


def test_diff_gate_and_cli(prod_and_staging, capsys) -> None:
    proj = prod_and_staging
    proj.write_models({"events": STAGING_SQL})
    assert proj.cli("build", "--env", "staging", "--release", "r2") == 0

    res = proj.operation(
        "virtual_data_diff", {"from_env": "staging", "to_env": "prod", "max_mismatch_pct": 1.0}, expect_ok=False
    )
    assert res.returncode != 0 and "exceeds max_mismatch_pct=1.0" in res.stdout

    assert proj.cli("diff-data", "--from", "staging", "--to", "prod", "--format", "json", "--threshold", "10") == 0
    out = json.loads(capsys.readouterr().out)
    assert out["passed"] is True and out["models"][0]["changed"] == 3

    assert proj.cli("diff-data", "--max-mismatch-pct", "1.0", "--select", "events") != 0
    # promote --data-diff gates before repointing the views.
    rc = proj.cli("promote", "--release", "r2", "--data-diff", "--max-mismatch-pct", "1.0")
    assert rc != 0
    assert proj.query("SELECT count(*) FROM analytics.events") == [(200,)]
    assert proj.query("SELECT v FROM analytics.events WHERE id = 4") == [("v4",)]
    assert proj.cli("promote", "--release", "r2", "--data-diff", "--max-mismatch-pct", "10.0") == 0
    assert proj.query("SELECT v FROM analytics.events WHERE id = 4") == [("changed",)]
