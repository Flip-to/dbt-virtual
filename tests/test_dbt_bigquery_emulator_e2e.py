"""dbt-virtual lifecycle on real dbt-core + dbt-bigquery against a BigQuery emulator.

Until now dbt never ran on the BigQuery dialect in CI: `scripts/run_emulator_e2e.py` renders the
macros in a Python harness. Here every command is the real thing: `dbt-virtual plan | build |
promote | audit | rollback | init-env | drop-env | gc | status | diff-data` shell out to dbt-bigquery,
which talks to `Flip-to/bigquery-emulator` (`DBT_VIRTUAL_BQ_EMULATOR`, see `bq_emulator_project.py`).

The tests skip when no emulator is configured. CI sets `DBT_VIRTUAL_REQUIRE_BQ_EMULATOR=1` so that a
missing emulator or adapter fails the job instead of skipping.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tests.bq_emulator_project import PROJECT, BqProject, emulator_unavailable_reason

_REASON = emulator_unavailable_reason()
if _REASON and os.environ.get("DBT_VIRTUAL_REQUIRE_BQ_EMULATOR") == "1":
    raise RuntimeError(f"DBT_VIRTUAL_REQUIRE_BQ_EMULATOR=1 but: {_REASON}")
pytestmark = pytest.mark.skipif(_REASON is not None, reason=_REASON or "")

ORDERS = (
    "{{ config(materialized='table', partition_by={'field': 'order_date', 'data_type': 'date'},"
    " cluster_by=['id']) }}\n"
    "select 1 as id, date '2026-01-01' as order_date, 10 as amount\n"
    "union all select 2, date '2026-01-02', 20\n"
)
EVENTS_V1 = (
    "{{ config(materialized='incremental', unique_key='event_id',"
    " partition_by={'field': 'event_date', 'data_type': 'date'}) }}\n"
    "select 1 as event_id, date '2026-01-01' as event_date, 'a' as kind\n"
    "union all select 2, date '2026-01-02', 'b'\n"
    "{% if is_incremental() %} union all select 3, date '2026-01-03', 'c' {% endif %}\n"
)
# v2 changes the transformation (new fingerprint): `kind` is upper-cased.
EVENTS_V2 = EVENTS_V1.replace("'a' as kind", "'A' as kind").replace("'b'", "'B'").replace("'c'", "'C'")
VIEW = "{{ config(materialized='view') }}\nselect id, amount * 2 as dbl from {{ ref('orders') }}\n"
UDF = "{{ config(materialized='virtual_function', parameters='n INT64', returns='INT64') }}\nn * 2\n"
TVF = (
    "{{ config(materialized='table_function', parameters='min_id INT64') }}\n"
    "select id, amount from {{ ref('orders') }} where id >= min_id\n"
)
MODELS = {
    "orders": ORDERS,
    "events": EVENTS_V1,
    "orders_v": VIEW,
    "udf_double": UDF,
    "tvf_orders": TVF,
}


@pytest.fixture
def proj(tmp_path: Path):
    p = BqProject(tmp_path, dict(MODELS))
    try:
        yield p
    finally:
        p.drop_everything()



def _fq(p: BqProject, dataset: str, name: str) -> str:
    return f"`{p.client.project}.{dataset}.{name}`"


def test_full_lifecycle_on_real_dbt_bigquery(proj: BqProject, capfd: pytest.CaptureFixture) -> None:
    from google.api_core.exceptions import NotFound

    prod, stg, phys = proj.schemas["prod"], proj.schemas["staging"], proj.physical

    # plan + build release r1 in staging, then promote to prod.
    assert proj.cli("plan", "--env", "staging") == 0
    fp1 = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))["by_name"]
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0
    assert (f"orders__{fp1['orders']}", "TABLE") in proj.tables(phys)
    assert proj.query(f"select id, dbl from {_fq(proj, stg, 'orders_v')} order by id") == [(1, 20), (2, 40)]
    assert proj.query(f"select {_fq(proj, stg, 'udf_double')}(21)") == [(42,)]
    assert proj.query(f"select id from {_fq(proj, stg, 'tvf_orders')}(2)") == [(2,)]

    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r1") == 0
    assert proj.query(f"select id, dbl from {_fq(proj, prod, 'orders_v')} order by id") == [(1, 20), (2, 40)]
    assert proj.query(f"select count(*) from {_fq(proj, prod, 'orders')}") == [(2,)]
    assert proj.query(f"select {_fq(proj, prod, 'udf_double')}(5)") == [(10,)]
    assert proj.query(f"select id from {_fq(proj, prod, 'tvf_orders')}(1) order by id") == [(1,), (2,)]

    # A row only the clone can carry: a v2 build that seeds from prod's v1 table keeps it.
    fp_events_v1 = fp1["events"]
    proj.query(
        f"insert into {_fq(proj, phys, 'events__' + fp_events_v1)} values (99, date '2026-01-09', 'seeded')"
    )

    # Release r2 changes `events` only: `orders` is REUSE, `events` is seeded by CLONE from prod.
    # A row that only an in-place rebuild of `orders` (create or replace) would remove: with the
    # state of the warehouse, `orders` is REUSE and is not rebuilt.
    orders_v1 = _fq(proj, phys, "orders__" + fp1["orders"])
    proj.query(f"insert into {orders_v1} values (77, date '2026-01-07', 700)")
    state = str(proj.dir / "state.json")
    assert proj.cli("status", "--env", "staging", "--output", state) == 0

    proj.write_models({"events": EVENTS_V2})
    assert proj.cli("plan", "--env", "staging") == 0
    fp2 = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))["by_name"]
    assert fp2["events"] != fp1["events"] and fp2["orders"] == fp1["orders"]
    assert proj.cli("build", "--env", "staging", "--release", "r2", "--state-file", state) == 0
    assert proj.query(f"select count(*) from {orders_v1} where id = 77") == [(1,)]
    seeded = proj.query(
        f"select event_id, kind from {_fq(proj, phys, 'events__' + fp2['events'])} order by event_id"
    )
    assert seeded == [(1, "A"), (2, "B"), (3, "C"), (99, "seeded")]

    assert proj.cli("audit", "--from", "staging", "--to", "prod", "--release", "r2") == 0
    assert proj.cli("promote", "--from", "staging", "--to", "prod", "--release", "r2") == 0
    assert proj.query(f"select count(*) from {_fq(proj, prod, 'events')}") == [(4,)]

    # Instant rollback to r1: prod's `events` view points at the v1 table again.
    assert proj.cli("rollback", "--env", "prod") == 0
    assert proj.query(f"select count(*) from {_fq(proj, prod, 'events')}") == [(3,)]
    assert proj.query(f"select kind from {_fq(proj, prod, 'events')} where event_id = 1") == [("a",)]

    # Ephemeral environment: zero-compute views over prod's tables, then drop.
    pr1 = proj.schemas["pr_1"]
    assert proj.cli("init-env", "--env", "pr_1", "--from", "prod") == 0
    assert proj.query(f"select count(*) from {_fq(proj, pr1, 'orders')}") == [(3,)]  # incl. row 77
    assert proj.cli("drop-env", "--env", "pr_1") == 0
    with pytest.raises(NotFound):
        proj.tables(pr1)

    # Janitor, status and the row-level data diff.
    assert proj.cli("gc", "--dry-run") == 0
    assert proj.cli("status", "--env", "staging", "--output", str(proj.dir / "state2.json")) == 0
    state_after = json.loads((proj.dir / "state2.json").read_text("utf-8"))
    assert "events" in json.dumps(state_after)
    capfd.readouterr()
    assert proj.cli("diff-data", "--from", "staging", "--to", "prod", "--format", "json") == 0
    out = capfd.readouterr().out
    summary = json.loads(out[out.index("{") :])
    by_model = {m["model"]: m for m in summary["models"]}
    # Staging holds r2 (`events` upper-cased, plus row 99), prod serves r1 again after the rollback.
    assert by_model["events"]["status"] == "differs", by_model["events"]
    assert by_model["events"]["only_candidate"] + by_model["events"]["changed"] > 0
    assert by_model["orders"]["status"] == "identical", by_model["orders"]


def test_build_select_never_rebuilds_a_reuse_table(proj: BqProject) -> None:
    """`build --select` narrows the plan: a REUSE table that the selector matches is not rebuilt."""
    phys = proj.physical
    assert proj.cli("plan", "--env", "staging") == 0
    fp1 = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))["by_name"]
    assert proj.cli("build", "--env", "staging", "--release", "r1") == 0

    # A row that only an in-place rebuild of `orders` (create or replace) would remove.
    orders_v1 = _fq(proj, phys, "orders__" + fp1["orders"])
    proj.query(f"insert into {orders_v1} values (77, date '2026-01-07', 700)")

    state = str(proj.dir / "state.json")
    assert proj.cli("status", "--env", "staging", "--output", state) == 0
    proj.write_models({"events": EVENTS_V2})
    args = ["build", "--env", "staging", "--release", "r2", "--state-file", state]
    assert proj.cli(*args, "--select", "orders events") == 0
    assert proj.query(f"select count(*) from {orders_v1} where id = 77") == [(1,)]
    fp2_events = json.loads((proj.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))[
        "by_name"
    ]["events"]
    # Nothing is promoted yet, so there is no base table to seed from: `events` is built in full
    # (2 rows; the third only appears on an incremental run).
    assert proj.query(f"select count(*) from {_fq(proj, phys, 'events__' + fp2_events)}") == [(2,)]


def test_fingerprints_do_not_depend_on_dbt_empty(tmp_path: Path) -> None:
    """`DBT_EMPTY=true` wraps `source()` / `ref()` at compile time; the CLI must not hash that."""
    p = BqProject(
        tmp_path,
        {
            "raw_summary": "{{ config(materialized='table') }}\nselect count(*) as n from {{ source('raw', 'raw_events') }}\n",
            "derived": "{{ config(materialized='table') }}\nselect n + 1 as n from {{ ref('raw_summary') }}\n",
        },
    )
    try:
        assert p.cli("plan", "--env", "staging") == 0
        full = json.loads((p.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))["by_name"]
        assert p.cli("plan", "--env", "staging", env={"DBT_EMPTY": "true"}) == 0
        empty = json.loads((p.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))["by_name"]
        assert empty == full
    finally:
        p.drop_everything()


MULTI_MODELS = {
    "orders": "{{ config(materialized='table', schema='gold') }}\n"
    "select 1 as id, 10 as amount union all select 2, 25\n",
    "daily": "{{ config(materialized='table') }}\nselect 7 as d\n",
    "big_total": "{{ config(materialized='virtual_function', schema='spacetime', parameters='min_amount INT64',"
    " returns='INT64') }}\n(select sum(amount) from {{ ref('orders') }} where amount >= min_amount)\n",
    "gold_rows": "{{ config(materialized='table_function', schema='spacetime', parameters='min_id INT64') }}\n"
    "select id from {{ ref('orders') }} where id >= min_id\n",
}


def test_multi_dataset_environments_refresh_and_demotion_on_bigquery(tmp_path: Path) -> None:
    """Prod spreads objects over datasets by custom schema (`gold`, `spacetime`); staging uses one.

    Covers promotion of tables, a scalar UDF and a TVF into their own datasets, `dbt-virtual run`
    (refresh in place), rollback by the recorded schema of each object and demotion of an object
    removed from the project.
    """
    p = BqProject(
        tmp_path,
        MULTI_MODELS,
        prod_datasets={"default": "prod", "gold": "prod_gold", "spacetime": "prod_spacetime"},
    )
    try:
        prod = f"p{p.tag}_prod"
        gold = f"p{p.tag}_prod_gold"
        space = f"p{p.tag}_prod_spacetime"
        assert p.cli("build", "--env", "staging", "--release", "r1") == 0
        assert p.cli("promote", "--from", "staging", "--to", "prod", "--release", "r1") == 0

        assert [n for n, _ in p.tables(prod)] == ["daily"]
        assert [n for n, _ in p.tables(gold)] == ["orders"]
        assert p.query(f"select {_fq(p, space, 'big_total')}(20)") == [(25,)]
        assert p.query(f"select id from {_fq(p, space, 'gold_rows')}(2)") == [(2,)]
        keys = dict(
            p.query(
                f"select model_name, schema_key from `{PROJECT}.{p.physical}._environments` "
                "where environment = 'prod'"
            )
        )
        assert keys == {"orders": "gold", "daily": None, "big_total": "spacetime", "gold_rows": "spacetime"}

        # `dbt-virtual run` refreshes the unchanged tables of prod in place.
        assert p.cli("run", "--env", "prod", "--select", "daily") == 0
        assert p.query(f"select d from `{PROJECT}.{prod}.daily`") == [(7,)]

        # r2 changes `daily`; rollback restores r1 by the recorded schema of each object.
        p.write_models({"daily": "{{ config(materialized='table') }}\nselect 8 as d\n"})
        assert p.cli("build", "--env", "staging", "--release", "r2") == 0
        assert p.cli("promote", "--from", "staging", "--to", "prod", "--release", "r2") == 0
        assert p.query(f"select d from `{PROJECT}.{prod}.daily`") == [(8,)]
        assert p.cli("rollback", "--env", "prod") == 0
        assert p.query(f"select d from `{PROJECT}.{prod}.daily`") == [(7,)]

        # A routine removed from the project is demoted (dropped from prod) by the next promote.
        (p.dir / "models" / "gold_rows.sql").unlink()
        assert p.cli("build", "--env", "staging", "--release", "r3") == 0
        assert p.cli("promote", "--from", "staging", "--to", "prod", "--release", "r3") == 0
        assert p.query(f"select {_fq(p, space, 'big_total')}(20)") == [(25,)]
        with pytest.raises(Exception, match="(?i)not found|gold_rows"):
            p.query(f"select id from {_fq(p, space, 'gold_rows')}(2)")
    finally:
        p.drop_everything()


def _table_kind(p: BqProject, dataset: str, ident: str) -> tuple:
    rows = p.query(
        f"select table_type, base_table_name from `{PROJECT}`.`{dataset}`.INFORMATION_SCHEMA.TABLES"
        f" where table_name = '{ident}'"
    )
    assert len(rows) == 1, (dataset, ident, rows)
    return rows[0]


def test_adopt_clones_legacy_tables_through_the_real_clone_script(proj: BqProject) -> None:
    """`dbt-virtual adopt` on BigQuery (regression of the adopt split on `;`, Flip-to/dbt-virtual#36).

    On BigQuery `clone_table_sql` is a `BEGIN ... EXCEPTION ... END` script. adopt used to cut it at
    every `;` and real BigQuery refused the pieces (`Syntax error: Expected ";" but got end of
    script`). The emulator runs the script as BigQuery does, so this test fails if adopt splits it
    again. `events` is a legacy table already 3 clones deep: adopt copies it instead of failing.
    """
    prod, phys = proj.schemas["prod"], proj.physical
    # The legacy deployment, written by hand in prod's dataset (not by dbt-virtual).
    proj.query(f"create table `{PROJECT}.{prod}.orders` as select 1 as id, 10 as amount union all select 2, 20")
    proj.query(f"create table `{PROJECT}.{proj.raw}.e0` as select 1 as event_id union all select 2")
    proj.query(f"create table `{PROJECT}.{proj.raw}.e1` clone `{PROJECT}.{proj.raw}.e0`")
    proj.query(f"create table `{PROJECT}.{proj.raw}.e2` clone `{PROJECT}.{proj.raw}.e1`")
    proj.query(f"create table `{PROJECT}.{prod}.events` clone `{PROJECT}.{proj.raw}.e2`")
    assert _table_kind(proj, prod, "events") == ("CLONE", "e2")

    fp = proj.fingerprints("prod")
    assert proj.cli("adopt", "--env", "prod", "--release", "adopt1", "--skip-compile") == 0

    # orders: cloned (depth 1). events: 3 clones deep, so the 4th link is a copy.
    assert _table_kind(proj, phys, f"orders__{fp['orders']}")[0] == "CLONE"
    assert _table_kind(proj, phys, f"events__{fp['events']}") == ("BASE TABLE", None)
    assert proj.query(f"select count(*) from `{PROJECT}.{phys}.events__{fp['events']}`") == [(2,)]
    # prod now serves views over the physical tables; the legacy tables are gone.
    assert dict(proj.tables(prod))["orders"] == "VIEW"
    assert dict(proj.tables(prod))["events"] == "VIEW"
    assert proj.query(f"select id, amount from `{PROJECT}.{prod}.orders` order by id") == [(1, 10), (2, 20)]
    assert proj.query(f"select count(*) from `{PROJECT}.{prod}.events`") == [(2,)]
    pointers = dict(
        proj.query(
            f"select model_name, physical_identifier from `{PROJECT}.{phys}._environments`"
            " where environment = 'prod' and model_name in ('orders', 'events')"
        )
    )
    assert pointers["orders"].endswith(f"orders__{fp['orders']}"), pointers
    assert pointers["events"].endswith(f"events__{fp['events']}"), pointers
