"""Opt-in lifecycle E2E of dbt-virtual against REAL BigQuery, in throwaway sandbox datasets.

Not run in CI: it needs a GCP project where you may create datasets, and it spends (tiny) query
cost. Every dataset it creates is named `dbt_virtual_sbx_<run>_...` and is dropped at the end
(unless `--keep`).

Requirements: a Python with dbt-core + dbt-bigquery + google-cloud-bigquery (run this script
with that interpreter; its `Scripts`/`bin` directory goes first on PATH so the `dbt-virtual` CLI
calls that `dbt`), and Application Default Credentials (`gcloud auth application-default login`).

    python scripts/run_bigquery_sandbox_e2e.py --project <gcp-project> [--location US] [--keep]
    # or: DBT_VIRTUAL_SBX_PROJECT=<gcp-project> python scripts/run_bigquery_sandbox_e2e.py

The temp dbt project has:
- `events`: incremental model (default schema),
- `orders`, `dim_static`: table models in custom schema `gold` (`orders` reads `events`),
- `big_orders`: `table_function` in custom schema `spacetime` that reads `orders` via ref(),
- `on-run-start: seed_incremental_clones()`, post-hook `sync_env_view()`, the routing overrides
  of docs/guides/getting_started.md, and `env_schemas` with a single staging dataset and a
  per-custom-schema prod layout (`default`, derived `<prod>_gold`, explicit `spacetime`).

It drives the `dbt-virtual` CLI (build/status/plan/promote/rollback/audit/init-env/drop-env/gc)
and `dbt run-operation`, and checks rows, object locations and routine bodies after each step.
`macros/`, `dbt_project.yml` and `src/dbt_virtual` are copied when the run starts, so edits made
to the working tree during a run do not leak into it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SANDBOX_PREFIX = "dbt_virtual_sbx_"

EVENTS_V1 = """{{ config(materialized='incremental', unique_key='id') }}
select id, v, amount
from unnest([
  struct(1 as id, 'a' as v, 10 as amount),
  struct(2 as id, 'b' as v, 25 as amount),
  struct(3 as id, 'c' as v, 40 as amount)
])
{% if is_incremental() %}
where id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
"""

# v2 changes the transformation (new fingerprint) and has one new source row. When the new
# table is seeded by CLONE, ids 1-3 keep v1's lower-case `v`; a full rebuild would upper-case them.
EVENTS_V2 = """{{ config(materialized='incremental', unique_key='id') }}
select id, upper(v) as v, amount
from unnest([
  struct(1 as id, 'a' as v, 10 as amount),
  struct(2 as id, 'b' as v, 25 as amount),
  struct(3 as id, 'c' as v, 40 as amount),
  struct(4 as id, 'd' as v, 55 as amount)
])
{% if is_incremental() %}
where id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
"""

ORDERS = """{{ config(materialized='table', schema='gold') }}
select id, amount from {{ ref('events') }}
"""

DIM_STATIC = """{{ config(materialized='table', schema='gold') }}
select 1 as k, 'static' as label
"""

BIG_ORDERS = """{{ config(materialized='table_function', schema='spacetime',
    parameters='min_amount INT64') }}
select id, amount from {{ ref('orders') }} where amount >= min_amount
"""

# docs/guides/getting_started.md, Step 3 (schema/alias routing and the ref() override).
ROUTING_OVERRIDES = """{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_schema_name(custom_schema_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_schema_name(custom_schema_name, node)) }}
{%- endmacro %}


{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
  {%- if dbt_virtual.is_virtual_node(node) -%}
    {{ return(dbt_virtual.virtual_alias_name(custom_alias_name, node)) }}
  {%- endif -%}
  {{ return(dbt.generate_alias_name(custom_alias_name, node)) }}
{%- endmacro %}


{% macro ref(model_name) -%}
  {{ return(dbt_virtual.virtual_ref(model_name, packagename=none)) }}
{%- endmacro %}
"""


class StepError(RuntimeError):
    pass


class Sandbox:
    def __init__(self, ns: argparse.Namespace) -> None:
        self.project = ns.project
        self.location = ns.location
        self.run = ns.run_id or secrets.token_hex(3)
        self.sbx = f"{SANDBOX_PREFIX}{self.run}"
        if not self.sbx.startswith(SANDBOX_PREFIX) or not self.run.replace("_", "").isalnum():
            raise SystemExit(f"refusing sandbox name {self.sbx!r}")
        self.ds = {
            "physical": f"{self.sbx}_physical",
            "staging": f"{self.sbx}_staging",
            "prod": f"{self.sbx}_prod",
            "prod_gold": f"{self.sbx}_prod_gold",
            "spacetime": f"{self.sbx}_spacetime",
            "pr_1": f"{self.sbx}_prod_pr_1",
        }
        self.adapter = "bigquery"
        self.threads = ns.threads
        self.keep = ns.keep
        self.root = Path(ns.workdir or tempfile.mkdtemp(prefix="dbt_virtual_sbx_"))
        self.proj = self.root / "proj"
        self.logs = self.root / "step_logs"
        self.bin_dir = Path(ns.dbt_bin_dir or Path(sys.executable).parent)
        self.dbt_exe = shutil.which("dbt", path=str(self.bin_dir))
        if not self.dbt_exe:
            raise SystemExit(f"no dbt executable in {self.bin_dir}; pass --dbt-bin-dir")
        self.results: list[tuple[str, bool, str]] = []
        self.step_no = 0

        from google.cloud import bigquery  # optional dependency, only needed here

        self.bq = bigquery
        self.client = bigquery.Client(project=self.project, location=self.location)

    # ---------------------------------------------------------------- setup / teardown
    def write_project(self) -> None:
        pkg = self.root / "dbt_virtual_pkg"
        src = self.root / "cli_src"
        self.proj.mkdir(parents=True)
        (self.proj / "models").mkdir()
        (self.proj / "macros").mkdir()
        self.logs.mkdir()
        pkg.mkdir()
        # Snapshot of the package and CLI as they are now.
        shutil.copy2(REPO / "dbt_project.yml", pkg / "dbt_project.yml")
        shutil.copytree(REPO / "macros", pkg / "macros")
        shutil.copytree(REPO / "src" / "dbt_virtual", src / "dbt_virtual")
        self.cli_src = src
        (self.proj / "packages.yml").write_text(
            f'packages:\n  - local: "{pkg.as_posix()}"\n', encoding="utf-8"
        )
        d = self.ds
        (self.proj / "dbt_project.yml").write_text(
            f"""name: 'dbt_virtual_sbx'
version: '1.0.0'
config-version: 2
profile: 'dbt_virtual_sbx'
model-paths: ["models"]
macro-paths: ["macros"]
on-run-start:
  - "{{{{ dbt_virtual.seed_incremental_clones() }}}}"
vars:
  dbt_virtual:
    enabled: true
    physical_schema: "{d['physical']}"
    environment: "{{{{ env_var('DBT_VIRTUAL_ENV', target.name) }}}}"
    base_environment: "prod"
    ref_resolution: "env_view"
    env_schemas:
      staging: "{d['staging']}"
      prod:
        default: "{d['prod']}"
        spacetime: "{d['spacetime']}"
    retention_releases: 5
    retention_days: 7
models:
  dbt_virtual_sbx:
    +virtual_env: true
    +post-hook:
      - "{{{{ dbt_virtual.sync_env_view() }}}}"
""",
            encoding="utf-8",
        )
        outputs = ""
        for target, dataset in (("staging", d["staging"]), ("prod", d["prod"])):
            outputs += (
                f"    {target}:\n      type: bigquery\n      method: oauth\n"
                f"      project: {self.project}\n      dataset: {dataset}\n"
                f"      location: {self.location}\n      threads: {self.threads}\n"
                "      priority: interactive\n      job_retries: 1\n"
                "      job_execution_timeout_seconds: 600\n"
                "      maximum_bytes_billed: 10000000000\n"
            )
        (self.proj / "profiles.yml").write_text(
            f"dbt_virtual_sbx:\n  target: staging\n  outputs:\n{outputs}", encoding="utf-8"
        )
        (self.proj / "macros" / "virtual_routing_overrides.sql").write_text(
            ROUTING_OVERRIDES, encoding="utf-8"
        )
        self.write_models(
            {"events": EVENTS_V1, "orders": ORDERS, "dim_static": DIM_STATIC,
             "big_orders": BIG_ORDERS}
        )
        self.env = {
            **os.environ,
            "PATH": str(self.bin_dir) + os.pathsep + os.environ.get("PATH", ""),
            "PYTHONPATH": str(self.cli_src),
            "DBT_PROFILES_DIR": str(self.proj),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "False",
            "PYTHONWARNINGS": "ignore",
        }
        self.env.pop("DBT_VIRTUAL_ENV", None)

    def write_models(self, models: dict[str, str]) -> None:
        for name, sql in models.items():
            (self.proj / "models" / f"{name}.sql").write_text(sql, encoding="utf-8")

    def sandbox_datasets(self) -> list[str]:
        return sorted(
            ds.dataset_id
            for ds in self.client.list_datasets(project=self.project)
            if ds.dataset_id == self.sbx or ds.dataset_id.startswith(self.sbx + "_")
        )

    def cleanup(self) -> list[str]:
        dropped = []
        for dataset_id in self.sandbox_datasets():
            assert dataset_id.startswith(SANDBOX_PREFIX)
            self.client.delete_dataset(
                f"{self.project}.{dataset_id}", delete_contents=True, not_found_ok=True
            )
            dropped.append(dataset_id)
        return dropped

    # ---------------------------------------------------------------- process helpers
    def _run(self, label: str, cmd: list[str],
             expect_ok: bool | None) -> subprocess.CompletedProcess:
        self.step_no += 1
        t0 = time.time()
        res = subprocess.run(
            cmd, cwd=self.proj, env=self.env, capture_output=True, text=True, check=False
        )
        out = (res.stdout or "") + (res.stderr or "")
        safe = "".join(c if c.isalnum() or c in "-." else "_" for c in label)[:60]
        log = self.logs / f"{self.step_no:02d}_{safe}.log"
        log.write_text(" ".join(cmd) + "\n\n" + out, encoding="utf-8")
        ok = expect_ok is None or (res.returncode == 0) == expect_ok
        status = "ok" if res.returncode == 0 else f"rc={res.returncode}"
        print(f"[{self.step_no:02d}] {label}: {status} ({time.time() - t0:.1f}s) log={log}")
        if not ok:
            tail = "\n".join(out.splitlines()[-60:])
            raise StepError(f"{label}: expected {'success' if expect_ok else 'failure'}, "
                            f"got rc={res.returncode}\n{tail}")
        res.output = out  # type: ignore[attr-defined]
        return res

    def cli(self, label: str, *args: str, expect_ok: bool = True) -> subprocess.CompletedProcess:
        cmd = [sys.executable, "-m", "dbt_virtual.cli", "--project-dir", str(self.proj), *args]
        return self._run(label, cmd, expect_ok)

    def dbt(self, label: str, *args: str,
            expect_ok: bool | None = True) -> subprocess.CompletedProcess:
        return self._run(label, [self.dbt_exe, *args], expect_ok)

    def operation(self, name: str, args: dict, *, expect_ok: bool = True,
                  label: str | None = None) -> subprocess.CompletedProcess:
        return self.dbt(label or f"run-operation {name}", "run-operation", name,
                        "--args", json.dumps(args), expect_ok=expect_ok)

    # ---------------------------------------------------------------- warehouse helpers
    def q(self, sql: str) -> list[dict]:
        return [dict(r.items()) for r in self.client.query(sql).result()]

    def fq(self, key: str, name: str) -> str:
        return f"`{self.project}`.`{self.ds[key]}`.`{name}`"

    def objects(self, key: str) -> dict[str, str] | None:
        from google.api_core.exceptions import NotFound

        ref = f"{self.project}.{self.ds[key]}"
        try:
            found = {t.table_id: t.table_type for t in self.client.list_tables(ref)}
            found.update({r.routine_id: r.type_ for r in self.client.list_routines(ref)})
        except NotFound:
            return None
        return found

    def view_target(self, key: str, name: str) -> str:
        return self.client.get_table(f"{self.project}.{self.ds[key]}.{name}").view_query

    def routine_body(self, key: str, name: str) -> str:
        return self.client.get_routine(f"{self.project}.{self.ds[key]}.{name}").body

    def registry(self, table: str, where: str) -> list[dict]:
        return self.q(f"SELECT * FROM {self.fq('physical', table)} WHERE {where}")

    def fingerprints(self) -> dict[str, str]:
        data = json.loads((self.proj / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))
        return data["by_name"]

    def check(self, name: str, cond: bool, detail: object = "") -> bool:
        self.results.append((name, bool(cond), str(detail)))
        print(f"    {'PASS' if cond else 'FAIL'} {name}" + ("" if cond else f" -- {detail}"))
        return bool(cond)


def tvf_ids(sb: Sandbox, key: str, min_amount: int) -> list[int]:
    rows = sb.q(f"SELECT id FROM {sb.fq(key, 'big_orders')}({min_amount}) ORDER BY id")
    return [r["id"] for r in rows]


MISSING_DISPATCH = re.compile(r"No macro named '(\w+)' found within namespace: 'dbt_virtual'")

# docs/guides/getting_started.md, Step 3, optional `is_incremental()` override.
IS_INCREMENTAL_OVERRIDE = """{% macro is_incremental() -%}
  {{ return(dbt_virtual.virtual_is_incremental()) }}
{%- endmacro %}
"""


def preflight(sb: Sandbox) -> None:
    """`dbt parse` must work with the package as is. dbt resolves every literal
    `adapter.dispatch('<name>', 'dbt_virtual')` at parse time, so a dispatched macro without a
    `bigquery__`/`default__` implementation breaks every dbt command on BigQuery. When that
    happens the failure is recorded and a clearly named shim is added to the TEST project (never
    the package) so the rest of the lifecycle can still be checked."""
    sb.dbt("dbt deps", "deps")
    shims: list[str] = []
    for _ in range(10):
        res = sb.dbt("dbt parse", "parse", expect_ok=None)
        if res.returncode == 0:
            break
        missing = MISSING_DISPATCH.search(res.output)
        if not missing or missing.group(1) in shims:
            raise StepError("dbt parse failed:\n" + "\n".join(res.output.splitlines()[-40:]))
        name = missing.group(1)
        sb.check(f"package parses on {sb.adapter}: '{name}' has a {sb.adapter}__/default__ "
                 "implementation", False, missing.group(0))
        shims.append(name)
        (sb.proj / "macros" / "zz_test_only_dispatch_shims.sql").write_text(
            "{# TEST-ONLY workaround for missing dbt_virtual dispatch implementations. #}\n"
            + "".join(
                f"{{% macro default__{n}() -%}}\n  {{%- set _args = varargs -%}}\n"
                f"  {{{{ exceptions.raise_compiler_error('dbt_virtual.{n} has no "
                f"implementation for this adapter') }}}}\n{{%- endmacro %}}\n"
                for n in shims
            ),
            encoding="utf-8",
        )
    sb.check("package parses (after test-only shims, if any)", True, shims)

    # The guide's optional is_incremental() override must parse too.
    override = sb.proj / "macros" / "zz_is_incremental_override.sql"
    override.write_text(IS_INCREMENTAL_OVERRIDE, encoding="utf-8")
    # Full parse: a partial parse does not re-render models when only a macro file is added.
    res = sb.dbt("dbt parse with guide's is_incremental override", "parse",
                 "--no-partial-parse", expect_ok=None)
    sb.check("getting_started is_incremental() override parses", res.returncode == 0,
             "\n".join(res.output.splitlines()[-6:]))
    override.unlink()


def lifecycle(sb: Sandbox) -> None:
    d = sb.ds
    preflight(sb)

    # ---- 1. release r1 in staging ------------------------------------------------------
    sb.cli("build staging r1", "build", "--env", "staging", "--release", "r1")
    fp1 = sb.fingerprints()
    print(f"    r1 fingerprints: {fp1}")
    phys = sb.objects("physical") or {}
    sb.check("r1 physical tables", set(phys) == {
        "_environments", "_releases", "_locks", "_intervals",
        f"events__{fp1['events']}", f"orders__{fp1['orders']}",
        f"dim_static__{fp1['dim_static']}"}, sorted(phys))
    stg = sb.objects("staging") or {}
    sb.check("r1 staging objects (one dataset for every custom schema)", stg == {
        "events": "VIEW", "orders": "VIEW", "dim_static": "VIEW",
        "big_orders": "TABLE_VALUED_FUNCTION"}, stg)
    sb.check("staging.events -> events__<fp1>",
             f"events__{fp1['events']}" in sb.view_target("staging", "events"),
             sb.view_target("staging", "events"))
    sb.check("staging.events rows", sb.q(
        f"SELECT COUNT(*) AS n FROM {sb.fq('staging', 'events')}")[0]["n"] == 3)
    sb.check("staging TVF big_orders(20)", tvf_ids(sb, "staging", 20) == [2, 3],
             tvf_ids(sb, "staging", 20))
    body = sb.routine_body("staging", "big_orders")
    sb.check("staging TVF reads staging.orders", f"{d['staging']}`.`orders`" in body, body)
    env_rows = {r["model_name"]: (r["fingerprint"], r["schema_key"]) for r in
                sb.registry("_environments", "environment = 'staging'")}
    sb.check("_environments staging pointers + schema_key", env_rows == {
        "events": (fp1["events"], None), "orders": (fp1["orders"], "gold"),
        "dim_static": (fp1["dim_static"], "gold"),
        "big_orders": (fp1["big_orders"], "spacetime")}, env_rows)
    r1_rows = sorted(r["model_name"] for r in sb.registry("_releases", "release_id = 'r1'"))
    sb.check("_releases r1 lists every virtual node",
             r1_rows == ["big_orders", "dim_static", "events", "orders"], r1_rows)
    sb.check("no prod datasets before promote",
             all(sb.objects(k) is None for k in ("prod", "prod_gold", "spacetime")))

    # ---- 2. promote r1 to prod (dbt run-operation) ---------------------------------------
    sb.operation("virtual_promote",
                 {"from_env": "staging", "to_env": "prod", "release_id": "r1"},
                 label="promote r1 (run-operation)")
    sb.check("prod default dataset", sb.objects("prod") == {"events": "VIEW"},
             sb.objects("prod"))
    sb.check("prod gold dataset (derived <prod>_gold)",
             sb.objects("prod_gold") == {"orders": "VIEW", "dim_static": "VIEW"},
             sb.objects("prod_gold"))
    sb.check("prod spacetime dataset holds the TVF",
             sb.objects("spacetime") == {"big_orders": "TABLE_VALUED_FUNCTION"},
             sb.objects("spacetime"))
    body = sb.routine_body("spacetime", "big_orders")
    sb.check("prod TVF reads prod gold dataset, not staging",
             f"{d['prod_gold']}`.`orders`" in body and d["staging"] not in body, body)
    sb.check("prod TVF rows after r1", tvf_ids(sb, "spacetime", 0) == [1, 2, 3],
             tvf_ids(sb, "spacetime", 0))
    st = {r["status"] for r in sb.registry("_releases", "release_id = 'r1'")}
    sb.check("r1 marked promoted", st == {"promoted"}, st)

    # ---- 3. change the incremental model -> SEED_INCREMENTAL via status + state file ----
    sb.write_models({"events": EVENTS_V2})
    sb.cli("status -> state.json", "status", "--env", "staging", "--base-env", "prod",
           "--output", "state.json")
    state = json.loads((sb.proj / "state.json").read_text("utf-8"))
    sb.check("state.json base_env_state has prod events@fp1",
             state.get("base_env_state", {}).get("events") == fp1["events"], state)
    sb.dbt("dbt compile staging (for plan)", "compile", "--target", "staging")
    plan = sb.cli("plan staging (state file)", "plan", "--env", "staging", "--skip-compile",
                  "--state-file", "state.json", "--format", "json")
    plan_json = json.loads(plan.stdout[plan.stdout.index("{"):])
    actions = {n["name"]: n["action"] for n in plan_json["nodes"]}
    sb.check("plan actions r2", actions == {
        "events": "SEED_INCREMENTAL", "orders": "FULL_BUILD",
        "big_orders": "ROUTINE_DEPLOY", "dim_static": "REUSE"}, actions)
    sb.cli("build staging r2 (state file)", "build", "--env", "staging", "--release", "r2",
           "--state-file", "state.json")
    fp2 = sb.fingerprints()
    print(f"    r2 fingerprints: {fp2}")
    sb.check("events fingerprint changed", fp2["events"] != fp1["events"])
    sb.check("dim_static fingerprint unchanged", fp2["dim_static"] == fp1["dim_static"])
    tbl = sb.q(
        f"SELECT table_type FROM `{sb.project}`.`{d['physical']}`.INFORMATION_SCHEMA.TABLES "
        f"WHERE table_name = 'events__{fp2['events']}'")
    print(f"    events__<fp2> table_type: {tbl}")
    rows = [(r["id"], r["v"], r["amount"]) for r in sb.q(
        f"SELECT id, v, amount FROM {sb.fq('physical', 'events__' + fp2['events'])} ORDER BY id")]
    sb.check("events__<fp2> seeded by CLONE + incremental delta only",
             rows == [(1, "a", 10), (2, "b", 25), (3, "c", 40), (4, "D", 55)], rows)
    sb.check("staging.events -> fp2", f"events__{fp2['events']}" in
             sb.view_target("staging", "events"))
    sb.check("prod.events untouched (fp1)", f"events__{fp1['events']}" in
             sb.view_target("prod", "events"))
    sb.check("prod TVF untouched (3 rows)", tvf_ids(sb, "spacetime", 0) == [1, 2, 3])
    r2_rows = sorted(r["model_name"] for r in sb.registry("_releases", "release_id = 'r2'"))
    sb.check("_releases r2 includes REUSE dim_static",
             r2_rows == ["big_orders", "dim_static", "events", "orders"], r2_rows)

    # ---- 4. promote r2 (CLI, with audit) -----------------------------------------------
    sb.cli("promote r2 --audit", "promote", "--from", "staging", "--to", "prod",
           "--release", "r2", "--audit")
    sb.check("prod.events -> fp2", f"events__{fp2['events']}" in sb.view_target("prod", "events"))
    sb.check("prod_gold.orders -> fp2", f"orders__{fp2['orders']}" in
             sb.view_target("prod_gold", "orders"))
    sb.check("prod TVF rows after r2", tvf_ids(sb, "spacetime", 0) == [1, 2, 3, 4],
             tvf_ids(sb, "spacetime", 0))
    sb.check("prod gold keeps dim_static", "dim_static" in (sb.objects("prod_gold") or {}))

    # ---- 5. rollbacks --------------------------------------------------------------------
    sb.operation("virtual_rollback", {"env": "prod"}, label="rollback prod (default) #1")
    sb.check("rollback #1: prod.events -> fp1", f"events__{fp1['events']}" in
             sb.view_target("prod", "events"))
    sb.check("rollback #1: prod TVF rows", tvf_ids(sb, "spacetime", 0) == [1, 2, 3],
             tvf_ids(sb, "spacetime", 0))
    body = sb.routine_body("spacetime", "big_orders")
    sb.check("rollback #1: prod TVF still reads prod gold", f"{d['prod_gold']}`.`orders`" in body
             and d["staging"] not in body, body)
    sb.check("rollback #1: objects kept", sb.objects("prod_gold") ==
             {"orders": "VIEW", "dim_static": "VIEW"}, sb.objects("prod_gold"))
    statuses = {r["release_id"]: r["s"] for r in sb.q(
        f"SELECT release_id, MAX(status) AS s FROM {sb.fq('physical', '_releases')} "
        "WHERE target_environment = 'prod' GROUP BY 1")}
    sb.check("rollback #1: statuses", statuses == {"r1": "promoted", "r2": "rolled_back"},
             statuses)
    res = sb.operation("virtual_rollback", {"env": "prod"}, expect_ok=False,
                       label="rollback prod (default) #2, nothing older")
    sb.check("rollback #2 refuses (no ping-pong back to r2)",
             "No previous promoted release" in res.output, res.output[-500:])
    sb.check("rollback #2: prod unchanged", tvf_ids(sb, "spacetime", 0) == [1, 2, 3])
    sb.cli("rollback prod --to-release r2 (roll forward)", "rollback", "--env", "prod",
           "--to-release", "r2")
    sb.check("roll-forward: prod TVF rows", tvf_ids(sb, "spacetime", 0) == [1, 2, 3, 4],
             tvf_ids(sb, "spacetime", 0))

    # ---- 6. audit gate -------------------------------------------------------------------
    res = sb.cli("audit r1 vs prod, max drop 10%", "audit", "--from", "staging", "--to", "prod",
                 "--release", "r1", "--max-row-drop-pct", "10", expect_ok=False)
    sb.check("audit fails on 25% row drop", "row count dropped" in res.output,
             res.output[-500:])
    res = sb.cli("audit r2 vs prod", "audit", "--from", "staging", "--to", "prod",
                 "--release", "r2")
    sb.check("audit r2 passes", "audit [events]: OK" in res.output, res.output[-500:])

    # ---- 7. lease locks ----------------------------------------------------------------
    sb.operation("virtual_acquire_lock", {"env": "prod", "owner": "ci_a"}, label="lock ci_a")
    res = sb.operation("virtual_acquire_lock", {"env": "prod", "owner": "ci_b"},
                       expect_ok=False, label="lock ci_b (conflict)")
    sb.check("second owner is refused", "lock conflict" in res.output, res.output[-500:])
    sb.operation("virtual_release_lock", {"env": "prod", "owner": "ci_a"}, label="unlock ci_a")
    sb.check("_locks empty", sb.registry("_locks", "TRUE") == [])

    # ---- 8. ephemeral PR environment -------------------------------------------------------
    sb.cli("init-env pr_1", "init-env", "--env", "pr_1", "--from", "prod")
    pr = sb.objects("pr_1")
    sb.check("pr_1 dataset (<prod>_pr_1) holds every object", pr == {
        "events": "VIEW", "orders": "VIEW", "dim_static": "VIEW",
        "big_orders": "TABLE_VALUED_FUNCTION"}, pr)
    if pr and "big_orders" in pr:
        body = sb.routine_body("pr_1", "big_orders")
        sb.check("pr_1 TVF reads pr_1.orders", f"{d['pr_1']}`.`orders`" in body, body)
        sb.check("pr_1 TVF rows", tvf_ids(sb, "pr_1", 0) == [1, 2, 3, 4])
    sb.cli("drop-env pr_1", "drop-env", "--env", "pr_1")
    sb.check("pr_1 dataset dropped", sb.objects("pr_1") is None)
    sb.check("pr_1 pointers removed", sb.registry("_environments", "environment = 'pr_1'") == [])
    sb.check("prod intact after drop-env", tvf_ids(sb, "spacetime", 0) == [1, 2, 3, 4])

    # ---- 9. garbage collection (dry run) -------------------------------------------------
    orphan = "events__0000deadbeef"
    sb.q(f"CREATE TABLE {sb.fq('physical', orphan)} AS SELECT 1 AS id")
    res = sb.cli("gc --dry-run --retention-days 0", "gc", "--dry-run", "--retention-days", "0")
    would = sorted(line.split("Would drop ")[1].strip() for line in res.output.splitlines()
                   if "Would drop " in line)
    sb.check("gc lists only the orphan", len(would) == 1 and orphan in would[0], would)
    res = sb.cli("gc --dry-run (7 days)", "gc", "--dry-run")
    sb.check("gc respects retention_days", "0 orphaned" in res.output, res.output[-300:])
    sb.check("dry run dropped nothing", orphan in (sb.objects("physical") or {}))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--project", default=os.environ.get("DBT_VIRTUAL_SBX_PROJECT"))
    ap.add_argument("--location", default=os.environ.get("DBT_VIRTUAL_SBX_LOCATION", "US"))
    ap.add_argument("--run-id", default=None, help="Suffix after dbt_virtual_sbx_ (default random).")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--workdir", default=None)
    ap.add_argument("--dbt-bin-dir", default=None,
                    help="Directory with the dbt executable (default: this Python's).")
    ap.add_argument("--keep", action="store_true", help="Do not drop the sandbox datasets.")
    ns = ap.parse_args()
    if not ns.project:
        ap.error("--project or DBT_VIRTUAL_SBX_PROJECT is required")

    sb = Sandbox(ns)
    if sb.sandbox_datasets():
        raise SystemExit(f"datasets with prefix {sb.sbx} already exist; pick another --run-id")
    print(f"sandbox prefix: {sb.sbx}  workdir: {sb.root}")
    sb.write_project()
    failed_step = None
    try:
        lifecycle(sb)
    except StepError as exc:
        failed_step = str(exc)
        print(f"\nSTEP FAILED: {exc}")
    finally:
        if sb.keep:
            print(f"--keep: sandbox datasets left in place: {sb.sandbox_datasets()}")
        else:
            print(f"dropped sandbox datasets: {sb.cleanup()}")
            print(f"remaining with prefix {sb.sbx}: {sb.sandbox_datasets()}")

    failures = [r for r in sb.results if not r[1]]
    print(f"\n{len(sb.results) - len(failures)}/{len(sb.results)} checks passed"
          + (f"; aborted at a step: {failed_step.splitlines()[0]}" if failed_step else ""))
    for name, _, detail in failures:
        print(f"  FAIL {name}: {detail[:300]}")
    return 1 if failures or failed_step else 0


if __name__ == "__main__":
    sys.exit(main())
