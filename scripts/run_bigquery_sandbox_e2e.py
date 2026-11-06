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

`--extended` adds two more opt-in projects, in the same sandbox (see `extended_single_location`
and `extended_cross_location`): `authorize_env_views`, env view description / labels / column
descriptions, `diff-data` on partitioned tables (with bytes billed per query read from
INFORMATION_SCHEMA.JOBS_BY_USER), the partition coverage audit, `gc --expire-env-days`, and
promote-by-clone between two datasets that stand for two "locations". `--only-extended` skips the
base lifecycle.

`--xproject` runs promote-by-clone across two GCP PROJECTS (`--project` acts as CI,
`--prod-project` / DBT_VIRTUAL_SBX_PROD_PROJECT as prod; both datasets' location must be the
same): see `extended_cross_project`. Datasets with the sandbox prefix are created and dropped in
both projects; nothing else is touched. Bytes billed per step are read from
INFORMATION_SCHEMA.JOBS_BY_USER of each project.
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
from datetime import datetime, timedelta, timezone
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
        self.metrics: list[str] = []
        self.seen_jobs: set[str] = set()
        self.started_at = datetime.now(timezone.utc) - timedelta(minutes=2)

        from google.cloud import bigquery  # optional dependency, only needed here

        self.bq = bigquery
        self.client = bigquery.Client(project=self.project, location=self.location)
        self.prod_project = getattr(ns, "prod_project", None)
        self.clients = {self.project: self.client}
        if self.prod_project and self.prod_project != self.project:
            self.clients[self.prod_project] = bigquery.Client(
                project=self.prod_project, location=self.location)
        # dataset key -> project, for keys that live outside `self.project` (--xproject)
        self.key_project: dict[str, str] = {}
        self.step_windows: list[tuple[str, datetime, datetime]] = []

    def pj(self, key: str) -> str:
        return self.key_project.get(key, self.project)

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

    def sandbox_datasets_by_project(self) -> dict[str, list[str]]:
        return {
            project: sorted(
                ds.dataset_id
                for ds in client.list_datasets(project=project)
                if ds.dataset_id == self.sbx or ds.dataset_id.startswith(self.sbx + "_")
            )
            for project, client in self.clients.items()
        }

    def sandbox_datasets(self) -> list[str]:
        return sorted(d for ids in self.sandbox_datasets_by_project().values() for d in ids)

    def cleanup(self) -> list[str]:
        dropped = []
        for project, ids in self.sandbox_datasets_by_project().items():
            for dataset_id in ids:
                assert dataset_id.startswith(SANDBOX_PREFIX)
                self.clients[project].delete_dataset(
                    f"{project}.{dataset_id}", delete_contents=True, not_found_ok=True
                )
                dropped.append(dataset_id)
        return dropped

    # ---------------------------------------------------------------- process helpers
    def _run(self, label: str, cmd: list[str],
             expect_ok: bool | None) -> subprocess.CompletedProcess:
        self.step_no += 1
        t0 = time.time()
        w0 = datetime.now(timezone.utc)
        res = subprocess.run(
            cmd, cwd=self.proj, env=self.env, capture_output=True, text=True, check=False
        )
        self.step_windows.append((label, w0, datetime.now(timezone.utc)))
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
        return f"`{self.pj(key)}`.`{self.ds[key]}`.`{name}`"

    def objects(self, key: str) -> dict[str, str] | None:
        from google.api_core.exceptions import NotFound

        ref = f"{self.pj(key)}.{self.ds[key]}"
        client = self.clients[self.pj(key)]
        try:
            found = {t.table_id: t.table_type for t in client.list_tables(ref)}
            found.update({r.routine_id: r.type_ for r in client.list_routines(ref)})
        except NotFound:
            return None
        return found

    def view_target(self, key: str, name: str) -> str:
        return self.clients[self.pj(key)].get_table(f"{self.pj(key)}.{self.ds[key]}.{name}").view_query

    def routine_body(self, key: str, name: str) -> str:
        return self.clients[self.pj(key)].get_routine(f"{self.pj(key)}.{self.ds[key]}.{name}").body

    def registry(self, table: str, where: str, key: str = "physical") -> list[dict]:
        return self.q(f"SELECT * FROM {self.fq(key, table)} WHERE {where}")

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


# ------------------------------------------------------------------------------------------------
# --extended: opt-in checks of the newer features. Each function builds its own dbt project in the
# same sandbox (every dataset starts with `dbt_virtual_sbx_<run>_x...`, so cleanup drops them).
# ------------------------------------------------------------------------------------------------

FACTS_DESCRIPTION = "Daily facts; it's a test."
FACTS_LABELS = {"team": "dbt_virtual", "tier": "sandbox"}
FACTS_SCHEMA_YML = """version: 2
models:
  - name: facts
    description: "Daily facts; it's a test."
    config:
      persist_docs: {relation: true, columns: true}
      labels: {team: dbt_virtual, tier: sandbox}
    columns:
      - name: id
        description: "Primary key; unique."
      - name: amount
        description: "Amount in cents"
"""

_FACTS_V1 = (
    "select id, date_sub(current_date(), interval mod(id, 30) day) as day, mod(id * 7, 1000) as amount "
    "from unnest(generate_array(1, 3000)) as id"
)
# v2 vs v1: 24 rows changed (ids that are multiples of 100 but not of 250), 12 rows removed
# (multiples of 250), 5 rows added (ids 5001..5005, partition = today). 3000 -> 2993 rows.
_FACTS_V2 = (
    "select id, day, case when mod(id, 100) = 0 then amount + 1 else amount end as amount "
    f"from ({_FACTS_V1}) where mod(id, 250) != 0 "
    "union all select 5000 + n as id, current_date() as day, 1 as amount "
    "from unnest(generate_array(1, 5)) as n"
)
# v3: v2 limited to the last 10 daily partitions (a "windowed build": 20 partitions missing).
_FACTS_V3 = f"select * from ({_FACTS_V2}) where day >= date_sub(current_date(), interval 9 day)"


def facts_sql(version: int, rpf: bool) -> str:
    body = {1: _FACTS_V1, 2: _FACTS_V2, 3: _FACTS_V3}[version]
    rpf_cfg = ", require_partition_filter=true" if rpf else ""
    return (
        "{{ config(materialized='table', unique_key='id', "
        f"partition_by={{'field': 'day', 'data_type': 'date'}}{rpf_cfg}) }}}}\n{body}\n"
    )


def ext_project(sb: Sandbox, name: str, ds: dict[str, str], extra_vars: dict,
                models: dict[str, str], schema_yml: str = "",
                target_projects: dict[str, str] | None = None) -> None:
    """Create a dbt project `name` next to the base one and make it the sandbox's current project."""
    proj = sb.root / name
    (proj / "models").mkdir(parents=True)
    (proj / "macros").mkdir()
    sb.proj = proj
    sb.logs = sb.root / f"step_logs_{name}"
    sb.logs.mkdir()
    sb.step_no = 0
    sb.ds = dict(ds)
    sb.env["DBT_PROFILES_DIR"] = str(proj)
    pkg = sb.root / "dbt_virtual_pkg"
    (proj / "packages.yml").write_text(f'packages:\n  - local: "{pkg.as_posix()}"\n', encoding="utf-8")
    cfg = {
        "enabled": True, "physical_schema": ds["physical"],
        "environment": "{{ env_var('DBT_VIRTUAL_ENV', target.name) }}",
        "base_environment": "prod", "ref_resolution": "env_view",
        "env_schemas": {"staging": ds["staging"], "prod": ds["prod"]},
        "retention_releases": 5, "retention_days": 7, **extra_vars,
    }
    (proj / "dbt_project.yml").write_text(
        f"""name: 'dbt_virtual_sbx_{name}'
version: '1.0.0'
config-version: 2
profile: 'dbt_virtual_sbx'
model-paths: ["models"]
macro-paths: ["macros"]
on-run-start:
  - "{{{{ dbt_virtual.seed_incremental_clones() }}}}"
vars:
  dbt_virtual: {json.dumps(cfg)}
models:
  dbt_virtual_sbx_{name}:
    +virtual_env: true
    +post-hook:
      - "{{{{ dbt_virtual.sync_env_view() }}}}"
""", encoding="utf-8")
    outputs = ""
    for target, dataset in (("staging", ds["staging"]), ("prod", ds["prod"])):
        tproject = (target_projects or {}).get(target, sb.project)
        outputs += (
            f"    {target}:\n      type: bigquery\n      method: oauth\n"
            f"      project: {tproject}\n      dataset: {dataset}\n"
            f"      location: {sb.location}\n      threads: {sb.threads}\n"
            "      priority: interactive\n      job_retries: 1\n"
            "      job_execution_timeout_seconds: 600\n"
            "      maximum_bytes_billed: 10000000000\n"
        )
    (proj / "profiles.yml").write_text(
        f"dbt_virtual_sbx:\n  target: staging\n  outputs:\n{outputs}", encoding="utf-8")
    (proj / "macros" / "virtual_routing_overrides.sql").write_text(ROUTING_OVERRIDES, encoding="utf-8")
    if schema_yml:
        (proj / "models" / "schema.yml").write_text(schema_yml, encoding="utf-8")
    sb.write_models(models)
    sb.dbt("dbt deps", "deps")


def authorized_entries(sb: Sandbox, physical_key: str, env_dataset: str) -> list[dict]:
    """The `dataset` access entries of the physical dataset that name `env_dataset`."""
    props = sb.client.get_dataset(f"{sb.project}.{sb.ds[physical_key]}")._properties
    return [a["dataset"] for a in props.get("access", [])
            if a.get("dataset", {}).get("dataset", {}).get("datasetId") == env_dataset]


DIFF_JOB_PATTERN = r"dv_seg|dv_only_candidate|dv_kind"


def job_stats(sb: Sandbox, pattern: str, expected: int) -> list[dict]:
    """Query jobs of the current user not reported before whose text matches `pattern` (re2) and
    the sandbox prefix, from INFORMATION_SCHEMA.JOBS_BY_USER. Retries while the view catches up.
    `cache` is True for a result served from the query cache (billed 0 bytes)."""
    sql = (
        "SELECT job_id, total_bytes_processed AS processed, total_bytes_billed AS billed, "
        "cache_hit AS cache, error_result.reason AS err "
        f"FROM `{sb.project}`.`region-{sb.location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_USER "
        f"WHERE creation_time >= TIMESTAMP('{sb.started_at:%Y-%m-%d %H:%M:%S}') "
        f"AND job_type = 'QUERY' AND state = 'DONE' AND REGEXP_CONTAINS(query, r'{pattern}') "
        f"AND query LIKE '%{sb.sbx}%' AND query NOT LIKE '%INFORMATION_SCHEMA.JOBS%' "
        "ORDER BY creation_time"
    )
    fresh: list[dict] = []
    for _ in range(12):
        fresh = [r for r in sb.q(sql) if r["job_id"] not in sb.seen_jobs]
        if len(fresh) >= expected:
            break
        time.sleep(5)
    sb.seen_jobs.update(r["job_id"] for r in fresh)
    return fresh


def cli_json(res: subprocess.CompletedProcess) -> dict:
    out = res.output  # type: ignore[attr-defined]
    return json.loads(out[out.index("{"):])


def diff_oracle(sb: Sandbox, cand: str, base: str, where: str = "TRUE") -> dict:
    """Independent full outer join of two `facts` tables (id key; day and amount compared)."""
    return sb.q(f"""
      SELECT COUNTIF(b.id IS NULL) AS only_candidate, COUNTIF(c.id IS NULL) AS only_target,
             COUNTIF(c.id IS NOT NULL AND b.id IS NOT NULL
                     AND (c.amount != b.amount OR c.day != b.day)) AS changed,
             COUNTIF(c.id IS NOT NULL) AS candidate_rows, COUNTIF(b.id IS NOT NULL) AS target_rows
      FROM (SELECT * FROM {cand} WHERE {where}) c
      FULL OUTER JOIN (SELECT * FROM {base} WHERE {where}) b ON c.id = b.id""")[0]


def report_metrics(sb: Sandbox, label: str, stats: list[dict]) -> None:
    processed = sum(int(r["processed"] or 0) for r in stats)
    billed = sum(int(r["billed"] or 0) for r in stats)
    cached = sum(1 for r in stats if r.get("cache"))
    line = (f"METRIC {label}: {len(stats)} queries ({cached} served from the query cache), "
            f"bytes processed {processed}, bytes billed {billed}, "
            f"billed per query {[int(r['billed'] or 0) for r in stats]}")
    print("    " + line)
    sb.metrics.append(line)


def queries_total(q: dict) -> int:
    return q["checksum_queries"] + q["join_queries"] + q["sample_queries"]


def extended_single_location(sb: Sandbox) -> None:
    p = sb.project
    x = f"{sb.sbx}_x1"
    ds = {"physical": f"{x}_phys", "staging": f"{x}_stg", "prod": f"{x}_prod",
          "pr_old": f"{x}_prod_pr_old"}
    ext_project(sb, "proj_x1", ds, {"authorize_env_views": True},
                {"facts": facts_sql(1, False), "facts_rpf": facts_sql(1, True)}, FACTS_SCHEMA_YML)
    print(f"\n=== extended 1: single location, datasets {x}_* ===")

    # ---- E1. build + authorize_env_views + env view metadata -------------------------------------
    sb.cli("x1 build staging r1", "build", "--env", "staging", "--release", "r1")
    fp1 = dict(sb.fingerprints())
    stg_auth = authorized_entries(sb, "physical", ds["staging"])
    sb.check("authorize_env_views: staging dataset authorized once on the physical dataset",
             len(stg_auth) == 1 and stg_auth[0].get("targetTypes") == ["VIEWS"], stg_auth)
    sb.check("authorize_env_views: prod dataset not authorized before it has views",
             authorized_entries(sb, "physical", ds["prod"]) == [])
    sb.cli("x1 promote r1", "promote", "--from", "staging", "--to", "prod", "--release", "r1")
    sb.check("authorize_env_views: prod dataset authorized once after promote",
             len(authorized_entries(sb, "physical", ds["prod"])) == 1,
             authorized_entries(sb, "physical", ds["prod"]))
    sb.cli("x1 build staging r1b (all REUSE)", "build", "--env", "staging", "--release", "r1b")
    sb.cli("x1 promote r1b (again)", "promote", "--from", "staging", "--to", "prod",
           "--release", "r1b")
    sb.check("authorize_env_views: no duplicate entries after re-running build and promote",
             len(authorized_entries(sb, "physical", ds["staging"])) == 1
             and len(authorized_entries(sb, "physical", ds["prod"])) == 1,
             (authorized_entries(sb, "physical", ds["staging"]),
              authorized_entries(sb, "physical", ds["prod"])))
    sb.check("authorize_env_views: physical dataset keeps its other access entries",
             len(sb.client.get_dataset(f"{p}.{ds['physical']}")._properties.get("access", [])) >= 3)

    for key in ("staging", "prod"):
        tbl = sb.client.get_table(f"{p}.{ds[key]}.facts")
        cols = {f.name: f.description for f in tbl.schema}
        sb.check(f"view metadata [{key}.facts]: description with ';' and quote",
                 tbl.description == FACTS_DESCRIPTION, tbl.description)
        sb.check(f"view metadata [{key}.facts]: labels", dict(tbl.labels or {}) == FACTS_LABELS,
                 tbl.labels)
        sb.check(f"view metadata [{key}.facts]: column descriptions (update_columns)",
                 cols == {"id": "Primary key; unique.", "day": None, "amount": "Amount in cents"},
                 cols)
        plain = sb.client.get_table(f"{p}.{ds[key]}.facts_rpf")
        sb.check(f"view metadata [{key}.facts_rpf]: nothing set without persist_docs/labels",
                 plain.description is None and not plain.labels, (plain.description, plain.labels))

    # ---- E2. release r2 = v2 data, row-level data diff -----------------------------------------
    sb.write_models({"facts": facts_sql(2, False), "facts_rpf": facts_sql(2, True)})
    sb.cli("x1 build staging r2", "build", "--env", "staging", "--release", "r2")
    fp2 = dict(sb.fingerprints())
    cand = sb.fq("physical", f"facts__{fp2['facts']}")
    base = sb.fq("physical", f"facts__{fp1['facts']}")
    oracle = diff_oracle(sb, cand, base)
    print(f"    oracle (independent join): {oracle}")
    sb.check("oracle matches the injected differences",
             (oracle["only_candidate"], oracle["only_target"], oracle["changed"]) == (5, 12, 24),
             oracle)

    def diff_model(label: str, *args: str, model: str = "facts", expect_ok: bool = True):
        res = sb.cli(label, "diff-data", "--from", "staging", "--to", "prod", "--format", "json",
                     *args, expect_ok=expect_ok)
        if not expect_ok:
            return res, None
        summary = cli_json(res)
        return res, next(m for m in summary["models"] if m["model"] == model)

    _, m = diff_model("x1 diff-data facts (default threshold)", "--select", "facts")
    q = m["queries"]
    sb.check("diff-data facts: counts equal the oracle",
             (m["only_candidate"], m["only_target"], m["changed"], m["candidate_rows"],
              m["target_rows"]) == (5, 12, 24, 2993, 3000), m)
    sb.check("diff-data facts: changed column is amount only",
             {k: v for k, v in m["column_mismatches"].items() if v} == {"amount": 24},
             m["column_mismatches"])
    sb.check("diff-data facts: mismatch_pct = 41/3000", abs(m["mismatch_pct"] - 41 * 100 / 3000) < 1e-3,
             m["mismatch_pct"])
    stats = job_stats(sb, DIFF_JOB_PATTERN, queries_total(q))
    sb.check("diff-data facts: one BigQuery job per reported query", len(stats) == queries_total(q),
             (q, len(stats)))
    report_metrics(sb, f"diff-data facts full table {q}", stats)
    full_processed = sum(int(r["processed"] or 0) for r in stats)

    _, mb = diff_model("x1 diff-data facts (bisection)", "--select", "facts", "--threshold", "20",
                           "--branching", "4")
    sb.check("diff-data facts bisection: same counts as the default run",
             (mb["only_candidate"], mb["only_target"], mb["changed"]) == (5, 12, 24)
             and mb["segments_split"] > 0, mb)
    report_metrics(sb, f"diff-data facts bisection {mb['queries']}",
                   job_stats(sb, DIFF_JOB_PATTERN, queries_total(mb["queries"])))

    _, mw = diff_model("x1 diff-data facts --window-days 3", "--select", "facts",
                           "--window-days", "3")
    win = diff_oracle(sb, cand, base, "day >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)")
    sb.check("diff-data facts --window-days 3: counts equal the windowed oracle",
             (mw["only_candidate"], mw["only_target"], mw["changed"]) ==
             (win["only_candidate"], win["only_target"], win["changed"]), (mw, win))
    wstats = job_stats(sb, DIFF_JOB_PATTERN, queries_total(mw["queries"]))
    report_metrics(sb, f"diff-data facts --window-days 3 {mw['queries']}", wstats)
    sb.check("window prunes partitions: fewer bytes processed than the full diff",
             sum(int(r["processed"] or 0) for r in wstats) < full_processed,
             (sum(int(r["processed"] or 0) for r in wstats), full_processed))

    # require_partition_filter table: a plain diff cannot scan it; --window-days can.
    res, _ = diff_model("x1 diff-data facts_rpf without a window", "--select", "facts_rpf",
                           model="facts_rpf", expect_ok=False)
    out = res.output  # type: ignore[attr-defined]
    failed = job_stats(sb, DIFF_JOB_PATTERN, 1)
    report_metrics(sb, f"diff-data facts_rpf without a window (failed jobs: "
                   f"{sorted({str(r['err']) for r in failed})})", failed)
    sb.check("diff-data on a require_partition_filter table needs a window",
             "partition" in out.lower() and "filter" in out.lower(), out[-400:])
    cand_r = sb.fq("physical", f"facts_rpf__{fp2['facts_rpf']}")
    base_r = sb.fq("physical", f"facts_rpf__{fp1['facts_rpf']}")
    orr = diff_oracle(sb, cand_r, base_r, "day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)")
    _, mr = diff_model("x1 diff-data facts_rpf --window-days 7", "--select", "facts_rpf",
                           "--window-days", "7", model="facts_rpf")
    sb.check("diff-data facts_rpf --window-days 7: counts equal the windowed oracle",
             (mr["only_candidate"], mr["only_target"], mr["changed"], mr["candidate_rows"],
              mr["target_rows"]) == (orr["only_candidate"], orr["only_target"], orr["changed"],
                                     orr["candidate_rows"], orr["target_rows"]), (mr, orr))
    report_metrics(sb, f"diff-data facts_rpf --window-days 7 {mr['queries']}",
                   job_stats(sb, DIFF_JOB_PATTERN, queries_total(mr["queries"])))
    res = sb.cli("x1 diff-data --max-mismatch-pct 1 fails (1.37% > 1)", "diff-data", "--from",
                 "staging", "--to", "prod", "--select", "facts", "--max-mismatch-pct", "1",
                 "--format", "json", expect_ok=False)
    sb.check("diff-data --max-mismatch-pct gate fails above the threshold",
             '"passed": false' in res.output, res.output[-300:])  # type: ignore[attr-defined]

    # ---- E3. partition coverage audit (virtual_audit) on real partitions -----------------------
    res = sb.cli("x1 audit r2 (30 partitions present; one table has require_partition_filter)",
                 "audit", "--from", "staging", "--to", "prod", "--release", "r2")
    out = res.output  # type: ignore[attr-defined]
    sb.check("audit r2 passes incl. coverage on facts and facts_rpf",
             "audit [facts]: OK" in out and "audit [facts_rpf]: OK" in out, out[-500:])
    sb.cli("x1 promote r2 --audit", "promote", "--from", "staging", "--to", "prod",
           "--release", "r2", "--audit")
    sb.check("prod.facts -> fp2 after promote --audit",
             f"facts__{fp2['facts']}" in sb.view_target("prod", "facts"), sb.view_target("prod", "facts"))

    sb.write_models({"facts": facts_sql(3, False)})
    sb.cli("x1 build staging r3 (windowed facts)", "build", "--env", "staging", "--release", "r3")
    today = datetime.now(timezone.utc).date()
    first = (today - timedelta(days=29)).strftime("%Y%m%d")
    last = (today - timedelta(days=10)).strftime("%Y%m%d")
    res = sb.cli("x1 audit r3 (20 partitions missing)", "audit", "--from", "staging", "--to",
                 "prod", "--release", "r3", expect_ok=False)
    out = res.output  # type: ignore[attr-defined]
    sb.check("audit fails on missing partitions and lists the range",
             "lacks 20 partition(s)" in out and f"{first}..{last} (20)" in out, out[-600:])
    ign = (today - timedelta(days=9)).isoformat()
    res = sb.cli("x1 audit r3 --coverage-ignore-before", "audit", "--from", "staging", "--to",
                 "prod", "--release", "r3", "--coverage-ignore-before", ign, expect_ok=None)
    sb.check("audit passes when the missing partitions are before --coverage-ignore-before",
             res.returncode == 0, res.output[-400:])  # type: ignore[attr-defined]
    res = sb.cli("x1 audit r3 --no-coverage-check", "audit", "--from", "staging", "--to", "prod",
                 "--release", "r3", "--no-coverage-check", expect_ok=None)
    sb.check("audit --no-coverage-check skips the check", res.returncode == 0,
             res.output[-400:])  # type: ignore[attr-defined]

    # ---- E4. gc --expire-env-days on an old unmapped environment -------------------------------
    sb.cli("x1 init-env pr_old", "init-env", "--env", "pr_old", "--from", "prod")
    sb.check("pr_old dataset exists", sb.objects("pr_old") is not None)
    sb.q(f"UPDATE {sb.fq('physical', '_environments')} SET updated_at = "
         "TIMESTAMP_SUB(updated_at, INTERVAL 40 DAY) WHERE environment IN ('pr_old', 'staging', 'prod')")
    res = sb.cli("x1 gc --dry-run --expire-env-days 60", "gc", "--dry-run", "--expire-env-days", "60")
    sb.check("gc --expire-env-days 60 expires nothing (40 days old)",
             "expired environment" not in res.output, res.output[-300:])  # type: ignore[attr-defined]
    res = sb.cli("x1 gc --dry-run --expire-env-days 30", "gc", "--dry-run", "--expire-env-days", "30")
    out = res.output  # type: ignore[attr-defined]
    sb.check("gc dry run lists only pr_old (mapped staging/prod are protected)",
             "Would drop expired environment pr_old" in out
             and "expired environment staging" not in out and "expired environment prod" not in out,
             out[-400:])
    sb.check("dry run dropped nothing", sb.objects("pr_old") is not None)
    sb.cli("x1 gc --expire-env-days 30", "gc", "--expire-env-days", "30")
    sb.check("pr_old dataset dropped by gc", sb.objects("pr_old") is None)
    sb.check("pr_old pointers removed", sb.registry("_environments", "environment = 'pr_old'") == [])
    rows_now = sb.q(f"SELECT COUNT(*) AS n FROM {sb.fq('prod', 'facts')}")[0]["n"]
    sb.check("prod view intact after gc; staging and prod pointers kept",
             rows_now == 2993 and len(sb.registry("_environments", "environment = 'staging'")) == 2
             and len(sb.registry("_environments", "environment = 'prod'")) == 2,
             rows_now)


def extended_cross_location(sb: Sandbox) -> None:
    """Promote-by-clone between two datasets in one project (two `physical_locations` that differ
    by schema only). A cross-PROJECT clone would need a second project and is not exercised."""
    p = sb.project
    x = f"{sb.sbx}_x2"
    ds = {"physical": f"{x}_phys_ci", "phys_prod": f"{x}_phys_prod", "staging": f"{x}_stg",
          "prod": f"{x}_prod"}
    locs = {"prod": {"database": p, "schema": ds["phys_prod"]},
            "default": {"database": p, "schema": ds["physical"]}}
    orders = "{{ config(materialized='table', unique_key='id') }}\nselect id, amount from {{ ref('events') }}\n"
    tvf = ("{{ config(materialized='table_function', parameters='min_amount INT64') }}\n"
           "select id, amount from {{ ref('orders') }} where amount >= min_amount\n")
    ext_project(sb, "proj_x2", ds, {"physical_locations": locs},
                {"events": EVENTS_V1, "orders": orders, "big_orders": tvf})
    print(f"\n=== extended 2: two locations (schemas), datasets {x}_* ===")

    def phys_prod_tables() -> dict[str, str]:
        rows = sb.q(f"SELECT table_name, table_type FROM "
                    f"`{p}`.`{ds['phys_prod']}`.INFORMATION_SCHEMA.TABLES")
        return {r["table_name"]: r["table_type"] for r in rows}

    sb.cli("x2 build staging b1 (CI location)", "build", "--env", "staging", "--release", "b1")
    fp1 = dict(sb.fingerprints())
    ci = sb.objects("physical") or {}
    sb.check("b1 built in the CI location only",
             f"events__{fp1['events']}" in ci and sb.objects("phys_prod") is None,
             (sorted(ci), sb.objects("phys_prod")))
    sb.cli("x2 promote b1 (target prod, clone across locations)", "promote", "--target", "prod",
           "--from", "staging", "--to", "prod", "--release", "b1")
    tabs = phys_prod_tables()
    sb.check("b1 tables cloned into the prod location (table_type CLONE)",
             all(tabs.get(f"{m}__{fp1[m]}") == "CLONE" for m in ("events", "orders")), tabs)
    sb.check("prod views point at the clones, not at the CI location",
             all(f"{ds['phys_prod']}`.`{m}__{fp1[m]}" in sb.view_target("prod", m)
                 and ds["physical"] not in sb.view_target("prod", m) for m in ("events", "orders")),
             sb.view_target("prod", "orders"))
    body = sb.routine_body("prod", "big_orders")
    sb.check("prod TVF reads the prod env dataset",
             f"{ds['prod']}`.`orders`" in body and ds["staging"] not in body, body)
    pointers = {r["model_name"] for r in sb.q(
        f"SELECT model_name FROM `{p}`.`{ds['phys_prod']}`._environments WHERE environment = 'prod'")}
    rel = sb.q(f"SELECT DISTINCT status, source_environment FROM `{p}`.`{ds['phys_prod']}`._releases "
               "WHERE release_id = 'b1'")
    sb.check("prod registry holds the pointers and the release (source = staging)",
             pointers == {"events", "orders", "big_orders"}
             and rel == [{"status": "promoted", "source_environment": "staging"}], (pointers, rel))
    sb.check("prod rows via views",
             [r["id"] for r in sb.q(f"SELECT id FROM {sb.fq('prod', 'orders')} ORDER BY id")] == [1, 2, 3])
    sb.check("prod TVF works", tvf_ids(sb, "prod", 20) == [2, 3], tvf_ids(sb, "prod", 20))
    sb.cli("x2 promote b1 again (idempotent)", "promote", "--target", "prod", "--from", "staging",
           "--to", "prod", "--release", "b1")
    sb.check("idempotent: release rows not duplicated", sb.q(
        f"SELECT COUNT(*) AS n FROM `{p}`.`{ds['phys_prod']}`._releases WHERE release_id = 'b1'"
    )[0]["n"] == 3)

    # The source goes away: the clones are independent of it.
    for key in ("physical", "staging"):
        sb.client.delete_dataset(f"{p}.{ds[key]}", delete_contents=True, not_found_ok=True)
    sb.check("CI location and staging dataset dropped",
             sb.objects("physical") is None and sb.objects("staging") is None)
    sb.check("prod views still work after the source was dropped",
             [r["id"] for r in sb.q(f"SELECT id FROM {sb.fq('prod', 'events')} ORDER BY id")] == [1, 2, 3]
             and tvf_ids(sb, "prod", 20) == [2, 3])

    # CI seeds an incremental model from the prod location (clone prod -> CI), then promotes again.
    sb.write_models({"events": EVENTS_V2})
    sb.cli("x2 build staging b2 (seed from the prod location)", "build", "--env", "staging",
           "--release", "b2")
    fp2 = dict(sb.fingerprints())
    rows = [(r["id"], r["v"], r["amount"]) for r in sb.q(
        f"SELECT id, v, amount FROM {sb.fq('physical', 'events__' + fp2['events'])} ORDER BY id")]
    sb.check("CI events__<fp2> = clone of prod's table + delta only",
             rows == [(1, "a", 10), (2, "b", 25), (3, "c", 40), (4, "D", 55)], rows)
    # The gates compare the release (CI location) with prod (its own location).
    res = sb.cli("x2 diff-data b2 vs prod (CI target)", "diff-data", "--from", "staging", "--to",
                 "prod", "--release", "b2", "--format", "json", expect_ok=None)
    dd = {mm["model"]: mm for mm in cli_json(res)["models"]} if res.returncode == 0 else {}  # type: ignore[attr-defined]
    sb.check("diff-data across locations compares b2 with prod's tables (1 new row each)",
             all(dd.get(n, {}).get("status") == "differs" and dd[n]["only_candidate"] == 1
                 and dd[n]["only_target"] == 0 and dd[n]["changed"] == 0 for n in ("events", "orders")),
             {n: (mm.get("status"), mm.get("only_candidate")) for n, mm in dd.items()})
    res = sb.cli("x2 audit b2 vs prod (CI target)", "audit", "--from", "staging", "--to", "prod",
                 "--release", "b2", expect_ok=None)
    sb.check("audit across locations reads prod's row counts",
             "audit [events]: OK (candidate=4 rows, prod=3 rows)" in res.output,  # type: ignore[attr-defined]
             res.output[-500:])  # type: ignore[attr-defined]
    res = sb.cli("x2 promote b2 --data-diff --max-mismatch-pct 0 (must fail)", "promote",
                 "--target", "prod", "--from", "staging", "--to", "prod", "--release", "b2",
                 "--data-diff", "--max-mismatch-pct", "0", expect_ok=None)
    sb.check("promote --data-diff across locations really diffs (b2 differs from prod: refused)",
             res.returncode != 0 and "data diff failed" in res.output,  # type: ignore[attr-defined]
             (res.returncode, res.output[-500:]))  # type: ignore[attr-defined]
    sb.check("a refused promote changed nothing: prod.events still -> fp1",
             f"events__{fp1['events']}" in sb.view_target("prod", "events"))
    sb.cli("x2 promote b2 (target prod)", "promote", "--target", "prod", "--from", "staging",
           "--to", "prod", "--release", "b2")
    sb.check("prod.events -> fp2 clone in the prod location",
             f"{ds['phys_prod']}`.`events__{fp2['events']}" in sb.view_target("prod", "events")
             and phys_prod_tables().get(f"events__{fp2['events']}") == "CLONE", phys_prod_tables())
    sb.check("prod TVF sees the 4th row", tvf_ids(sb, "prod", 0) == [1, 2, 3, 4], tvf_ids(sb, "prod", 0))
    sb.cli("x2 rollback prod", "rollback", "--target", "prod", "--env", "prod")
    sb.check("rollback to b1 works across locations: prod.events -> fp1",
             f"events__{fp1['events']}" in sb.view_target("prod", "events"), sb.view_target("prod", "events"))



IAM_PROBE_PERMISSIONS = [
    "bigquery.jobs.create", "bigquery.jobs.list", "bigquery.datasets.create", "bigquery.datasets.get",
    "bigquery.datasets.update", "bigquery.datasets.delete", "bigquery.tables.create",
    "bigquery.tables.get", "bigquery.tables.getData", "bigquery.tables.list",
    "bigquery.tables.updateData", "bigquery.tables.delete", "bigquery.routines.create",
    "bigquery.routines.get", "bigquery.routines.list",
]


def iam_probe(sb: Sandbox) -> None:
    """Which BigQuery permissions the running identity holds in each project
    (`projects.testIamPermissions`, read-only; project-level grants only, a dataset-level
    grant does not show here)."""
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    session = AuthorizedSession(creds)
    gcloud_token = None  # fallback when the ADC credential may not call Resource Manager (403)
    print("\n=== IAM probe (project-level permissions of the running identity) ===")
    for label, project in (("CI", sb.project), ("prod", sb.prod_project)):
        url = f"https://cloudresourcemanager.googleapis.com/v1/projects/{project}:testIamPermissions"
        resp = session.post(url, json={"permissions": IAM_PROBE_PERMISSIONS})
        if resp.status_code == 403:
            import requests

            if gcloud_token is None:
                gcloud_token = subprocess.run(
                    ["gcloud", "auth", "print-access-token"], capture_output=True, text=True,
                    check=False, shell=(os.name == "nt")).stdout.strip()
            if gcloud_token:
                resp = requests.post(url, json={"permissions": IAM_PROBE_PERMISSIONS},
                                     headers={"Authorization": f"Bearer {gcloud_token}"}, timeout=60)
        granted = set(resp.json().get("permissions", [])) if resp.ok else set()
        missing = [x for x in IAM_PROBE_PERMISSIONS if x not in granted]
        sb.metrics.append(f"IAM {label}: HTTP {resp.status_code}; granted {len(granted)}/"
                          f"{len(IAM_PROBE_PERMISSIONS)}; missing: {missing}")
        print(sb.metrics[-1])


def extended_cross_project(sb: Sandbox) -> None:
    """Promote-by-clone from a CI project into a prod project (two `physical_locations` with
    different `database`), against real BigQuery. `sb.project` is CI, `sb.prod_project` is prod."""
    ci, pp = sb.project, sb.prod_project
    assert pp and pp != ci, "--xproject needs --prod-project different from --project"
    x = f"{sb.sbx}_x3"
    ds = {"physical": f"{x}_phys_ci", "phys_prod": f"{x}_phys_prod", "staging": f"{x}_stg",
          "prod": f"{x}_prod"}
    sb.key_project = {"physical": ci, "staging": ci, "phys_prod": pp, "prod": pp}
    locs = {"prod": {"database": pp, "schema": ds["phys_prod"]},
            "default": {"database": ci, "schema": ds["physical"]}}
    orders = "{{ config(materialized='table', unique_key='id') }}\nselect id, amount from {{ ref('events') }}\n"
    tvf = ("{{ config(materialized='table_function', parameters='min_amount INT64') }}\n"
           "select id, amount from {{ ref('orders') }} where amount >= min_amount\n")
    udf = ("{{ config(materialized='virtual_function', parameters='x INT64', returns='INT64') }}\n"
           "x + (select max(amount) from {{ ref('orders') }})\n")
    ext_project(sb, "proj_x3", ds, {"physical_locations": locs},
                {"events": EVENTS_V1, "orders": orders, "big_orders": tvf, "amt_plus": udf},
                target_projects={"staging": ci, "prod": pp})
    print(f"\n=== extended 3: two PROJECTS, datasets {x}_* ===")
    iam_probe(sb)

    def tables_of(project: str, dataset: str) -> dict[str, str]:
        rows = sb.clients[project].query(
            f"SELECT table_name, table_type FROM `{project}`.`{dataset}`.INFORMATION_SCHEMA.TABLES"
        ).result()
        return {r["table_name"]: r["table_type"] for r in rows}

    def prod_tables() -> dict[str, str]:
        return tables_of(pp, ds["phys_prod"])

    def q_prod(sql: str) -> list[dict]:
        return [dict(r.items()) for r in sb.clients[pp].query(sql).result()]

    def ids(key: str, name: str) -> list[int]:
        return [r["id"] for r in q_prod(f"SELECT id FROM {sb.fq(key, name)} ORDER BY id")]

    def prod_routines_work() -> bool:
        return ([r["id"] for r in q_prod(
            f"SELECT id FROM {sb.fq('prod', 'big_orders')}(20) ORDER BY id")] == [2, 3]
            and q_prod(f"SELECT {sb.fq('prod', 'amt_plus')}(1) AS v")[0]["v"] == 41)

    sb.cli("x3 build staging b1 (CI project)", "build", "--env", "staging", "--release", "b1")
    fp1 = dict(sb.fingerprints())
    ci_objs = sb.objects("physical") or {}
    sb.check("b1 built in the CI project only",
             f"events__{fp1['events']}" in ci_objs and sb.objects("phys_prod") is None
             and sb.objects("prod") is None, (sorted(ci_objs), sb.objects("phys_prod")))

    # 1. cross-project CLONE
    sb.cli("x3 promote b1 --source-location CI (target prod project)", "promote", "--target",
           "prod", "--from", "staging", "--to", "prod", "--release", "b1",
           "--source-location", f"{ci}.{ds['physical']}")
    tabs = prod_tables()
    sb.check("1. b1 tables in the prod project have table_type CLONE",
             all(tabs.get(f"{m}__{fp1[m]}") == "CLONE" for m in ("events", "orders")), tabs)
    sb.check("1. prod views (prod project) point at the clones in the prod project",
             all(f"`{pp}`.`{ds['phys_prod']}`.`{m}__{fp1[m]}`" in sb.view_target("prod", m)
                 and ci not in sb.view_target("prod", m) for m in ("events", "orders")),
             sb.view_target("prod", "orders"))
    pointers = {r["model_name"] for r in q_prod(
        f"SELECT model_name FROM `{pp}`.`{ds['phys_prod']}`._environments WHERE environment = 'prod'")}
    rel = q_prod(f"SELECT DISTINCT status, source_environment FROM `{pp}`.`{ds['phys_prod']}`._releases "
                 "WHERE release_id = 'b1'")
    sb.check("1. prod registry (prod project) holds pointers and the release rows (source = staging)",
             pointers == {"events", "orders", "big_orders", "amt_plus"}
             and rel == [{"status": "promoted", "source_environment": "staging"}], (pointers, rel))
    ci_prod_rows = sb.registry("_environments", "environment = 'prod'")
    sb.check("1. the CI registry was not written with prod pointers", ci_prod_rows == [], ci_prod_rows)
    sb.check("1. prod rows via views", ids("prod", "orders") == [1, 2, 3] and ids("prod", "events") == [1, 2, 3])

    # 2. routine templates: the project id is rewritten
    tvf_body = sb.routine_body("prod", "big_orders")
    udf_body = sb.routine_body("prod", "amt_plus")
    sb.check("2. prod TVF body reads the prod project's env dataset (CI project id rewritten)",
             f"`{pp}`.`{ds['prod']}`.`orders`" in tvf_body and ci not in tvf_body, tvf_body)
    sb.check("2. prod UDF body reads the prod project's env dataset (CI project id rewritten)",
             f"`{pp}`.`{ds['prod']}`.`orders`" in udf_body and ci not in udf_body, udf_body)
    sb.check("2. prod TVF and UDF answer", prod_routines_work())
    sb.cli("x3 promote b1 again (idempotent)", "promote", "--target", "prod", "--from", "staging",
           "--to", "prod", "--release", "b1", "--source-location", f"{ci}.{ds['physical']}")
    sb.check("1. idempotent: release rows not duplicated", q_prod(
        f"SELECT COUNT(*) AS n FROM `{pp}`.`{ds['phys_prod']}`._releases WHERE release_id = 'b1'"
    )[0]["n"] == 4)

    # 3. the CI project's datasets go away
    for key in ("physical", "staging"):
        sb.clients[ci].delete_dataset(f"{ci}.{ds[key]}", delete_contents=True, not_found_ok=True)
    sb.check("3. CI datasets dropped", sb.objects("physical") is None and sb.objects("staging") is None)
    sb.check("3. prod views and routines still work after the CI datasets were dropped",
             ids("prod", "events") == [1, 2, 3] and ids("prod", "orders") == [1, 2, 3]
             and prod_routines_work())

    # 4. CI seeds an incremental model from the prod project (clone prod -> CI)
    sb.write_models({"events": EVENTS_V2})
    sb.cli("x3 build staging b2 (seed from the prod project)", "build", "--env", "staging",
           "--release", "b2")
    fp2 = dict(sb.fingerprints())
    rows = [(r["id"], r["v"], r["amount"]) for r in sb.q(
        f"SELECT id, v, amount FROM {sb.fq('physical', 'events__' + fp2['events'])} ORDER BY id")]
    sb.check("4. CI events__<fp2> = clone of the prod project's table + delta only (old values kept)",
             rows == [(1, "a", 10), (2, "b", 25), (3, "c", 40), (4, "D", 55)], rows)

    # 5. gates against prod in the other project
    res = sb.cli("x3 diff-data b2 vs prod", "diff-data", "--from", "staging", "--to", "prod",
                 "--release", "b2", "--format", "json", expect_ok=None)
    dd = {mm["model"]: mm for mm in cli_json(res)["models"]} if res.returncode == 0 else {}  # type: ignore[attr-defined]
    sb.check("5. diff-data compares b2 (CI project) with prod's tables (prod project): 1 new row each",
             all(dd.get(n, {}).get("status") == "differs" and dd[n]["only_candidate"] == 1
                 and dd[n]["only_target"] == 0 and dd[n]["changed"] == 0 for n in ("events", "orders")),
             {n: (mm.get("status"), mm.get("only_candidate")) for n, mm in dd.items()})
    res = sb.cli("x3 audit b2 vs prod", "audit", "--from", "staging", "--to", "prod",
                 "--release", "b2", expect_ok=None)
    sb.check("5. audit reads prod's row counts across projects",
             "audit [events]: OK (candidate=4 rows, prod=3 rows)" in res.output,  # type: ignore[attr-defined]
             res.output[-500:])  # type: ignore[attr-defined]
    res = sb.cli("x3 promote b2 --data-diff --max-mismatch-pct 0 (must fail)", "promote", "--target",
                 "prod", "--from", "staging", "--to", "prod", "--release", "b2",
                 "--source-location", f"{ci}.{ds['physical']}", "--data-diff",
                 "--max-mismatch-pct", "0", expect_ok=None)
    sb.check("5. promote --data-diff across projects refuses (b2 differs from prod) and changes nothing",
             res.returncode != 0 and "data diff failed" in res.output  # type: ignore[attr-defined]
             and f"events__{fp1['events']}" in sb.view_target("prod", "events"),
             (res.returncode, res.output[-500:]))  # type: ignore[attr-defined]
    sb.cli("x3 promote b2", "promote", "--target", "prod", "--from", "staging", "--to", "prod",
           "--release", "b2", "--source-location", f"{ci}.{ds['physical']}")
    sb.check("prod.events -> fp2 clone in the prod project",
             f"`{pp}`.`{ds['phys_prod']}`.`events__{fp2['events']}`" in sb.view_target("prod", "events")
             and prod_tables().get(f"events__{fp2['events']}") == "CLONE", prod_tables())
    sb.cli("x3 rollback prod", "rollback", "--target", "prod", "--env", "prod")
    sb.check("rollback to b1 works: prod.events -> fp1",
             f"events__{fp1['events']}" in sb.view_target("prod", "events"), sb.view_target("prod", "events"))


def jobs_via_api(sb: Sandbox, client, project: str) -> list[dict]:
    """Same rows as the JOBS_BY_USER query, through jobs.list / jobs.get (own jobs only)."""
    rows = []
    for item in client.list_jobs(project=project, min_creation_time=sb.started_at, all_users=False):
        if item.job_type != "query":
            continue
        job = client.get_job(item.job_id, project=project, location=item.location)
        if sb.sbx not in (job.query or "") or "INFORMATION_SCHEMA.JOBS" in job.query:
            continue
        rows.append({"creation_time": job.created, "statement_type": job.statement_type,
                     "processed": job.total_bytes_processed, "billed": job.total_bytes_billed,
                     "err": (job.error_result or {}).get("reason")})
    return sorted(rows, key=lambda r: r["creation_time"])


def report_bytes_by_step(sb: Sandbox) -> None:
    """Bytes processed / billed of every query job of the running user, per project, attributed
    to the CLI/dbt step that was running when it was created (INFORMATION_SCHEMA.JOBS_BY_USER)."""
    from google.api_core.exceptions import Forbidden

    for project, client in sb.clients.items():
        sql = (
            "SELECT creation_time, statement_type, total_bytes_processed AS processed, "
            "total_bytes_billed AS billed, error_result.reason AS err "
            f"FROM `{project}`.`region-{sb.location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_USER "
            f"WHERE creation_time >= TIMESTAMP('{sb.started_at:%Y-%m-%d %H:%M:%S}') "
            "AND job_type = 'QUERY' AND state = 'DONE' "
            f"AND query LIKE '%{sb.sbx}%' AND query NOT LIKE '%INFORMATION_SCHEMA.JOBS%' "
            "ORDER BY creation_time"
        )
        rows: list[dict] = []
        for _ in range(12):  # the view lags: wait until the count is stable
            try:
                new = [dict(r.items()) for r in client.query(sql).result()]
            except Forbidden:  # no `bigquery.jobs.list` on the project: list the user's own jobs
                new = jobs_via_api(sb, client, project)
            if rows and len(new) == len(rows):
                break
            rows = new
            time.sleep(10)
        slack = timedelta(seconds=2)
        per: dict[str, list[dict]] = {}
        for r in rows:
            t = r["creation_time"]
            label = next((lbl for lbl, a, b in sb.step_windows if a - slack <= t <= b + slack),
                         "(outside CLI steps: script checks)")
            per.setdefault(label, []).append(r)
        sb.metrics.append(f"bytes per step, project {'CI' if project == sb.project else 'prod'} "
                          f"({len(rows)} jobs, {sum(r['processed'] or 0 for r in rows)} processed, "
                          f"{sum(r['billed'] or 0 for r in rows)} billed):")
        for label, rs in per.items():
            kinds: dict[str, int] = {}
            for r in rs:
                kinds[r["statement_type"] or "?"] = kinds.get(r["statement_type"] or "?", 0) + 1
            sb.metrics.append(
                f"  {label}: {len(rs)} jobs {kinds}, processed {sum(r['processed'] or 0 for r in rs)}, "
                f"billed {sum(r['billed'] or 0 for r in rs)}, errors {sum(1 for r in rs if r['err'])}")


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
    ap.add_argument("--extended", action="store_true",
                    help="Also run the opt-in checks of the newer features (see the module docstring).")
    ap.add_argument("--only-extended", action="store_true",
                    help="Run only the --extended checks, not the base lifecycle.")
    ap.add_argument("--only-cross-location", action="store_true",
                    help="Run only the cross-location extended project (implies --only-extended).")
    ap.add_argument("--prod-project", default=os.environ.get("DBT_VIRTUAL_SBX_PROD_PROJECT"),
                    help="Second GCP project acting as prod (--xproject); --project acts as CI.")
    ap.add_argument("--xproject", action="store_true",
                    help="Run only the cross-PROJECT promote-by-clone checks (needs --prod-project).")
    ns = ap.parse_args()
    if not ns.project:
        ap.error("--project or DBT_VIRTUAL_SBX_PROJECT is required")
    if ns.xproject and (not ns.prod_project or ns.prod_project == ns.project):
        ap.error("--xproject needs --prod-project (or DBT_VIRTUAL_SBX_PROD_PROJECT) "
                 "different from --project")

    sb = Sandbox(ns)
    if sb.sandbox_datasets():
        raise SystemExit(f"datasets with prefix {sb.sbx} already exist; pick another --run-id")
    print(f"sandbox prefix: {sb.sbx}  workdir: {sb.root}")
    sb.write_project()
    step_failures: list[str] = []
    steps = []
    if not ns.only_extended:
        steps.append(lifecycle)
    if ns.xproject:
        steps = [extended_cross_project]
    elif ns.only_cross_location:
        steps = [extended_cross_location]
    elif ns.extended or ns.only_extended:
        steps += [extended_single_location, extended_cross_location]
    try:
        for step in steps:
            try:
                step(sb)
            except StepError as exc:
                step_failures.append(str(exc))
                print(f"\nSTEP FAILED in {step.__name__}: {exc}")
    finally:
        if sb.keep:
            print(f"--keep: sandbox datasets left in place: {sb.sandbox_datasets_by_project()}")
        else:
            print(f"dropped sandbox datasets: {sb.cleanup()}")
            print(f"remaining with prefix {sb.sbx} per project: {sb.sandbox_datasets_by_project()}")
    if ns.xproject:
        report_bytes_by_step(sb)
    for line in sb.metrics:
        print(line)

    failures = [r for r in sb.results if not r[1]]
    print(f"\n{len(sb.results) - len(failures)}/{len(sb.results)} checks passed"
          + "".join(f"; aborted at a step: {f.splitlines()[0]}" for f in step_failures))
    for name, _, detail in failures:
        print(f"  FAIL {name}: {detail[:300]}")
    return 1 if failures or step_failures else 0


if __name__ == "__main__":
    sys.exit(main())
