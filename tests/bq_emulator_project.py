"""A throwaway dbt project run by real dbt-core + dbt-bigquery against a BigQuery emulator.

Wiring (tests only, nothing of this is in the package): `DBT_VIRTUAL_BQ_EMULATOR` holds the
emulator's REST URL (for example `http://127.0.0.1:9250`). dbt-bigquery 1.12 takes the endpoint from
the profile (`api_endpoint`), and the method `oauth-secrets` with a dummy token never contacts
Google, so no patch of the adapter or of `google.cloud.bigquery.Client` is needed. The test helper's
own client uses `AnonymousCredentials` and the same endpoint.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

from dbt_virtual.cli import main as dbt_virtual_main
from tests.test_dbt_duckdb_regressions import ROUTING_SHIM

REPO = Path(__file__).resolve().parent.parent
EMULATOR = os.environ.get("DBT_VIRTUAL_BQ_EMULATOR", "")
PROJECT = os.environ.get("DBT_VIRTUAL_BQ_EMULATOR_PROJECT", "test-project")


def emulator_unavailable_reason() -> str | None:
    """Why the real-dbt BigQuery tests cannot run here (None when they can)."""
    if not EMULATOR:
        return "DBT_VIRTUAL_BQ_EMULATOR (emulator REST URL) is not set"
    try:
        import dbt.adapters.bigquery  # noqa: F401
        import google.cloud.bigquery  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on the environment
        return f"dbt-bigquery / google-cloud-bigquery is not installed: {exc}"
    return None


def bq_client():
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    return bigquery.Client(
        project=PROJECT,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=EMULATOR),
    )


class BqProject:
    """A dbt project on BigQuery with `staging`, `prod` and extra ephemeral environments.

    Dataset names carry a random token so tests can share one emulator instance.
    """

    def __init__(
        self,
        root: Path,
        models: dict[str, str],
        extra_targets: tuple[str, ...] = ("pr_1",),
        prod_datasets: dict[str, str] | None = None,
    ) -> None:
        """`prod_datasets` maps each custom schema of prod to a dataset suffix (`default` is the
        dbt target schema); by default prod is one dataset."""
        tag = uuid.uuid4().hex[:8]
        self.tag = tag
        self.physical = f"p{tag}_physical"
        self.raw = f"p{tag}_raw"
        self.schemas = {"staging": f"p{tag}_staging", "prod": f"p{tag}_prod"}
        for t in extra_targets:
            self.schemas[t] = f"p{tag}_{t}"
        self.dir = root / "proj"
        (self.dir / "models").mkdir(parents=True)
        (self.dir / "macros").mkdir()
        pkg_dir = root / "dbt_virtual_pkg"
        pkg_dir.mkdir(parents=True)
        shutil.copy2(REPO / "dbt_project.yml", pkg_dir / "dbt_project.yml")
        shutil.copytree(REPO / "macros", pkg_dir / "macros")
        (self.dir / "packages.yml").write_text(
            f'packages:\n  - local: "{pkg_dir.as_posix()}"\n', encoding="utf-8"
        )
        env_schemas = "".join(
            f'      {k}: "{v}"\n' for k, v in self.schemas.items() if k != "prod"
        )
        if prod_datasets:
            env_schemas += "      prod:\n" + "".join(
                f'        {key}: "p{tag}_{suffix}"\n' for key, suffix in prod_datasets.items()
            )
        else:
            env_schemas += f'      prod: "{self.schemas["prod"]}"\n'
        (self.dir / "dbt_project.yml").write_text(
            "name: 'bqe2e'\n"
            "version: '1.0.0'\n"
            "config-version: 2\n"
            "profile: 'bqe2e'\n"
            'model-paths: ["models"]\n'
            'macro-paths: ["macros"]\n'
            "on-run-start:\n"
            '  - "{{ dbt_virtual.seed_incremental_clones() }}"\n'
            "vars:\n"
            "  dbt_virtual:\n"
            f'    physical_schema: "{self.physical}"\n'
            "    environment: \"{{ var('_dbt_virtual_env', target.name) }}\"\n"
            '    base_environment: "prod"\n'
            "    env_schemas:\n"
            f"{env_schemas}"
            "models:\n"
            "  bqe2e:\n"
            "    +virtual_env: true\n"
            "    +pre-hook:\n"
            '      - "{{ dbt_virtual.clone_if_new_fp() }}"\n'
            "    +post-hook:\n"
            '      - "{{ dbt_virtual.sync_env_view() }}"\n',
            encoding="utf-8",
        )
        (self.dir / "macros" / "routing.sql").write_text(ROUTING_SHIM, encoding="utf-8")
        outputs = "".join(
            f"    {name}:\n"
            "      type: bigquery\n"
            "      method: oauth-secrets\n"
            "      token: not-a-real-token\n"
            f"      project: {PROJECT}\n"
            f"      dataset: {schema}\n"
            "      location: US\n"
            f"      api_endpoint: {EMULATOR}\n"
            "      threads: 1\n"
            "      job_retries: 0\n"
            for name, schema in self.schemas.items()
        )
        (self.dir / "profiles.yml").write_text(
            f"bqe2e:\n  target: staging\n  outputs:\n{outputs}", encoding="utf-8"
        )
        self.client = bq_client()
        from google.cloud import bigquery

        for ds in (self.physical, self.raw, *self.schemas.values()):
            self.client.create_dataset(bigquery.Dataset(f"{PROJECT}.{ds}"), exists_ok=True)
        # A source table outside every environment, for `source()` models.
        self.query(f"create table `{PROJECT}.{self.raw}.raw_events` as select 1 as id union all select 2")
        (self.dir / "models" / "sources.yml").write_text(
            "version: 2\n"
            "sources:\n"
            "  - name: raw\n"
            f"    schema: {self.raw}\n"
            "    tables:\n"
            "      - name: raw_events\n",
            encoding="utf-8",
        )
        self.write_models(models)
        self.dbt("deps")

    # -- dbt -----------------------------------------------------------------------------------
    def write_models(self, models: dict[str, str]) -> None:
        for name, sql in models.items():
            (self.dir / "models" / f"{name}.sql").write_text(sql, encoding="utf-8")

    def _env(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        env = dict(os.environ)
        env.update(extra or {})
        return env

    def dbt(self, *args: str, expect_ok: bool = True, env: dict[str, str] | None = None):
        cmd = [sys.executable, "-m", "dbt.cli.main", *args, "--profiles-dir", str(self.dir)]
        res = subprocess.run(
            cmd, cwd=self.dir, capture_output=True, text=True, check=False, env=self._env(env)
        )
        if expect_ok and res.returncode != 0:
            raise AssertionError(f"{' '.join(args)} failed:\n{res.stdout}\n{res.stderr}")
        return res

    def cli(self, *args: str, env: dict[str, str] | None = None) -> int:
        """Run `dbt-virtual` in-process; the dbt subprocesses it spawns inherit `env`."""
        wanted = self._env(env)
        saved = {k: os.environ.get(k) for k in wanted}
        os.environ.update(wanted)
        try:
            return dbt_virtual_main(["--project-dir", str(self.dir), *args])
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def operation(self, name: str, args: dict, *, env: str = "staging", expect_ok: bool = True):
        return self.dbt(
            "run-operation", name, "--target", env, "--args", json.dumps(args), expect_ok=expect_ok
        )

    def fingerprints(self, env: str = "staging") -> dict[str, str]:
        self.dbt("compile", "--target", env)
        assert self.cli("plan", "--env", env, "--skip-compile") == 0
        data = json.loads((self.dir / ".dbt_virtual" / "fingerprints.json").read_text("utf-8"))
        return data["by_name"]

    # -- warehouse -----------------------------------------------------------------------------
    def query(self, sql: str) -> list[tuple]:
        job = self.client.query(sql)
        return [tuple(r.values()) for r in job.result()]

    def tables(self, dataset: str) -> list[tuple[str, str]]:
        return sorted(
            (t.table_id, t.table_type) for t in self.client.list_tables(f"{PROJECT}.{dataset}")
        )

    def drop_everything(self) -> None:
        """Drop every dataset of this project (they all start with `p<tag>_`)."""
        try:
            datasets = [d.dataset_id for d in self.client.list_datasets()]
        except Exception:  # noqa: BLE001 - best-effort cleanup on a throwaway emulator
            return
        for ds in datasets:
            if ds.startswith(f"p{self.tag}_"):
                try:
                    self.client.delete_dataset(
                        f"{PROJECT}.{ds}", delete_contents=True, not_found_ok=True
                    )
                except Exception:  # noqa: BLE001, S110
                    pass
