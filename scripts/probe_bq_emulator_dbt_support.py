"""Does the running BigQuery emulator support what real dbt-bigquery needs?

`tests/test_dbt_bigquery_emulator_e2e.py` runs the dbt-virtual lifecycle on real dbt-core +
dbt-bigquery. That needs four emulator behaviours that landed after `Flip-to/bigquery-emulator`
`flipto/main` was last checked (see `.beans/dbt-virtual-0031--real-dbt-bigquery-on-emulator.md`):

1. `DROP PROCEDURE [IF EXISTS]` is a no-op (the table-function deploy runs it before CREATE).
2. `tables.get` reports `timePartitioning` / `clustering` of a table created with SQL (dbt-bigquery
   reads them to decide whether it may replace a table).
3. `CREATE TABLE ... CLONE` works (incremental seeding).
4. A clone keeps its partitioning and clustering.

The CI job runs this before the real-dbt step: with all four it runs the step and fails on any
problem; without, it prints a warning that names the missing ones and skips the step. Remove the
gate (and this script) once the emulator PRs listed in the bean are merged.

Environment: `BIGQUERY_EMULATOR_HOST` (REST URL), `BIGQUERY_EMULATOR_PROJECT` (default test-project),
`GITHUB_OUTPUT` (optional: `supported=true|false` is appended).
"""

from __future__ import annotations

import os
import sys
import uuid


def main() -> int:
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    endpoint = os.environ.get("BIGQUERY_EMULATOR_HOST", "http://127.0.0.1:9050")
    project = os.environ.get("BIGQUERY_EMULATOR_PROJECT", "test-project")
    client = bigquery.Client(
        project=project,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=endpoint),
    )
    ds = f"probe_{uuid.uuid4().hex[:8]}"
    client.create_dataset(bigquery.Dataset(f"{project}.{ds}"))
    fq = f"`{project}`.`{ds}`"
    missing: list[str] = []

    def run(sql: str) -> bool:
        try:
            list(client.query(sql).result())
            return True
        except Exception as exc:  # noqa: BLE001 - any failure means "not supported"
            print(f"  probe failed: {sql[:80]!r}: {str(exc)[:160]!r}")
            return False

    def layout(table: str) -> tuple[object, object]:
        t = client.get_table(f"{project}.{ds}.{table}")
        return t.time_partitioning, t.clustering_fields

    try:
        if not run(f"DROP PROCEDURE IF EXISTS {fq}.nothing"):
            missing.append("DROP PROCEDURE IF EXISTS")
        if run(f"CREATE TABLE {fq}.t PARTITION BY d CLUSTER BY id AS SELECT 1 AS id, DATE '2026-01-01' AS d"):
            part, cluster = layout("t")
            if part is None or cluster != ["id"]:
                missing.append("tables.get timePartitioning/clustering after CREATE TABLE")
            if run(f"CREATE TABLE {fq}.c CLONE {fq}.t"):
                part, cluster = layout("c")
                if part is None or cluster != ["id"]:
                    missing.append("CLONE keeps partitioning/clustering")
            else:
                missing.append("CREATE TABLE ... CLONE")
        else:
            missing.append("CREATE TABLE ... PARTITION BY ... CLUSTER BY ... AS SELECT")
    finally:
        client.delete_dataset(f"{project}.{ds}", delete_contents=True, not_found_ok=True)

    supported = not missing
    if supported:
        print("The emulator supports what real dbt-bigquery needs.")
    else:
        print("The emulator lacks: " + "; ".join(missing))
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"supported={'true' if supported else 'false'}\n")
            fh.write(f"missing={'; '.join(missing)}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
