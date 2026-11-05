"""Helpers that materialize an earlier dbt-virtual release (a git tag) for upgrade tests.

`export_release("v0.1.0", dest)` writes `dbt_project.yml`, `macros/` and `src/` of that tag, so a
test can install the old package into a dbt project, drive the old CLI against it, and then
switch the project to the current checkout.
"""

from __future__ import annotations

import io
import os
import subprocess
import sys
import tarfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def tag_available(tag: str) -> bool:
    try:
        res = subprocess.run(
            ["git", "-C", str(REPO), "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}"],
            capture_output=True, check=False,
        )
    except OSError:
        return False
    return res.returncode == 0


def export_release(tag: str, dest: Path) -> Path:
    """Extract the package files of git tag `tag` into `dest` (created) and return it."""
    dest.mkdir(parents=True, exist_ok=True)
    archive = subprocess.run(
        ["git", "-C", str(REPO), "archive", "--format=tar", tag, "dbt_project.yml", "macros", "src"],
        capture_output=True, check=True,
    ).stdout
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(dest, **({"filter": "data"} if hasattr(tarfile, "data_filter") else {}))
    return dest


def run_release_cli(release_dir: Path, project_dir: Path, *args: str) -> subprocess.CompletedProcess:
    """Run the `dbt-virtual` CLI of an exported release against `project_dir`.

    The release's `src/` goes first on `sys.path`; the CLI shells out to `dbt`, so the running
    interpreter's script directory is put on `PATH`.
    """
    code = (
        "import sys; sys.path.insert(0, sys.argv[1]); "
        "from dbt_virtual.cli import main; sys.exit(main(sys.argv[2:]))"
    )
    env = dict(os.environ)
    scripts = str(Path(sys.executable).parent)
    env["PATH"] = scripts + os.pathsep + env.get("PATH", "")
    return subprocess.run(
        [sys.executable, "-c", code, str(release_dir / "src"), "--project-dir", str(project_dir), *args],
        capture_output=True, text=True, check=False, env=env,
    )


def legacy_harness(release_dir: Path, **kwargs):
    """A `MacroTestHarness` that renders the macros of an exported release instead of this checkout's."""
    from tests.test_macros_execution import MacroTestHarness

    files = [
        "fingerprint/get_fingerprint.sql",
        "routing/virtual_routing.sql",
        "adapters/dispatch.sql",
        "lifecycle/hooks.sql",
        "lifecycle/operations.sql",
    ]

    class LegacyHarness(MacroTestHarness):
        def _load_macros(self) -> None:
            sources = [(release_dir / "macros" / f).read_text(encoding="utf-8") for f in files]
            self.template_source = "\n\n".join(sources)

    return LegacyHarness(**kwargs)
