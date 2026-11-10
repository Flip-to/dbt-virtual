"""dbt-virtual: SQLMesh-style virtual data environments for dbt."""

from importlib.metadata import PackageNotFoundError, version

try:
    # pyproject.toml is the one place the version is written; a hard-coded string here went stale
    # (0.1.0 printed by the 0.2.1 release).
    __version__ = version("dbt-virtual")
except PackageNotFoundError:  # running from a source tree that was never installed
    __version__ = "0+unknown"
