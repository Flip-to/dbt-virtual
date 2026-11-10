import re
from pathlib import Path

import dbt_virtual


def test_version_matches_pyproject():
    # The 0.2.1 release printed `dbt-virtual 0.1.0`: __version__ was a hard-coded string.
    text = (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    declared = re.search(r'(?m)^version\s*=\s*"([^"]+)"', text).group(1)
    assert dbt_virtual.__version__ == declared
