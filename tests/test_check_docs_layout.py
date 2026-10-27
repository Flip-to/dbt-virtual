from __future__ import annotations

from pathlib import Path

from scripts.check_docs_layout import problems, write_indexes


def test_repo_docs_layout_is_clean() -> None:
    repo = Path(__file__).resolve().parent.parent
    write_indexes(repo)
    found = problems(repo)
    assert found == [], f"Docs layout problems found: {found}"
