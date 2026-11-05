"""Registry and lease-lock concurrency on DuckDB (in-process, one cursor per worker thread).

The scenarios live in `tests/registry_concurrency.py` and also run against the BigQuery emulator
(`scripts/run_emulator_concurrency_upgrade_e2e.py`) and PostgreSQL
(`scripts/run_snowflake_emulator_e2e.py`). What each one catches is in its docstring.
"""

from __future__ import annotations

import pytest

from tests import registry_concurrency as rc


@pytest.fixture
def backend() -> rc.DuckDBBackend:
    return rc.DuckDBBackend()


def test_flushes_of_different_environments_keep_every_pointer(backend) -> None:
    rc.scenario_flushes_of_different_environments(backend, n=4)


def test_interleaved_upserts_of_one_pointer_leave_one_row(backend) -> None:
    rc.scenario_interleaved_pointer_upserts(backend)


def test_lock_race_has_exactly_one_winner(backend) -> None:
    rc.scenario_lock_race_has_exactly_one_winner(backend, n=4, rounds=3)


def test_legacy_locks_table_without_unique_key_is_upgraded(backend) -> None:
    rc.scenario_legacy_locks_table_is_upgraded(backend)


def test_lease_lifecycle_expired_lease_is_taken_over(backend) -> None:
    rc.scenario_lease_lifecycle(backend)


def test_promote_is_refused_while_the_lease_is_held(backend) -> None:
    rc.scenario_promote_is_refused_while_the_lease_is_held(backend)


def test_simultaneous_promotes_never_mix_releases(backend) -> None:
    rc.scenario_simultaneous_promotes(backend, rounds=2)


def test_same_owner_reacquires_the_lease_known_limitation(backend) -> None:
    rc.scenario_same_owner_reacquires_the_lease(backend)
