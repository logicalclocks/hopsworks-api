# ruff: noqa: INP001
"""Fixtures shared by the unit and integration tests of this ML system.

Unit tests use `system` and `load_fixture` and never connect. Integration tests
use `project` and `test_objects`; an integration run without a connection fails
rather than skipping, because a skipped suite reads as a passing one.

Every object an integration test creates carries `SUFFIX` in its name, so two
runs cannot collide and a leftover names its owner, and goes into
`test_objects`, which deletes it after the test, on failure too.

Regenerate the fixtures after a source schema changes with the command recorded
in `FIXTURES_REGENERATE`.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest
import yaml


SYSTEM_DIR = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
RUN_ID = os.environ.get("HOPS_TEST_RUN_ID") or f"local-{int(time.time())}"
SUFFIX = "_test_" + RUN_ID.replace("-", "_")
FIXTURES_REGENERATE = (
    "python -m <slug_pkg>.fixtures --rows 300  # filled in by the data phase"
)


@pytest.fixture(scope="session")
def system() -> dict:
    """This system's system.yaml, as the tests see it."""
    return yaml.safe_load((SYSTEM_DIR / "system.yaml").read_text(encoding="utf-8"))


def load_fixture(name: str):
    """A fixture under tests/fixtures as a pandas DataFrame (csv, parquet or json)."""
    import pandas as pd

    path = FIXTURES / name
    if path.suffix == ".csv":
        return pd.read_csv(path)
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.DataFrame(json.loads(path.read_text(encoding="utf-8")))


@pytest.fixture(name="load_fixture")
def _load_fixture_fixture():
    return load_fixture


@pytest.fixture(scope="session")
def project():
    """A Hopsworks connection; without one an integration run fails, never skips."""
    try:
        import hopsworks

        return hopsworks.login()
    except Exception as exc:  # noqa: BLE001 - any failure to connect fails the run
        pytest.fail(
            f"integration tests need a Hopsworks connection: {exc}", pytrace=False
        )


@pytest.fixture
def test_objects():
    """Collect what a test creates; each is deleted after the test, even when it fails."""
    created: list = []
    yield created
    failures = []
    for obj in reversed(created):
        try:
            obj.delete()
        except Exception as exc:  # noqa: BLE001 - report every leftover, delete the rest
            failures.append(f"{getattr(obj, 'name', obj)}: {exc}")
    if failures:
        pytest.fail(
            "could not delete test objects: " + "; ".join(failures), pytrace=False
        )
