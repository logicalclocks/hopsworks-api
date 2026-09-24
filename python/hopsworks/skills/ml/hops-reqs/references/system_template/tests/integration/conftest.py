# ruff: noqa: INP001
"""An integration run that collects nothing fails: zero tests is not a pass."""

from __future__ import annotations

import pytest


def pytest_collection_modifyitems(items):
    """Mark everything under tests/integration so it can be selected by marker too."""
    for item in items:
        item.add_marker(pytest.mark.integration)


def pytest_sessionfinish(session, exitstatus):
    """Turn pytest's "no tests collected" exit status into a failure."""
    if exitstatus == pytest.ExitCode.NO_TESTS_COLLECTED:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
