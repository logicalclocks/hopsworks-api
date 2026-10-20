"""Lint: every Python file of the system passes ruff with the rules in pyproject.toml.

/hops-build runs `ruff format` and `ruff check --fix` before it tests a phase,
so these fail only on what the fixer cannot decide (an unused variable, a
bare except, a name used before it is defined), which is a bug to fix by hand.
"""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SYSTEM_DIR = Path(__file__).resolve().parents[2]
# The version the rules were written for; uv fetches it when ruff is not installed.
RUFF_VERSION = "0.15.6"


def _ruff() -> list[str]:
    if importlib.util.find_spec("ruff"):
        return [sys.executable, "-m", "ruff"]
    if shutil.which("ruff"):
        return ["ruff"]
    # A Hopsworks terminal ships uv without uvx.
    if shutil.which("uv"):
        return ["uv", "tool", "run", f"ruff@{RUFF_VERSION}"]
    pytest.fail("ruff is not available: pip install ruff, or install uv")


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [*_ruff(), *args, str(SYSTEM_DIR)],
        cwd=SYSTEM_DIR,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def test_the_code_passes_the_linter():
    done = _run("check", "--no-fix")
    assert done.returncode == 0, done.stdout + done.stderr


def test_the_code_is_formatted():
    done = _run("format", "--check")
    assert done.returncode == 0, "run `ruff format .`:\n" + done.stdout + done.stderr
