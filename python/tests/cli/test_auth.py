"""Tests for the CLI's ``hopsworks.login`` wrapper and its logging posture."""

from __future__ import annotations

import logging
from pathlib import Path

import hopsworks
from click.testing import CliRunner
from hopsworks.cli import auth
from hopsworks.cli.main import cli


def test_login_defaults_to_python_engine(monkeypatch):
    """Pyspark on the laptop must not flip the CLI onto the Spark engine."""
    seen = {}
    monkeypatch.setattr(hopsworks, "login", lambda **kw: seen.update(kw) or "proj")
    assert auth.login(host="https://h.example", api_key_value="k") == "proj"
    assert seen["engine"] == "python"
    assert (seen["host"], seen["port"]) == ("h.example", 443)

    seen.clear()
    auth.login(host="http://h.example:8080", engine="spark", internal=False)
    assert (seen["engine"], seen["port"]) == ("spark", 8080)

    seen.clear()
    auth.login(host="", internal=True)
    assert seen == {"hostname_verification": False, "engine": "python"}


def test_cli_quiets_sdk_info_logging(tmp_home):
    """Every command runs with the root logger at WARNING, hiding SDK chatter.

    Uses ``tmp_home`` rather than only setting ``$HOME``: ``config.CONFIG_PATH`` is
    resolved at import time, so overriding the variable alone left this reading the
    developer's own config and calling their cluster.
    """
    logging.getLogger().setLevel(logging.INFO)
    CliRunner().invoke(cli, ["project", "list"])
    assert logging.getLogger().level == logging.WARNING


def test_echo_and_tables_keep_their_order_after_a_quiet_login():
    """Piped stdout, a real process: after the login's stdout swap, a title stays before its table."""
    import subprocess
    import sys

    script = (
        "import sys\n"
        "import click\n"
        "from hopsworks.cli import auth, output\n"
        "sys.stdout = auth._ThreadSilencedStdout(sys.stdout)\n"
        "for t in ('first', 'second'):\n"
        "    click.echo(t)\n"
        "    output.print_table(['id'], [[t + '-row']])\n"
    )
    done = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(Path(__file__).resolve().parents[2]),
    )
    lines = [line.strip() for line in done.stdout.splitlines() if line.strip()]
    assert lines.index("first") < lines.index("first-row") < lines.index("second")
    assert lines.index("second") < lines.index("second-row")
