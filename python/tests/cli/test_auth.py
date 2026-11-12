"""Tests for the CLI's ``hopsworks.login`` wrapper and its logging posture."""

from __future__ import annotations

import logging

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


def test_sdk_prints_stay_off_stdout_only_in_json_mode(capsys):
    """`--json` keeps stdout for the payload: the SDK's banners (job created, launching job) are dropped."""
    from hopsworks.cli import output

    try:
        output.set_json_mode(True)
        with output.sdk_quiet():
            print("Job created successfully, explore it at https://h/p/1/jobs/named/x")
        output.print_json({"job": "x", "execution": 7})
        assert capsys.readouterr().out == '{\n  "job": "x",\n  "execution": 7\n}\n'

        output.set_json_mode(False)
        with output.sdk_quiet():
            print("Launching job: x")
        assert capsys.readouterr().out == "Launching job: x\n"
    finally:
        output.set_json_mode(False)


def test_a_nested_quiet_keeps_the_outer_silence(capsys):
    """An inner SDK call's quiet must not end the silence of the block around it."""
    with auth.quiet():
        with auth.quiet():
            print("inner banner")
        print("outer banner")
    print("after")
    assert capsys.readouterr().out == "after\n"
