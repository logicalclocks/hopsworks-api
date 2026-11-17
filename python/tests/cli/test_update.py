"""Tests for ``hops update``."""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


def test_update_prints_command(tmp_path, monkeypatch):
    # Force the "virtualenv" branch for a deterministic message.
    fake_prefix = str(tmp_path / "venv")
    monkeypatch.setattr("sys.prefix", fake_prefix)
    monkeypatch.setattr("sys.base_prefix", "/usr")

    with (
        mock.patch("hopsworks.cli.commands.update._is_editable", return_value=False),
        mock.patch.dict("os.environ", {}, clear=False),
    ):
        result = CliRunner().invoke(cli, ["update"])

    assert result.exit_code == 0, result.output
    assert "pip install --upgrade hopsworks" in result.output


def test_update_detects_hopsworks_venv(monkeypatch):
    monkeypatch.setattr("sys.prefix", "/srv/hops/venv")
    monkeypatch.setattr("sys.base_prefix", "/usr")
    result = CliRunner().invoke(cli, ["update"])
    assert result.exit_code == 0, result.output
    assert "Hopsworks-managed" in result.output


def test_update_json_mode(monkeypatch):
    monkeypatch.setattr("sys.prefix", "/home/u/venv")
    monkeypatch.setattr("sys.base_prefix", "/usr")
    with mock.patch("hopsworks.cli.commands.update._is_editable", return_value=False):
        result = CliRunner().invoke(cli, ["--json", "update"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert "upgrade_command" in payload
    assert payload["version"]
