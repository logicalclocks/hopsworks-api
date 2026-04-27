"""Tests for ``hops init`` and ``hops update``."""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


# --- hops init -------------------------------------------------------------


def test_init_creates_all_three_templates(tmp_path):
    result = CliRunner().invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".claude/skills/hops/SKILL.md").exists()
    assert (tmp_path / ".claude/commands/hops.md").exists()
    assert (tmp_path / ".claude/agents/hops-fti.md").exists()

    settings = json.loads((tmp_path / ".claude/settings.local.json").read_text())
    assert "Bash(hops *)" in settings["permissions"]["allow"]


def test_init_is_idempotent(tmp_path):
    runner = CliRunner()
    first = runner.invoke(cli, ["init", "--dir", str(tmp_path)])
    assert first.exit_code == 0, first.output
    # Second run must not error out even though every file already exists.
    second = runner.invoke(cli, ["init", "--dir", str(tmp_path)])
    assert second.exit_code == 0, second.output
    assert "already up to date" in (second.stdout + second.stderr)


def test_init_does_not_overwrite_user_edits(tmp_path):
    skill_path = tmp_path / ".claude/skills/hops/SKILL.md"
    skill_path.parent.mkdir(parents=True)
    skill_path.write_text("# customized\n")

    result = CliRunner().invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert skill_path.read_text() == "# customized\n"
    assert "differs" in (result.stdout + result.stderr)


def test_init_force_overwrites(tmp_path):
    skill_path = tmp_path / ".claude/skills/hops/SKILL.md"
    skill_path.parent.mkdir(parents=True)
    skill_path.write_text("# customized\n")

    result = CliRunner().invoke(cli, ["init", "--dir", str(tmp_path), "--force"])
    assert result.exit_code == 0, result.output
    assert skill_path.read_text() != "# customized\n"


def test_init_preserves_existing_settings_permissions(tmp_path):
    settings_path = tmp_path / ".claude/settings.local.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text(
        json.dumps({"permissions": {"allow": ["Bash(git *)"]}, "something_else": 42})
    )
    result = CliRunner().invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    settings = json.loads(settings_path.read_text())
    assert "Bash(git *)" in settings["permissions"]["allow"]
    assert "Bash(hops *)" in settings["permissions"]["allow"]
    assert settings["something_else"] == 42


def test_init_fails_on_malformed_settings(tmp_path):
    settings_path = tmp_path / ".claude/settings.local.json"
    settings_path.parent.mkdir(parents=True)
    settings_path.write_text("this is not json {")
    result = CliRunner().invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code != 0
    assert "parse" in result.output.lower() or "json" in result.output.lower()


# --- hops update -----------------------------------------------------------


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
