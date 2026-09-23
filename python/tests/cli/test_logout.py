"""Tests for ``hops logout`` — clearing cached credentials in ``~/.hops.toml``."""

from __future__ import annotations

import pytest
from click.testing import CliRunner
from hopsworks.cli import config
from hopsworks.cli.main import cli


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / ".hops.toml")
    monkeypatch.setattr(config, "LEGACY_YAML_PATH", tmp_path / ".hops" / "config")
    for key in (
        "HOPSWORKS_HOST",
        "HOPSWORKS_API_KEY",
        "HOPSWORKS_PROJECT",
        "HOPSWORKS_PROJECT_ID",
        "REST_ENDPOINT",
        "PROJECT_NAME",
        "SECRETS_DIR",
    ):
        monkeypatch.delenv(key, raising=False)
    return tmp_path


def test_logout_clears_cached_profile(tmp_home):
    config.save(
        config.HopsConfig(
            host="https://c.example",
            api_key="K",
            api_key_name="n",
            project="demo",
        )
    )
    result = CliRunner().invoke(cli, ["logout"])

    assert result.exit_code == 0, result.output
    assert "Logged out from https://c.example" in result.output
    assert not (tmp_home / ".hops.toml").exists()
    assert config.load().api_key is None


def test_logout_when_not_logged_in_is_a_noop(tmp_home):
    result = CliRunner().invoke(cli, ["logout"])

    assert result.exit_code == 0, result.output
    assert "Not logged in" in result.output


def test_logout_internal_mode_keeps_pod_credentials(tmp_home, monkeypatch):
    secrets = tmp_home / "secrets"
    secrets.mkdir()
    (secrets / "token.jwt").write_text("jwt")
    monkeypatch.setenv("REST_ENDPOINT", "https://cluster.internal")
    monkeypatch.setenv("SECRETS_DIR", str(secrets))

    result = CliRunner().invoke(cli, ["logout"])

    assert result.exit_code == 0, result.output
    assert "Internal mode" in result.output
