"""Unit tests for ``hopsworks.cli.config``.

Covers the env/flag/file precedence chain, atomic write + mode 0600,
internal-mode detection, and the one-shot migration from the Go CLI's YAML.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import pytest
from hopsworks.cli import config


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """Redirect HOME so tests never touch the developer's real ~/.hops.toml."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / ".hops.toml")
    monkeypatch.setattr(config, "LEGACY_YAML_PATH", tmp_path / ".hops" / "config")
    for key in (
        "HOPSWORKS_HOST",
        "HOPSWORKS_API_KEY",
        "HOPSWORKS_PROJECT",
        "HOPSWORKS_PROJECT_ID",
        "HOPSWORKS_HOSTNAME_VERIFICATION",
        "REST_ENDPOINT",
        "PROJECT_NAME",
        "SECRETS_DIR",
    ):
        monkeypatch.delenv(key, raising=False)
    return tmp_path


def test_load_returns_empty_when_nothing_set(tmp_home):
    cfg = config.load()
    assert cfg.host is None
    assert cfg.api_key is None
    assert cfg.internal is False
    assert cfg.is_authenticated() is False


def test_save_round_trip_preserves_values(tmp_home):
    cfg = config.HopsConfig(
        host="https://c.app.hopsworks.ai",
        api_key="AAA.BBB",
        api_key_name="jim-laptop",
        project="demo",
        project_id=119,
        feature_store_id=67,
    )
    config.save(cfg)

    loaded = config.load()
    assert loaded.host == "https://c.app.hopsworks.ai"
    assert loaded.api_key == "AAA.BBB"
    assert loaded.api_key_name == "jim-laptop"
    assert loaded.project == "demo"
    assert loaded.project_id == 119
    assert loaded.feature_store_id == 67


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="NTFS does not honor POSIX 0o600 permission bits",
)
def test_save_writes_0600_permissions(tmp_home):
    config.save(config.HopsConfig(host="h", api_key="k"))
    mode = stat.S_IMODE(os.stat(config.CONFIG_PATH).st_mode)
    assert mode == 0o600


def test_flags_take_precedence_over_file(tmp_home):
    config.save(
        config.HopsConfig(host="file-host", api_key="file-key", project="file-proj")
    )
    cfg = config.load(
        flag_host="flag-host",
        flag_api_key="flag-key",
        flag_project="flag-proj",
    )
    assert cfg.host == "flag-host"
    assert cfg.api_key == "flag-key"
    assert cfg.project == "flag-proj"


def test_env_overrides_file_but_flags_override_env(tmp_home, monkeypatch):
    config.save(config.HopsConfig(host="file-host", api_key="file-key"))
    monkeypatch.setenv("HOPSWORKS_HOST", "env-host")
    monkeypatch.setenv("HOPSWORKS_API_KEY", "env-key")

    cfg = config.load()
    assert cfg.host == "env-host"
    assert cfg.api_key == "env-key"

    cfg2 = config.load(flag_host="flag-host")
    assert cfg2.host == "flag-host"
    assert cfg2.api_key == "env-key"


def test_internal_mode_detected_from_env(tmp_home, monkeypatch):
    secrets = tmp_home / "secrets"
    secrets.mkdir()
    (secrets / "token.jwt").write_text("   jwt-here  \n")
    monkeypatch.setenv("REST_ENDPOINT", "https://cluster.internal")
    monkeypatch.setenv("SECRETS_DIR", str(secrets))
    monkeypatch.setenv("PROJECT_NAME", "inside_project")
    monkeypatch.setenv("HOPSWORKS_PROJECT_ID", "42")

    cfg = config.load()
    assert cfg.internal is True
    assert cfg.host == "https://cluster.internal"
    assert cfg.project == "inside_project"
    assert cfg.project_id == 42
    assert cfg.jwt_token == "jwt-here"


def test_migrate_legacy_yaml_once(tmp_home):
    legacy = tmp_home / ".hops" / "config"
    legacy.parent.mkdir()
    legacy.write_text(
        "host: https://legacy.hopsworks.ai\n"
        'api_key: "LEGACY.KEY"\n'
        "project: old_proj\n"
        "project_id: 7\n"
    )

    cfg = config.load()
    assert cfg.host == "https://legacy.hopsworks.ai"
    assert cfg.api_key == "LEGACY.KEY"
    assert cfg.project == "old_proj"
    assert cfg.project_id == 7
    # Migration should now have written the TOML file.
    assert Path(config.CONFIG_PATH).exists()


def test_clear_removes_profile(tmp_home):
    config.save(config.HopsConfig(host="h", api_key="k"))
    assert config.CONFIG_PATH.exists()
    config.clear()
    assert not config.CONFIG_PATH.exists()


# --- hostname verification -------------------------------------------------


def test_hostname_verification_defaults_off(tmp_home):
    assert config.load().hostname_verification is False


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("true", True),
        ("TRUE", True),
        ("1", True),
        ("y", True),
        ("yes", True),
        ("  Yes  ", True),
        ("false", False),
        ("0", False),
        ("no", False),
        ("", False),
        (None, False),
    ],
)
def test_env_truthy_matches_sdk_parsing(raw, expected):
    assert config._env_truthy(raw) is expected


def test_hostname_verification_read_from_file(tmp_home):
    config.save(config.HopsConfig(host="h", api_key="k", hostname_verification=True))
    assert config.load().hostname_verification is True


def test_save_omits_hostname_verification_when_off(tmp_home):
    config.save(config.HopsConfig(host="h", api_key="k"))
    assert "hostname_verification" not in config.CONFIG_PATH.read_text()


def test_env_overrides_file_for_hostname_verification(tmp_home, monkeypatch):
    config.save(config.HopsConfig(host="h", api_key="k", hostname_verification=True))
    monkeypatch.setenv("HOPSWORKS_HOSTNAME_VERIFICATION", "false")
    assert config.load().hostname_verification is False


def test_flag_overrides_env_for_hostname_verification(tmp_home, monkeypatch):
    monkeypatch.setenv("HOPSWORKS_HOSTNAME_VERIFICATION", "true")
    assert config.load(flag_hostname_verification=False).hostname_verification is False
    assert config.load(flag_hostname_verification=True).hostname_verification is True
    # None means "not passed" — the env value stands.
    assert config.load(flag_hostname_verification=None).hostname_verification is True


def test_env_applies_in_internal_mode(tmp_home, monkeypatch):
    secrets = tmp_home / "secrets"
    secrets.mkdir()
    (secrets / "token.jwt").write_text("jwt\n")
    monkeypatch.setenv("REST_ENDPOINT", "https://cluster.internal")
    monkeypatch.setenv("SECRETS_DIR", str(secrets))
    monkeypatch.setenv("HOPSWORKS_HOSTNAME_VERIFICATION", "true")

    cfg = config.load()
    assert cfg.internal is True
    assert cfg.hostname_verification is True
