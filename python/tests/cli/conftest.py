"""Shared fixtures for CLI tests.

The central ``mock_project`` fixture patches ``hopsworks.cli.session`` so no
test ever touches ``hopsworks.login()`` or the real SDK. Individual tests
customise the returned ``Project`` mock to shape the command's output.
"""

from __future__ import annotations

from unittest import mock

import pytest
from hopsworks.cli import config, session


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """Redirect HOME and env vars so config operations are hermetic."""
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


@pytest.fixture
def authed_config(tmp_home):
    """Persist a valid-looking config so ``session.require_auth`` passes."""
    config.save(
        config.HopsConfig(
            host="https://c.app.hopsworks.ai",
            api_key="AAA.BBB",
            project="demo",
        )
    )
    return config.load()


@pytest.fixture
def mock_project(authed_config):
    """Yield a fake ``Project`` wired into ``session.get_project``.

    Tests can reach into the returned mock to set return values on feature
    store, job api, model registry, and model serving before invoking the
    CLI with ``CliRunner``.
    """
    project = mock.MagicMock(name="Project")
    project.name = "demo"
    project.id = 119

    fs = mock.MagicMock(name="FeatureStore")
    fs.id = 67
    fs.name = "demo_featurestore"
    project.get_feature_store.return_value = fs

    with (
        mock.patch.object(session, "get_project", return_value=project),
        mock.patch.object(session, "get_feature_store", return_value=fs),
    ):
        yield project
