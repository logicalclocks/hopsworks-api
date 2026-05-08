"""Tests for ``hops setup`` — browser token flow.

We mock out ``requests`` (no real network) and ``auth.verify`` (no SDK load)
so the test can exercise branching without pulling in ``hopsworks``.
"""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import config
from hopsworks.cli.commands import setup as setup_mod
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


def test_suggest_key_name_sanitizes(monkeypatch):
    monkeypatch.setenv("USER", "Jim.Dowling")
    monkeypatch.setattr(setup_mod.socket, "gethostname", lambda: "dev16.hops.works")
    name = setup_mod._suggest_key_name()
    assert name == "jim-dowling-dev16"


def test_setup_short_circuits_when_cached_key_works(tmp_home, monkeypatch):
    config.save(
        config.HopsConfig(
            host="https://c.app.hopsworks.ai",
            api_key="CACHED.KEY",
            api_key_name="jim-laptop",
            project="demo",
        )
    )
    with mock.patch.object(setup_mod.auth, "verify") as mock_verify:
        mock_verify.return_value = mock.Mock(name="Project", id=1)
        mock_verify.return_value.name = "demo"
        result = CliRunner().invoke(cli, ["setup"])

    assert result.exit_code == 0, result.output
    mock_verify.assert_called_once()
    assert "Connected" in result.output or "Connected" in (result.stderr or "")


def test_setup_runs_token_flow_when_forced(tmp_home):
    created_response = {
        "flowId": "tf-abc",
        "waitSecret": "sekret",
        "webUrl": "https://c.app.hopsworks.ai/token-flow/tf-abc",
    }
    wait_response = {
        "apiKey": "NEW.KEY",
        "workspaceUsername": "demo",
        "apiKeyName": "jim-laptop",
        "timeout": False,
    }

    # Two ``requests.post`` calls now: ``/create`` (kicks off the flow) and
    # ``/wait/<flowId>`` (long-poll). The wait endpoint switched to POST + JSON
    # so the wait secret never lands in proxy access logs / browser history /
    # crash reporters as a query string.
    create_mock = mock.Mock()
    create_mock.json.return_value = created_response
    create_mock.raise_for_status = mock.Mock()

    wait_mock = mock.Mock()
    wait_mock.json.return_value = wait_response
    wait_mock.raise_for_status = mock.Mock()

    def _post(url, *args, **kwargs):
        # Route by URL so /create and /wait return the right payload.
        if "/wait/" in url:
            return wait_mock
        return create_mock

    with (
        mock.patch.object(setup_mod.requests, "post", side_effect=_post) as post,
        mock.patch.object(setup_mod, "_open_browser", return_value=True),
        mock.patch.object(setup_mod.auth, "verify") as verify,
    ):
        verify.return_value = mock.Mock()
        verify.return_value.name = "demo"
        result = CliRunner().invoke(
            cli,
            [
                "setup",
                "--host",
                "https://c.app.hopsworks.ai",
                "--key-name",
                "jim-laptop",
                "--force",
            ],
        )

    assert result.exit_code == 0, result.output

    # Find the /create and /wait POSTs.
    create_call = next(c for c in post.call_args_list if "/wait/" not in c.args[0])
    wait_call = next(c for c in post.call_args_list if "/wait/" in c.args[0])

    # /create still uses query params — those have no secrets.
    assert create_call.kwargs["params"]["key_name"] == "jim-laptop"
    assert create_call.kwargs["params"]["utm_source"] == "hops-cli"

    # /wait carries the secret in the JSON body, never in the URL or params.
    assert wait_call.kwargs["json"]["waitSecret"] == "sekret"
    assert "tf-abc" in wait_call.args[0]
    assert "params" not in wait_call.kwargs or "wait_secret" not in (
        wait_call.kwargs.get("params") or {}
    )

    # Key ended up persisted with the server-reported name.
    saved = config.load()
    assert saved.api_key == "NEW.KEY"
    assert saved.api_key_name == "jim-laptop"
    assert saved.project == "demo"


def test_setup_rejects_bad_key_name(tmp_home):
    result = CliRunner().invoke(
        cli,
        [
            "setup",
            "--host",
            "https://c.app.hopsworks.ai",
            "--key-name",
            "has spaces!",
            "--force",
        ],
    )
    assert result.exit_code != 0
    assert "Key name" in result.output


def test_setup_internal_mode_does_not_write_config(tmp_home, monkeypatch):
    secrets = tmp_home / "secrets"
    secrets.mkdir()
    (secrets / "token.jwt").write_text("jwt-here")
    monkeypatch.setenv("REST_ENDPOINT", "https://cluster.internal")
    monkeypatch.setenv("SECRETS_DIR", str(secrets))
    monkeypatch.setenv("PROJECT_NAME", "inside_project")

    with mock.patch.object(setup_mod.auth, "login") as login:
        login.return_value = mock.Mock()
        login.return_value.name = "inside_project"
        result = CliRunner().invoke(cli, ["setup"])

    assert result.exit_code == 0, result.output
    assert not (tmp_home / ".hops.toml").exists()
