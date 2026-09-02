"""Tests for ``hops setup`` — browser token flow.

We mock out ``requests`` (no real network) and ``auth.verify`` (no SDK load)
so the test can exercise branching without pulling in ``hopsworks``.
"""

from __future__ import annotations

import re
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
    assert re.fullmatch(r"jim-dowling-dev16-[0-9a-f]{4}", name), name
    assert name != setup_mod._suggest_key_name()  # unique per run


def test_prefer_host_scheme():
    f = setup_mod._prefer_host_scheme
    assert (
        f("http://c.example/token-flow/tf-1", "https://c.example")
        == "https://c.example/token-flow/tf-1"
    )
    assert f("http://other/x", "https://c.example") == "http://other/x"
    assert f("https://c.example/x", "https://c.example") == "https://c.example/x"


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


def _flow_post(create=None, wait=None):
    """A ``requests.post`` stand-in routing /create and /wait to canned payloads."""
    created = mock.Mock()
    created.json.return_value = create or {
        "flowId": "tf-abc",
        "waitSecret": "sekret",
        "webUrl": "https://c.app.hopsworks.ai/token-flow/tf-abc",
    }
    created.raise_for_status = mock.Mock()
    waited = mock.Mock()
    waited.json.return_value = wait or {
        "apiKey": "NEW.KEY",
        "workspaceUsername": "demo",
        "apiKeyName": "jim-laptop",
        "timeout": False,
    }
    waited.raise_for_status = mock.Mock()

    def _post(url, *args, **kwargs):
        return waited if "/wait/" in url else created

    return _post


def _run_token_flow(argv, wait=None):
    """Invoke `hops setup` with the token flow mocked out, and return the post mock."""
    with (
        mock.patch.object(
            setup_mod.requests, "post", side_effect=_flow_post(wait=wait)
        ) as post,
        mock.patch.object(setup_mod, "_open_browser", return_value=True),
        mock.patch.object(setup_mod.auth, "verify") as verify,
    ):
        verify.return_value = mock.Mock()
        verify.return_value.name = "demo"
        result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 0, result.output
    return post


def test_setup_new_host_drops_cached_project(tmp_home):
    """--host for another cluster must not verify the cached project there."""
    config.save(
        config.HopsConfig(
            host="https://old.example",
            api_key="OLD.KEY",
            api_key_name="jim-laptop",
            project="blah",
            project_id=7,
        )
    )
    create_mock = mock.Mock()
    create_mock.json.return_value = {
        "flowId": "tf-new",
        "waitSecret": "s",
        "webUrl": "https://new.example/token-flow/tf-new",
    }
    wait_mock = mock.Mock()
    wait_mock.json.return_value = {
        "apiKey": "NEW.KEY",
        "workspaceUsername": "fresh",
        "apiKeyName": "jim-laptop",
        "timeout": False,
    }

    def _post(url, *args, **kwargs):
        return wait_mock if "/wait/" in url else create_mock

    with (
        mock.patch.object(setup_mod.requests, "post", side_effect=_post),
        mock.patch.object(setup_mod, "_open_browser", return_value=True),
        mock.patch.object(setup_mod.auth, "verify") as verify,
    ):
        verify.return_value = mock.Mock()
        verify.return_value.name = "fresh"
        result = CliRunner().invoke(
            cli, ["setup", "--host", "https://new.example", "--key-name", "jim-laptop"]
        )

    assert result.exit_code == 0, result.output
    verify.assert_called_once()
    assert verify.call_args.kwargs["project"] == "fresh"
    assert verify.call_args.kwargs["host"] == "https://new.example"
    saved = config.load()
    assert (saved.host, saved.api_key, saved.project) == (
        "https://new.example",
        "NEW.KEY",
        "fresh",
    )
    assert saved.project_id is None
    assert "differs from the cached" in result.output


def test_setup_token_flow_verifies_tls_by_default(tmp_home):
    # Without an explicit opt-out the flow stays strict: the /wait response
    # carries a fresh API key, so the conservative default matters.
    post = _run_token_flow(
        ["setup", "--host", "https://c.app.hopsworks.ai", "--key-name", "k", "--force"],
    )
    for call in post.call_args_list:
        assert call.kwargs["verify"] is True


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


def test_setup_signs_in_again_when_the_cached_key_is_dead(tmp_home):
    """A stale key must not end the command: re-running `hops setup` reconnects."""
    config.save(
        config.HopsConfig(
            host="https://c.app.hopsworks.ai",
            api_key="STALE.KEY",
            api_key_name="old-key",
            project="demo",
        )
    )
    verified = mock.Mock()
    verified.name = "demo"

    with (
        mock.patch.object(setup_mod.requests, "post", side_effect=_flow_post()),
        mock.patch.object(setup_mod, "_open_browser", return_value=True),
        mock.patch.object(
            setup_mod.auth,
            "verify",
            side_effect=[RuntimeError("key revoked"), verified],
        ) as verify,
    ):
        result = CliRunner().invoke(cli, ["setup"])

    assert result.exit_code == 0, result.output
    assert verify.call_count == 2  # the dead cached key, then the fresh one
    assert "signing in again" in result.output
    assert config.load().api_key == "NEW.KEY"


def test_setup_success_prints_a_single_line(tmp_home):
    config.save(
        config.HopsConfig(
            host="https://c.app.hopsworks.ai",
            api_key="CACHED.KEY",
            api_key_name="jim-laptop",
            project="demo",
        )
    )
    verified = mock.Mock()
    verified.name = "demo"

    with mock.patch.object(setup_mod.auth, "verify", return_value=verified):
        result = CliRunner().invoke(cli, ["setup"])

    lines = [line for line in result.output.splitlines() if line.strip()]
    assert lines == ["✓ Connected to https://c.app.hopsworks.ai as demo"], result.output


def test_setup_failure_prints_one_line_naming_the_host(tmp_home):
    with (
        mock.patch.object(
            setup_mod.requests,
            "post",
            side_effect=setup_mod.requests.RequestException("connection refused"),
        ),
        mock.patch.object(setup_mod.auth, "verify"),
    ):
        result = CliRunner().invoke(
            cli, ["setup", "--host", "https://c.app.hopsworks.ai", "--force"]
        )

    assert result.exit_code != 0
    assert (
        "Failed to connect to https://c.app.hopsworks.ai: connection refused"
        in result.output
    )


def test_reason_keeps_one_clipped_line():
    long_error = "first line of the failure\nurl: https://c.example\nbody: " + "x" * 500

    assert setup_mod._reason(long_error) == "first line of the failure"
    assert len(setup_mod._reason("y" * 400)) == 160


def test_setup_failure_names_the_tls_remedy(tmp_home):
    """A self-signed cluster is the common case; the flags that fix it live here."""
    with (
        mock.patch.object(
            setup_mod.requests,
            "post",
            side_effect=setup_mod.requests.exceptions.SSLError(
                "certificate verify failed"
            ),
        ),
        mock.patch.object(setup_mod.auth, "verify"),
    ):
        result = CliRunner().invoke(
            cli, ["setup", "--host", "https://self-signed.example", "--force"]
        )

    assert result.exit_code != 0
    assert "certificate verify failed" in result.output
    assert "--insecure" in result.output and "--ca-bundle" in result.output


def test_setup_failure_without_tls_trouble_has_no_hint(tmp_home):
    with (
        mock.patch.object(
            setup_mod.requests,
            "post",
            side_effect=setup_mod.requests.RequestException("connection refused"),
        ),
        mock.patch.object(setup_mod.auth, "verify"),
    ):
        result = CliRunner().invoke(
            cli, ["setup", "--host", "https://c.app.hopsworks.ai", "--force"]
        )

    assert "--insecure" not in result.output
