"""Tests for the ``--verify/--no-verify`` flag wiring.

The flag mirrors the SDK's ``hopsworks.login(hostname_verification=...)``: it
threads from the CLI (root flag, the ``login`` command, or appended after any
subcommand) through :mod:`hopsworks.cli.config` and
:mod:`hopsworks.cli.session` down to ``hopsworks.login``. These tests assert
each hand-off without touching the real SDK.
"""

from __future__ import annotations

import sys
from unittest import mock

import click
from click.testing import CliRunner
from hopsworks.cli import auth, config, main, session
from hopsworks.cli.main import cli


# --- root flag -> config.load ----------------------------------------------


def _capture_load(monkeypatch):
    """Replace ``main.config.load`` with a spy and return the captured kwargs."""
    captured: dict = {}

    def spy(**kwargs):
        captured.update(kwargs)
        return config.HopsConfig()

    monkeypatch.setattr(main.config, "load", spy)
    return captured


def test_root_no_verify_forwarded_to_config_load(tmp_home, monkeypatch):
    captured = _capture_load(monkeypatch)
    CliRunner().invoke(cli, ["--no-verify", "fg", "list"])
    assert captured["flag_hostname_verification"] is False


def test_root_verify_forwarded_to_config_load(tmp_home, monkeypatch):
    captured = _capture_load(monkeypatch)
    CliRunner().invoke(cli, ["--verify", "fg", "list"])
    assert captured["flag_hostname_verification"] is True


def test_root_without_flag_forwards_none(tmp_home, monkeypatch):
    captured = _capture_load(monkeypatch)
    CliRunner().invoke(cli, ["fg", "list"])
    assert captured["flag_hostname_verification"] is None


# --- explicitness tracking (drives the hops sql default) --------------------


def test_default_is_not_explicit(tmp_home):
    cfg = config.load()
    assert cfg.hostname_verification is False
    assert cfg.hostname_verification_explicit is False


def test_flag_marks_explicit(tmp_home):
    assert config.load(flag_hostname_verification=False).hostname_verification_explicit
    assert config.load(flag_hostname_verification=True).hostname_verification_explicit


def test_env_marks_explicit(tmp_home, monkeypatch):
    monkeypatch.setenv("HOPSWORKS_HOSTNAME_VERIFICATION", "false")
    cfg = config.load()
    assert cfg.hostname_verification is False
    assert cfg.hostname_verification_explicit is True


# --- flag appended after a subcommand (LLM-style, left-to-right) ------------


def _run_capturing_login(tmp_home, argv):
    """Invoke ``argv`` and return the ``hostname_verification`` seen at login."""
    config.save(config.HopsConfig(host="https://h", api_key="k", project="p"))
    seen: dict = {}

    def capture(ctx):
        seen["hv"] = ctx.obj["config"].hostname_verification
        raise click.exceptions.Exit(0)

    with mock.patch.object(session, "get_project", side_effect=capture):
        CliRunner().invoke(cli, argv)
    return seen.get("hv")


def test_no_verify_after_subcommand(tmp_home):
    assert _run_capturing_login(tmp_home, ["context", "--no-verify"]) is False


def test_verify_after_subcommand(tmp_home):
    assert _run_capturing_login(tmp_home, ["context", "--verify"]) is True


def test_subcommand_without_flag_uses_saved_value(tmp_home):
    config.save(
        config.HopsConfig(
            host="https://h", api_key="k", project="p", hostname_verification=True
        )
    )
    seen: dict = {}

    def capture(ctx):
        seen["hv"] = ctx.obj["config"].hostname_verification
        raise click.exceptions.Exit(0)

    with mock.patch.object(session, "get_project", side_effect=capture):
        CliRunner().invoke(cli, ["context"])
    assert seen["hv"] is True


# --- session.get_project -> auth.login -------------------------------------


def test_get_project_passes_hostname_verification():
    cfg = config.HopsConfig(
        host="https://h", api_key="k", project="p", hostname_verification=True
    )
    ctx = click.Context(cli)
    ctx.obj = {"config": cfg}
    fake_project = mock.MagicMock(name="Project")
    captured: dict = {}

    def fake_login(**kwargs):
        captured.update(kwargs)
        return fake_project

    with mock.patch.object(session.auth, "login", side_effect=fake_login):
        out = session.get_project(ctx)

    assert out is fake_project
    assert captured["hostname_verification"] is True


# --- auth.login / auth.verify -> hopsworks.login ---------------------------


def test_auth_login_forwards_to_sdk(monkeypatch):
    fake_hopsworks = mock.MagicMock()
    monkeypatch.setitem(sys.modules, "hopsworks", fake_hopsworks)

    auth.login(host="https://h", api_key_value="k", hostname_verification=False)
    assert fake_hopsworks.login.call_args.kwargs["hostname_verification"] is False

    auth.login(host="https://h", api_key_value="k", hostname_verification=True)
    assert fake_hopsworks.login.call_args.kwargs["hostname_verification"] is True


def test_auth_login_internal_forwards_to_sdk(monkeypatch):
    fake_hopsworks = mock.MagicMock()
    monkeypatch.setitem(sys.modules, "hopsworks", fake_hopsworks)

    auth.login(host="ignored", internal=True, hostname_verification=True)
    assert fake_hopsworks.login.call_args.kwargs["hostname_verification"] is True


def test_auth_verify_forwards_to_sdk(monkeypatch):
    fake_hopsworks = mock.MagicMock()
    monkeypatch.setitem(sys.modules, "hopsworks", fake_hopsworks)

    auth.verify(host="https://h", api_key_value="k", hostname_verification=False)
    assert fake_hopsworks.login.call_args.kwargs["hostname_verification"] is False


# --- trino _connect -> TrinoApi.connect(verify=...) ------------------------


def _trino_connect_verify(hostname_verification, *, explicit=True):
    """Return the ``verify`` kwarg ``trino._connect`` passes to ``api.connect``."""
    from hopsworks.cli.commands import trino

    cfg = config.HopsConfig(
        host="https://h",
        api_key="k",
        project="p",
        hostname_verification=hostname_verification,
        hostname_verification_explicit=explicit,
    )
    ctx = click.Context(cli)
    ctx.obj = {"config": cfg}

    fake_api = mock.MagicMock(name="TrinoApi")
    fake_project = mock.MagicMock(name="Project")
    fake_project.get_trino_api.return_value = fake_api
    fake_project.name = "p"

    with mock.patch.object(session, "get_project", return_value=fake_project):
        trino._connect(ctx, catalog="iceberg", schema="db")
    return fake_api.connect.call_args.kwargs["verify"]


def test_trino_connect_defaults_to_verify():
    # hops sql verifies the Trino coordinator by default (unlike REST/login),
    # so a query against a properly-certed cluster is secure with no flag.
    assert _trino_connect_verify(False, explicit=False) is True


def test_trino_connect_forwards_no_verify():
    # An explicit --no-verify must disable Trino TLS verification, otherwise a
    # self-signed / IP-SAN-mismatched coordinator cert makes ``hops sql``
    # unreachable.
    assert _trino_connect_verify(False, explicit=True) is False


def test_trino_connect_forwards_verify():
    assert _trino_connect_verify(True, explicit=True) is True


# --- discoverability --------------------------------------------------------


def test_root_help_lists_verify():
    out = CliRunner().invoke(cli, ["--help"]).output
    assert "--verify" in out
    assert "--no-verify" in out


def test_login_help_lists_verify():
    out = CliRunner().invoke(cli, ["login", "--help"]).output
    assert "--no-verify" in out
