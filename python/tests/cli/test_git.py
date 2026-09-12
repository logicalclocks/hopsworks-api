"""Unit tests for ``hops git provider`` (registration of Git provider tokens)."""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner
from hopsworks.cli.commands import git as git_cmd


class _Provider:
    def __init__(self, git_provider, username, host):
        self.git_provider = git_provider
        self.username = username
        self.host = host


class _Api:
    """Stand-in for GitProviderApi recording registrations."""

    providers: list = []
    set_calls: list = []
    deleted: list = []

    def _get_providers(self):
        return list(self.providers)

    def _set_provider(self, provider, username, token, host):
        self.set_calls.append((provider, username, token, host))
        self.providers.append(_Provider(provider, username, host))
        return self.providers[-1]

    def _delete_provider(self, provider, host):
        self.deleted.append((provider, host))


@pytest.fixture
def api(monkeypatch):
    _Api.providers = []
    _Api.set_calls = []
    _Api.deleted = []
    monkeypatch.setattr(git_cmd.git_provider_api, "GitProviderApi", _Api)
    monkeypatch.setattr(git_cmd.session, "get_project", lambda ctx: object())
    return _Api


def test_canonical_provider_and_default_host():
    assert git_cmd.canonical_provider("github") == "GitHub"
    assert git_cmd.canonical_provider("GitLab") == "GitLab"
    assert git_cmd.default_host("BitBucket") == "bitbucket.org"
    assert git_cmd.provider_for_host("github.com") == "GitHub"
    assert git_cmd.provider_for_host("git.corp.example") is None
    with pytest.raises(click.BadParameter):
        git_cmd.canonical_provider("svn")


def test_set_registers_with_default_host_and_hidden_token(api):
    result = CliRunner().invoke(
        git_cmd.git_group,
        ["provider", "set", "--username", "jim"],
        input="tok3n\n",
    )
    assert result.exit_code == 0, result.output
    assert api.set_calls == [("GitHub", "jim", "tok3n", "github.com")]
    assert "tok3n" not in result.output  # never echoed


def test_set_is_idempotent_without_force(api):
    api.providers.append(_Provider("GitHub", "jim", "github.com"))
    result = CliRunner().invoke(
        git_cmd.git_group,
        ["provider", "set", "--username", "jim", "--token", "new"],
    )
    assert result.exit_code == 0, result.output
    assert api.set_calls == []
    assert "already registered" in result.output


def test_set_force_replaces(api):
    api.providers.append(_Provider("GitHub", "jim", "github.com"))
    result = CliRunner().invoke(
        git_cmd.git_group,
        ["provider", "set", "--username", "jim", "--token", "new", "--force"],
    )
    assert result.exit_code == 0, result.output
    assert api.set_calls == [("GitHub", "jim", "new", "github.com")]


def test_delete_requires_existing(api):
    result = CliRunner().invoke(git_cmd.git_group, ["provider", "delete"])
    assert result.exit_code != 0
    api.providers.append(_Provider("GitHub", "jim", "github.com"))
    result = CliRunner().invoke(git_cmd.git_group, ["provider", "delete"])
    assert result.exit_code == 0, result.output
    assert api.deleted == [("GitHub", "github.com")]


def test_list_prints_registered_providers(api):
    api.providers.append(_Provider("GitLab", "jim", "gitlab.com"))
    result = CliRunner().invoke(git_cmd.git_group, ["provider", "list"])
    assert result.exit_code == 0, result.output
    assert "GitLab" in result.output and "gitlab.com" in result.output
