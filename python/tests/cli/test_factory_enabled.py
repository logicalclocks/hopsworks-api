"""``hops factory`` runs only on a cluster whose factory_enabled variable is true."""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.commands import factory
from hopsworks.cli.main import cli
from hopsworks_common.client import exceptions


REAL_CHECK = factory._require_enabled


@pytest.fixture
def variable(mock_project, monkeypatch):
    monkeypatch.setattr(factory, "_require_enabled", REAL_CHECK)
    with mock.patch(
        "hopsworks_common.core.variable_api.VariableApi._get_variable"
    ) as get:
        yield get


def _missing_variable() -> exceptions.RestAPIError:
    response = mock.MagicMock(status_code=404)
    response.json.return_value = {"errorCode": 100009, "errorMsg": "not found"}
    return exceptions.RestAPIError("variables/factory_enabled", response)


@pytest.mark.parametrize("value", ["false", "", "yes", None])
def test_refuses_every_factory_command_unless_the_variable_is_true(variable, value):
    variable.return_value = value
    for args in (["factory", "list"], ["factory", "system", "list"]):
        result = CliRunner().invoke(cli, args)
        assert result.exit_code == 1
        assert "The Factory is not enabled for this Hopsworks cluster." in result.output
    variable.assert_called_with("factory_enabled")


def test_refuses_when_the_cluster_has_no_variable(variable):
    variable.side_effect = _missing_variable()
    result = CliRunner().invoke(cli, ["factory", "list"])
    assert result.exit_code == 1
    assert "not enabled" in result.output


def test_runs_when_the_variable_is_true(variable, mock_project):
    variable.return_value = "true"
    with mock.patch(
        "hopsworks_common.core.factory_api._list", return_value=[]
    ) as listed:
        result = CliRunner().invoke(cli, ["factory", "list"])
    assert "not enabled" not in result.output
    listed.assert_called_once()


def test_help_needs_no_cluster(variable, monkeypatch):
    monkeypatch.setattr("sys.argv", ["hops", "factory", "list", "--help"])
    result = CliRunner().invoke(cli, ["factory", "list", "--help"])
    assert result.exit_code == 0
    variable.assert_not_called()
