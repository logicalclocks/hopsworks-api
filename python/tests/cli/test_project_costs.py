"""CLI tests for ``hops project costs``: window and project passing, table and JSON output."""

from __future__ import annotations

import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.cost import CostCoverage, CostRow, NamespaceCosts


def _costs() -> NamespaceCosts:
    return NamespaceCosts(
        window="WEEK",
        start="2026-10-09T09:00:00Z",
        end="2026-10-16T09:00:00Z",
        currency="USD",
        total_cost=8.0,
        gpu_workload_cost=6.0,
        cpu_workload_cost=2.0,
        efficiency=0.37,
        coverage=CostCoverage(observed_buckets=5, total_buckets=7),
        items=[
            CostRow(
                namespace="fraud",
                kind="NAMESPACE",
                department_name="risk",
                cpu_cost=5.0,
                total_cost=8.0,
                gpu_workload_cost=6.0,
                cpu_workload_cost=2.0,
                total_efficiency=0.37,
            )
        ],
    )


@pytest.fixture
def costs_api(mock_project):
    api = mock.MagicMock()
    api.get_project_costs.return_value = _costs()
    with mock.patch("hopsworks.get_costs_api", return_value=api, create=True):
        yield api


def test_project_costs_defaults_to_week_and_the_active_project(costs_api):
    result = CliRunner().invoke(cli, ["project", "costs"])

    assert result.exit_code == 0, result.output
    costs_api.get_project_costs.assert_called_once_with("week", project=None)
    lines = result.output.splitlines()
    assert lines[0].split()[:2] == ["NAMESPACE", "DEPARTMENT"]
    assert lines[2].split()[:2] == ["fraud", "risk"]
    assert "Data for 5 of 7 buckets" in result.output
    assert "Total 8.00 USD, efficiency 37%" in result.output
    assert "idle" not in result.output


def test_project_costs_passes_window_and_project(costs_api):
    result = CliRunner().invoke(
        cli, ["project", "costs", "--window", "Month", "--project", "fraud"]
    )

    assert result.exit_code == 0, result.output
    costs_api.get_project_costs.assert_called_once_with("month", project="fraud")


def test_project_costs_json(costs_api):
    result = CliRunner().invoke(cli, ["--json", "project", "costs"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["total_cost"] == 8.0


def test_project_costs_reports_a_forbidden_project(costs_api):
    response = mock.MagicMock(status_code=403)
    response.json.return_value = {
        "errorCode": 160000,
        "usrMsg": "Your role in this project is not authorized",
    }
    costs_api.get_project_costs.side_effect = RestAPIError("url", response)

    result = CliRunner().invoke(cli, ["project", "costs"])

    assert result.exit_code != 0
    assert "Could not get project costs" in result.output
