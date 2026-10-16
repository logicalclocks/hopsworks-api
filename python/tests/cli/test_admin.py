"""CLI tests for ``hops admin costs``: window passing, table rendering and JSON output."""

from __future__ import annotations

import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks_common.cost import CostRow, NamespaceCosts


def _costs() -> NamespaceCosts:
    return NamespaceCosts(
        window="WEEK",
        start="2026-10-09T09:00:00Z",
        end="2026-10-16T09:00:00Z",
        currency="USD",
        total_cost=10.0,
        idle_cost=2.0,
        efficiency=0.42,
        items=[
            CostRow(namespace="__idle__", kind="IDLE", cpu_cost=2.0, total_cost=2.0),
            CostRow(
                namespace="fraud",
                kind="NAMESPACE",
                cpu_cost=5.0,
                ram_cost=2.5,
                pv_cost=0.5,
                total_cost=8.0,
                total_efficiency=0.37,
            ),
        ],
    )


@pytest.fixture
def costs_api(mock_project):
    api = mock.MagicMock()
    api.get_namespace_costs.return_value = _costs()
    with mock.patch("hopsworks.get_costs_api", return_value=api, create=True):
        yield api


def test_admin_costs_defaults_to_week(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code == 0, result.output
    costs_api.get_namespace_costs.assert_called_once_with("week")


def test_admin_costs_passes_window_lower_case(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs", "--window", "MONTH"])

    assert result.exit_code == 0, result.output
    costs_api.get_namespace_costs.assert_called_once_with("month")


def test_admin_costs_rejects_unknown_window(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs", "--window", "fortnight"])

    assert result.exit_code != 0
    costs_api.get_namespace_costs.assert_not_called()


def test_admin_costs_table_sorted_by_total_with_share(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert "NAMESPACE" in lines[0]
    assert "EFFICIENCY" in lines[0]
    fraud = lines[2].split()
    assert fraud == [
        "fraud",
        "5.00",
        "0.00",
        "2.50",
        "0.50",
        "0.00",
        "0.00",
        "8.00",
        "80%",
        "37%",
    ]
    idle = lines[3].split()
    assert idle[0] == "__idle__"
    assert idle[-2:] == ["20%", "-"]
    assert lines[4] == (
        "Total 10.00 USD, idle 2.00, efficiency 42%, "
        "week from 2026-10-09T09:00:00Z to 2026-10-16T09:00:00Z (UTC)"
    )


def test_admin_costs_json(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["total_cost"] == 10.0
    assert payload["items"][1]["namespace"] == "fraud"


def test_admin_costs_reports_backend_failure(costs_api):
    costs_api.get_namespace_costs.side_effect = RuntimeError("OpenCost is down")

    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code != 0
    assert "OpenCost is down" in result.output
