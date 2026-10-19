"""CLI tests for ``hops admin costs``: window and view passing, table rendering and JSON output."""

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
        total_cost=10.0,
        idle_cost=2.0,
        gpu_workload_cost=6.0,
        cpu_workload_cost=2.0,
        efficiency=0.42,
        coverage=CostCoverage(observed_buckets=7, total_buckets=7),
        items=[
            CostRow(
                namespace="__idle__",
                kind="IDLE",
                cpu_cost=2.0,
                total_cost=2.0,
                share=0.2,
            ),
            CostRow(
                namespace="fraud",
                kind="NAMESPACE",
                department_id=7,
                department_name="risk",
                cpu_cost=5.0,
                ram_cost=2.5,
                pv_cost=0.5,
                total_cost=8.0,
                gpu_workload_cost=6.0,
                cpu_workload_cost=2.0,
                total_efficiency=0.37,
                share=0.8,
            ),
        ],
    )


def _department_costs() -> NamespaceCosts:
    costs = _costs()
    costs.items = [
        CostRow(
            kind="UNASSIGNED",
            namespace_count=3,
            cpu_cost=1.0,
            total_cost=1.0,
            cpu_workload_cost=1.0,
            share=0.1,
        ),
        CostRow(namespace="__idle__", kind="IDLE", total_cost=2.0, share=0.2),
        CostRow(
            kind="DEPARTMENT",
            department_id=7,
            department_name="risk",
            namespace_count=2,
            cpu_cost=7.0,
            total_cost=7.0,
            gpu_workload_cost=6.0,
            cpu_workload_cost=1.0,
            total_efficiency=0.5,
            share=0.7,
        ),
    ]
    return costs


@pytest.fixture
def costs_api(mock_project):
    api = mock.MagicMock()
    api.get_namespace_costs.return_value = _costs()
    api.get_department_costs.return_value = _department_costs()
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
    assert lines[0].split()[:2] == ["NAMESPACE", "DEPARTMENT"]
    assert "EFFICIENCY" in lines[0]
    assert "GPU WORKLOADS" in lines[0]
    assert "CPU WORKLOADS" in lines[0]
    fraud = lines[2].split()
    assert fraud == [
        "fraud",
        "risk",
        "5.00",
        "0.00",
        "2.50",
        "0.50",
        "0.00",
        "0.00",
        "8.00",
        "6.00",
        "2.00",
        "80%",
        "37%",
    ]
    idle = lines[3].split()
    assert idle[:2] == ["__idle__", "-"]
    assert idle[-4:] == ["-", "-", "20%", "-"]
    assert lines[4] == (
        "Total 10.00 USD, idle 2.00, efficiency 42%, "
        "week from 2026-10-09T09:00:00Z to 2026-10-16T09:00:00Z (UTC)"
    )
    assert lines[5] == "GPU workloads: 6.00 | CPU workloads: 2.00"


def test_admin_costs_prints_coverage_when_incomplete(costs_api):
    costs = _costs()
    costs.coverage = CostCoverage(
        observed_buckets=2, total_buckets=7, first_observed="2026-10-15T00:00:00Z"
    )
    costs_api.get_namespace_costs.return_value = costs

    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[4] == "Data for 2 of 7 buckets, starting 2026-10-15T00:00:00Z"
    assert lines[5].startswith("Total 10.00 USD")


def test_admin_costs_without_data_omits_starting(costs_api):
    costs = NamespaceCosts(
        window="HOUR", coverage=CostCoverage(observed_buckets=0, total_buckets=6)
    )
    costs_api.get_namespace_costs.return_value = costs

    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code == 0, result.output
    assert "Data for 0 of 6 buckets\n" in result.output


def test_admin_costs_omits_coverage_when_complete(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code == 0, result.output
    assert "Data for" not in result.output


@pytest.mark.parametrize(
    ("cpu_no_requests", "ram_no_requests", "shown"),
    [(True, True, "No requests"), (True, False, "37%"), (False, False, "37%")],
)
def test_admin_costs_efficiency_no_requests(
    costs_api, cpu_no_requests, ram_no_requests, shown
):
    costs = _costs()
    costs.items[1].cpu_no_requests = cpu_no_requests
    costs.items[1].ram_no_requests = ram_no_requests
    costs_api.get_namespace_costs.return_value = costs

    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[2].split(maxsplit=12)[-1].strip() == shown


def test_admin_costs_shows_backend_user_message(costs_api):
    response = mock.MagicMock(status_code=503)
    response.json.return_value = {
        "errorCode": 100081,
        "errorMsg": "OpenCost could not be reached.",
        "usrMsg": "No OpenCost certificate at /var/run/secrets/opencost/ca.crt",
    }
    costs_api.get_namespace_costs.side_effect = RestAPIError(
        "admin/costs/namespaces", response
    )

    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code != 0
    assert (
        "Could not get namespace costs: Hopsworks refused the request" in result.output
    )
    assert result.output.count("No OpenCost certificate") == 1, result.output
    assert "Server response" not in result.output


def test_admin_costs_names_the_scopes_a_key_without_admin_lacks(costs_api):
    response = mock.MagicMock(status_code=403)
    response.json.return_value = {
        "errorCode": 320004,
        "errorMsg": "No valid scope found for this invocation",
        "usrMsg": "Endpoint requires one of [ADMIN]",
    }
    costs_api.get_namespace_costs.side_effect = RestAPIError(
        "admin/costs/namespaces", response
    )

    result = CliRunner().invoke(cli, ["admin", "costs"])

    assert result.exit_code != 0
    assert "ADMIN and PROJECT scopes" in result.output
    assert "hops login --api-key" in result.output
    assert "hops setup --force" not in result.output


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


def test_admin_costs_by_department(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs", "--by", "department"])

    assert result.exit_code == 0, result.output
    costs_api.get_department_costs.assert_called_once_with("week")
    costs_api.get_namespace_costs.assert_not_called()
    lines = result.output.splitlines()
    assert lines[0].split()[:3] == ["DEPARTMENT", "NAMESPACES", "CPU"]
    assert "GPU WORKLOADS" in lines[0]
    assert lines[2].split() == [
        "risk",
        "2",
        "7.00",
        "0.00",
        "0.00",
        "0.00",
        "0.00",
        "0.00",
        "7.00",
        "6.00",
        "1.00",
        "70%",
        "50%",
    ]
    assert lines[3].split()[:2] == ["__idle__", "-"]
    assert lines[3].split()[-4:] == ["-", "-", "20%", "-"]
    assert lines[4].startswith("Not a project  3 ")
    assert lines[4].split()[-4:] == ["0.00", "1.00", "10%", "-"]
    assert lines[5].startswith("Total 10.00 USD")


def test_admin_costs_by_department_passes_window(costs_api):
    result = CliRunner().invoke(
        cli, ["admin", "costs", "--by", "DEPARTMENT", "--window", "year"]
    )

    assert result.exit_code == 0, result.output
    costs_api.get_department_costs.assert_called_once_with("year")


def test_admin_costs_rejects_unknown_view(costs_api):
    result = CliRunner().invoke(cli, ["admin", "costs", "--by", "project"])

    assert result.exit_code != 0
    costs_api.get_namespace_costs.assert_not_called()
    costs_api.get_department_costs.assert_not_called()


def test_admin_costs_by_department_reports_backend_failure(costs_api):
    costs_api.get_department_costs.side_effect = RuntimeError("OpenCost is down")

    result = CliRunner().invoke(cli, ["admin", "costs", "--by", "department"])

    assert result.exit_code != 0
    assert "Could not get department costs: OpenCost is down" in result.output
