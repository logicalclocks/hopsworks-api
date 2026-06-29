"""CliRunner tests for the ``hops alert`` command group.

The alert commands wrap ``project.get_alerts_api()``; the shared
``mock_project`` fixture already returns a ``MagicMock`` project, so each test
shapes ``project.get_alerts_api.return_value`` and asserts the CLI both calls
the SDK with the right arguments and renders / exits correctly.
"""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


def _alert(**fields):
    """A fake alert whose ``to_dict`` returns the given fields."""
    a = mock.MagicMock()
    a.to_dict.return_value = fields
    for k, v in fields.items():
        setattr(a, k, v)
    return a


# --- project alerts -------------------------------------------------------


def test_alert_list_renders_rows(mock_project):
    aa = mock_project.get_alerts_api.return_value
    aa.get_alerts.return_value = [
        _alert(
            id=1, status="FAILED", severity="WARNING", receiver="email", service="JOBS"
        )
    ]
    result = CliRunner().invoke(cli, ["alert", "list"])
    assert result.exit_code == 0, result.output
    aa.get_alerts.assert_called_once_with()
    assert "FAILED" in result.output


def test_alert_list_json(mock_project):
    aa = mock_project.get_alerts_api.return_value
    aa.get_alerts.return_value = [_alert(id=7, status="FAILED")]
    result = CliRunner().invoke(cli, ["alert", "list", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [{"id": 7, "status": "FAILED"}]


def test_alert_create_passes_args(mock_project):
    aa = mock_project.get_alerts_api.return_value
    aa.create_project_alert.return_value = _alert(id=3)
    result = CliRunner().invoke(
        cli,
        [
            "alert",
            "create",
            "--receiver",
            "email",
            "--status",
            "job_failed",
            "--severity",
            "warning",
            "--service",
            "Jobs",
        ],
    )
    assert result.exit_code == 0, result.output
    aa.create_project_alert.assert_called_once_with(
        "email", "job_failed", "warning", "Jobs", 0
    )


def test_alert_create_rejects_bad_status(mock_project):
    result = CliRunner().invoke(
        cli,
        [
            "alert",
            "create",
            "--receiver",
            "e",
            "--status",
            "nope",
            "--severity",
            "warning",
            "--service",
            "Jobs",
        ],
    )
    assert result.exit_code != 0
    assert "nope" in result.output


def test_alert_get_not_found(mock_project):
    aa = mock_project.get_alerts_api.return_value
    aa.get_alert.return_value = None
    result = CliRunner().invoke(cli, ["alert", "get", "99"])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_alert_delete_confirms_and_calls_sdk(mock_project):
    aa = mock_project.get_alerts_api.return_value
    result = CliRunner().invoke(cli, ["alert", "delete", "5", "--yes"])
    assert result.exit_code == 0, result.output
    aa.delete_alert.assert_called_once_with(5)


def test_alert_delete_aborts_without_yes(mock_project):
    aa = mock_project.get_alerts_api.return_value
    result = CliRunner().invoke(cli, ["alert", "delete", "5"], input="n\n")
    assert result.exit_code != 0
    aa.delete_alert.assert_not_called()


# --- job alerts (the path the benchmark alerting task needs) --------------


def test_job_alert_create_passes_args(mock_project):
    aa = mock_project.get_alerts_api.return_value
    aa.create_job_alert.return_value = _alert(id=11)
    result = CliRunner().invoke(
        cli,
        [
            "alert",
            "job",
            "create",
            "flaky42",
            "--receiver",
            "email",
            "--status",
            "failed",
            "--severity",
            "critical",
        ],
    )
    assert result.exit_code == 0, result.output
    aa.create_job_alert.assert_called_once_with(
        "flaky42", "email", "failed", "critical"
    )


def test_job_alert_list(mock_project):
    aa = mock_project.get_alerts_api.return_value
    aa.get_job_alerts.return_value = [_alert(id=1, job_name="flaky42", status="FAILED")]
    result = CliRunner().invoke(cli, ["alert", "job", "list", "flaky42"])
    assert result.exit_code == 0, result.output
    aa.get_job_alerts.assert_called_once_with("flaky42")
    assert "flaky42" in result.output


# --- receivers ------------------------------------------------------------


def test_receiver_list(mock_project):
    aa = mock_project.get_alerts_api.return_value
    r = mock.MagicMock()
    r.to_dict.return_value = {"name": "email", "email_configs": [{"to": "a@b.c"}]}
    aa.get_alert_receivers.return_value = [r]
    result = CliRunner().invoke(cli, ["alert", "receiver", "list"])
    assert result.exit_code == 0, result.output
    assert "email" in result.output and "a@b.c" in result.output


def test_receiver_create_email(mock_project):
    aa = mock_project.get_alerts_api.return_value
    created = mock.MagicMock()
    created.name = "ops"
    created.to_dict.return_value = {"name": "ops"}
    aa.create_alert_receiver.return_value = created
    result = CliRunner().invoke(
        cli, ["alert", "receiver", "create", "ops", "--email", "a@b.c"]
    )
    assert result.exit_code == 0, result.output
    _, kwargs = aa.create_alert_receiver.call_args
    assert kwargs["name"] == "ops"
    assert kwargs["email_configs"] == [{"to": "a@b.c", "send_resolved": False}]
    assert kwargs["slack_configs"] is None


def test_receiver_create_bad_pagerduty(mock_project):
    result = CliRunner().invoke(
        cli, ["alert", "receiver", "create", "ops", "--pagerduty", "no-colon"]
    )
    assert result.exit_code != 0
    assert "service_key:routing_key" in result.output


# --- feature group alerts (name resolution) -------------------------------


def test_fg_alert_create_resolves_ids(mock_project):
    fs = mock_project.get_feature_store.return_value  # id 67 from conftest
    fg = mock.MagicMock()
    fg.id = 5
    fs.get_feature_group.return_value = fg
    aa = mock_project.get_alerts_api.return_value
    aa.create_feature_group_alert.return_value = _alert(id=9)
    result = CliRunner().invoke(
        cli,
        [
            "alert",
            "fg",
            "create",
            "txn",
            "--receiver",
            "email",
            "--status",
            "feature_validation_warning",
            "--severity",
            "warning",
        ],
    )
    assert result.exit_code == 0, result.output
    aa.create_feature_group_alert.assert_called_once_with(
        67, 5, "email", "feature_validation_warning", "warning"
    )
