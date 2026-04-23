"""CliRunner tests for Phase 3: REST escape-hatch + chart/dashboard/superset.

Every command is driven through the shared ``mock_project`` fixture. We also
stub out ``hopsworks_common.client.get_instance`` where commands reach through
it for raw REST calls (connector create/delete).
"""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


# --- deployment ------------------------------------------------------------


def test_deployment_start_calls_sdk(mock_project):
    ms = mock.MagicMock()
    deployment = mock.MagicMock()
    ms.get_deployment.return_value = deployment
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "start", "fraud"])
    assert result.exit_code == 0, result.output
    deployment.start.assert_called_with(await_running=600)


def test_deployment_stop_calls_sdk(mock_project):
    ms = mock.MagicMock()
    deployment = mock.MagicMock()
    ms.get_deployment.return_value = deployment
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "stop", "fraud", "--wait", "30"])
    assert result.exit_code == 0, result.output
    deployment.stop.assert_called_with(await_stopped=30)


def test_deployment_predict_parses_json_data(mock_project):
    ms = mock.MagicMock()
    deployment = mock.MagicMock()
    deployment.predict.return_value = {"predictions": [0.9]}
    ms.get_deployment.return_value = deployment
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(
        cli, ["deployment", "predict", "fraud", "--data", '{"instances": [[1,2]]}']
    )
    assert result.exit_code == 0, result.output
    deployment.predict.assert_called_with(data={"instances": [[1, 2]]})


def test_deployment_logs(mock_project):
    ms = mock.MagicMock()
    deployment = mock.MagicMock()
    deployment.get_logs.return_value = "line1\nline2"
    ms.get_deployment.return_value = deployment
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "logs", "fraud", "--tail", "5"])
    assert result.exit_code == 0, result.output
    deployment.get_logs.assert_called_with(component="predictor", tail=5)
    assert "line1" in result.output


def test_deployment_delete(mock_project):
    ms = mock.MagicMock()
    deployment = mock.MagicMock()
    ms.get_deployment.return_value = deployment
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "delete", "fraud", "--yes"])
    assert result.exit_code == 0, result.output
    deployment.delete.assert_called_with(force=False)


def test_deployment_create_via_model_deploy(mock_project):
    mr = mock.MagicMock()
    model = mock.MagicMock()
    model.version = 1
    created = mock.MagicMock()
    created.name = "fraud"
    model.deploy.return_value = created
    mr.get_models.return_value = [model]
    mock_project.get_model_registry.return_value = mr
    result = CliRunner().invoke(
        cli, ["deployment", "create", "fraud", "--name", "fraud"]
    )
    assert result.exit_code == 0, result.output
    model.deploy.assert_called_once()


# --- job -------------------------------------------------------------------


def test_job_run_with_wait(mock_project):
    api = mock.MagicMock()
    job = mock.MagicMock()
    execution = mock.MagicMock()
    execution.id, execution.state = 5, "RUNNING"
    job.run.return_value = execution
    api.get_job.return_value = job
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "run", "etl", "--wait"])
    assert result.exit_code == 0, result.output
    job.run.assert_called_with(args=None, await_termination=True)


def test_job_schedule(mock_project):
    api = mock.MagicMock()
    job = mock.MagicMock()
    job.schedule.return_value = mock.MagicMock()
    api.get_job.return_value = job
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "schedule", "etl", "0 0 * * * ?"])
    assert result.exit_code == 0, result.output
    job.schedule.assert_called_with(
        cron_expression="0 0 * * * ?", start_time=None, end_time=None
    )


def test_job_unschedule(mock_project):
    api = mock.MagicMock()
    job = mock.MagicMock()
    api.get_job.return_value = job
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "unschedule", "etl"])
    assert result.exit_code == 0, result.output
    job.unschedule.assert_called_once()


def test_job_history(mock_project):
    api = mock.MagicMock()
    job = mock.MagicMock()
    e1 = mock.MagicMock()
    e1.id, e1.state, e1.final_status = 1, "FINISHED", "SUCCEEDED"
    e1.submission_time = "2026-04-01"
    e2 = mock.MagicMock()
    e2.id, e2.state, e2.final_status = 2, "RUNNING", "-"
    e2.submission_time = "2026-04-02"
    job.get_executions.return_value = [e1, e2]
    api.get_job.return_value = job
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "history", "etl"])
    assert result.exit_code == 0, result.output
    # Most recent first
    lines = [ln for ln in result.output.splitlines() if ln.startswith(("1", "2"))]
    assert lines and lines[0].startswith("2")


def test_job_logs(mock_project):
    api = mock.MagicMock()
    job = mock.MagicMock()
    execution = mock.MagicMock()
    execution.download_logs.return_value = ("/tmp/out.log", "/tmp/err.log")
    job.get_executions.return_value = [execution]
    api.get_job.return_value = job
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "logs", "etl"])
    assert result.exit_code == 0, result.output
    execution.download_logs.assert_called_once()


# --- dataset ---------------------------------------------------------------


def test_dataset_mkdir(mock_project):
    api = mock.MagicMock()
    api.mkdir.return_value = "/Projects/demo/newdir"
    mock_project.get_dataset_api.return_value = api
    result = CliRunner().invoke(cli, ["dataset", "mkdir", "/Projects/demo/newdir"])
    assert result.exit_code == 0, result.output
    api.mkdir.assert_called_with("/Projects/demo/newdir")


def test_dataset_remove(mock_project):
    api = mock.MagicMock()
    mock_project.get_dataset_api.return_value = api
    result = CliRunner().invoke(
        cli, ["dataset", "remove", "/Projects/demo/stale", "--yes"]
    )
    assert result.exit_code == 0, result.output
    api.remove.assert_called_with("/Projects/demo/stale")


# --- connector writes -----------------------------------------------------


def test_connector_create_jdbc_posts_body(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.id = 67
    fake_client = mock.MagicMock()
    fake_client._project_id = 119
    with mock.patch("hopsworks_common.client.get_instance", return_value=fake_client):
        result = CliRunner().invoke(
            cli,
            [
                "connector",
                "create",
                "jdbc",
                "mydb",
                "--url",
                "jdbc:postgresql://host/db",
                "--user",
                "u",
                "--password",
                "p",
            ],
        )
    assert result.exit_code == 0, result.output
    call = fake_client._send_request.call_args
    assert call.args[0] == "POST"
    body = json.loads(call.kwargs["data"])
    assert body["storageConnectorType"] == "JDBC"
    assert body["connectionString"] == "jdbc:postgresql://host/db"
    assert {"name": "user", "value": "u"} in body["arguments"]


def test_connector_delete_calls_rest(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.id = 67
    fake_client = mock.MagicMock()
    fake_client._project_id = 119
    with mock.patch("hopsworks_common.client.get_instance", return_value=fake_client):
        result = CliRunner().invoke(cli, ["connector", "delete", "mydb", "--yes"])
    assert result.exit_code == 0, result.output
    call = fake_client._send_request.call_args
    assert call.args[0] == "DELETE"
    assert "mydb" in call.args[1]


def test_connector_databases_delegates_to_sdk(mock_project):
    fs = mock_project.get_feature_store.return_value
    ds = mock.MagicMock()
    ds.get_databases.return_value = ["db1", "db2"]
    fs.get_data_source.return_value = ds
    result = CliRunner().invoke(cli, ["connector", "databases", "mydb"])
    assert result.exit_code == 0, result.output
    assert "db1" in result.output


# --- chart -----------------------------------------------------------------


def test_chart_list(mock_project):
    api = mock.MagicMock()
    api.list_charts.return_value = [
        {"id": 1, "title": "Revenue", "description": "", "url": "/charts/r.html"}
    ]
    mock_project.get_chart_api.return_value = api
    result = CliRunner().invoke(cli, ["chart", "list"])
    assert result.exit_code == 0, result.output
    assert "Revenue" in result.output


def test_chart_create(mock_project):
    api = mock.MagicMock()
    api.create_chart.return_value = {"id": 1, "title": "Revenue"}
    mock_project.get_chart_api.return_value = api
    result = CliRunner().invoke(
        cli, ["chart", "create", "Revenue", "--url", "/charts/r.html"]
    )
    assert result.exit_code == 0, result.output
    api.create_chart.assert_called_once()


def test_chart_delete(mock_project):
    api = mock.MagicMock()
    mock_project.get_chart_api.return_value = api
    result = CliRunner().invoke(cli, ["chart", "delete", "1", "--yes"])
    assert result.exit_code == 0, result.output
    api.delete_chart.assert_called_with(1)


def test_chart_update_requires_field(mock_project):
    api = mock.MagicMock()
    mock_project.get_chart_api.return_value = api
    result = CliRunner().invoke(cli, ["chart", "update", "1"])
    assert result.exit_code != 0


# --- dashboard -------------------------------------------------------------


def test_dashboard_list(mock_project):
    api = mock.MagicMock()
    api.list_dashboards.return_value = [{"id": 1, "name": "Ops", "charts": [{"id": 5}]}]
    mock_project.get_dashboard_api.return_value = api
    result = CliRunner().invoke(cli, ["dashboard", "list"])
    assert result.exit_code == 0, result.output
    assert "Ops" in result.output


def test_dashboard_add_chart_delegates(mock_project):
    api = mock.MagicMock()
    mock_project.get_dashboard_api.return_value = api
    result = CliRunner().invoke(cli, ["dashboard", "add-chart", "1", "--chart", "5"])
    assert result.exit_code == 0, result.output
    api.add_chart.assert_called_with(1, 5)


# --- superset --------------------------------------------------------------


def test_superset_dashboard_list(mock_project):
    api = mock.MagicMock()
    api.list_dashboards.return_value = {
        "result": [
            {"id": 1, "dashboard_title": "Ops", "published": True, "slug": "ops"}
        ],
        "count": 1,
    }
    mock_project.get_superset_api.return_value = api
    result = CliRunner().invoke(cli, ["superset", "dashboard", "list"])
    assert result.exit_code == 0, result.output
    assert "Ops" in result.output


def test_superset_dashboard_create(mock_project):
    api = mock.MagicMock()
    api.create_dashboard.return_value = {"id": 3, "dashboard_title": "My Dash"}
    mock_project.get_superset_api.return_value = api
    result = CliRunner().invoke(
        cli, ["superset", "dashboard", "create", "My Dash", "--published"]
    )
    assert result.exit_code == 0, result.output
    api.create_dashboard.assert_called_with(
        dashboard_title="My Dash", published=True, slug=None
    )


def test_superset_dataset_list(mock_project):
    api = mock.MagicMock()
    api.list_datasets.return_value = {
        "result": [{"id": 1, "table_name": "txn", "schema": "public", "database": {}}],
    }
    mock_project.get_superset_api.return_value = api
    result = CliRunner().invoke(cli, ["superset", "dataset", "list"])
    assert result.exit_code == 0, result.output
    assert "txn" in result.output


def test_superset_chart_create(mock_project):
    api = mock.MagicMock()
    api.create_chart.return_value = {"id": 7, "slice_name": "Bar"}
    mock_project.get_superset_api.return_value = api
    result = CliRunner().invoke(
        cli,
        [
            "superset",
            "chart",
            "create",
            "--name",
            "Bar",
            "--viz-type",
            "bar",
            "--datasource-id",
            "1",
            "--params",
            "{}",
        ],
    )
    assert result.exit_code == 0, result.output
    api.create_chart.assert_called_once()


def test_superset_chart_delete(mock_project):
    api = mock.MagicMock()
    mock_project.get_superset_api.return_value = api
    result = CliRunner().invoke(cli, ["superset", "chart", "delete", "7", "--yes"])
    assert result.exit_code == 0, result.output
    api.delete_chart.assert_called_with(7)
