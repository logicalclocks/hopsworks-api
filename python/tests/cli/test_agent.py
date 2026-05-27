"""CliRunner tests for ``hops agent`` commands.

Drive every verb through the shared ``mock_project`` fixture so no test hits
real Hopsworks. The fake ``ModelServing`` returns ``Deployment``-shaped mocks
that mimic the ``has_model`` / ``model_name`` shape ``_is_agent`` relies on.
"""

from __future__ import annotations

from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


def _agent_mock(name: str = "my-agent", deployment_id: int = 7) -> mock.MagicMock:
    """Build a Deployment-shaped mock that ``_is_agent`` classifies as agent."""
    d = mock.MagicMock()
    d.id = deployment_id
    d.name = name
    d.has_model = False
    d.model_name = None
    d.serving_tool = "DEFAULT"
    d.model_server = "PYTHON"
    d.api_protocol = "REST"
    d.description = ""
    predictor = mock.MagicMock()
    predictor.environment = "agent-env"
    predictor.script_file = "/Projects/demo/Resources/agents/my-agent/agent.py"
    d.predictor = predictor
    return d


def _model_mock(name: str = "fraud") -> mock.MagicMock:
    d = mock.MagicMock()
    d.id = 99
    d.name = name
    d.has_model = True
    d.model_name = name
    return d


def test_agent_help_lists_subcommands():
    result = CliRunner().invoke(cli, ["agent", "--help"])
    assert result.exit_code == 0, result.output
    for verb in ("list", "info", "create", "start", "stop", "query", "logs", "delete"):
        assert verb in result.output


def test_agent_list_filters_out_model_deployments(mock_project):
    ms = mock.MagicMock()
    ms.get_deployments.return_value = [_agent_mock("my-agent"), _model_mock("fraud")]
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "list"])
    assert result.exit_code == 0, result.output
    assert "my-agent" in result.output
    assert "fraud" not in result.output


def test_agent_info_unknown_name_errors(mock_project):
    ms = mock.MagicMock()
    ms.get_deployment.return_value = None
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "info", "missing"])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_agent_info_rejects_model_deployment(mock_project):
    ms = mock.MagicMock()
    ms.get_deployment.return_value = _model_mock("fraud")
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "info", "fraud"])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_agent_start_calls_sdk(mock_project):
    ms = mock.MagicMock()
    agent = _agent_mock("my-agent")
    ms.get_deployment.return_value = agent
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "start", "my-agent", "--wait", "30"])
    assert result.exit_code == 0, result.output
    agent.start.assert_called_with(await_running=30)


def test_agent_stop_calls_sdk(mock_project):
    ms = mock.MagicMock()
    agent = _agent_mock("my-agent")
    ms.get_deployment.return_value = agent
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "stop", "my-agent", "--wait", "45"])
    assert result.exit_code == 0, result.output
    agent.stop.assert_called_with(await_stopped=45)


def test_agent_query_parses_json_data(mock_project):
    ms = mock.MagicMock()
    agent = _agent_mock("my-agent")
    agent.predict.return_value = {"answer": 42}
    ms.get_deployment.return_value = agent
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(
        cli,
        ["agent", "query", "my-agent", "--data", '{"prompt": "hello"}'],
    )
    assert result.exit_code == 0, result.output
    agent.predict.assert_called_with(data={"prompt": "hello"})
    assert "42" in result.output


def test_agent_query_requires_data_or_file(mock_project):
    ms = mock.MagicMock()
    ms.get_deployment.return_value = _agent_mock("my-agent")
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "query", "my-agent"])
    assert result.exit_code != 0
    assert "Provide --data or --file" in result.output


def test_agent_logs_one_shot(mock_project):
    ms = mock.MagicMock()
    agent = _agent_mock("my-agent")
    agent.read_logs.return_value = "line1\nline2"
    ms.get_deployment.return_value = agent
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "logs", "my-agent", "--tail", "5"])
    assert result.exit_code == 0, result.output
    agent.read_logs.assert_called_with(
        component="predictor", tail=5, source="opensearch", since=None, until=None
    )
    assert "line1" in result.output


def test_agent_delete(mock_project):
    ms = mock.MagicMock()
    agent = _agent_mock("my-agent")
    ms.get_deployment.return_value = agent
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "delete", "my-agent", "--yes"])
    assert result.exit_code == 0, result.output
    agent.delete.assert_called_with(force=False)


def test_agent_create_invokes_deploy_agent(mock_project, tmp_path):
    ms = mock.MagicMock()
    created = _agent_mock("entry")
    ms.deploy_agent.return_value = created
    mock_project.get_model_serving.return_value = ms

    entry = tmp_path / "entry.py"
    entry.write_text("def predict(x):\n    return x\n")

    result = CliRunner().invoke(
        cli,
        [
            "agent",
            "create",
            str(entry),
            "--name",
            "my-agent",
            "--description",
            "demo",
        ],
    )
    assert result.exit_code == 0, result.output
    ms.deploy_agent.assert_called_once()
    kwargs = ms.deploy_agent.call_args.kwargs
    assert kwargs["entry"] == str(entry)
    assert kwargs["name"] == "my-agent"
    assert kwargs["description"] == "demo"


def test_agent_list_falls_back_to_model_name_heuristic(mock_project):
    """Non-SDK deployment objects without ``has_model`` still classify correctly."""
    legacy_agent = mock.MagicMock(spec=["id", "name", "model_name", "predictor"])
    legacy_agent.id = 1
    legacy_agent.name = "legacy-agent"
    legacy_agent.model_name = None
    legacy_agent.predictor = mock.MagicMock(
        environment="agent-env", script_file="/x/y.py"
    )

    legacy_model = mock.MagicMock(spec=["id", "name", "model_name", "predictor"])
    legacy_model.id = 2
    legacy_model.name = "legacy-model"
    legacy_model.model_name = "fraud"
    legacy_model.predictor = mock.MagicMock(environment="env", script_file="-")

    ms = mock.MagicMock()
    ms.get_deployments.return_value = [legacy_agent, legacy_model]
    mock_project.get_model_serving.return_value = ms

    result = CliRunner().invoke(cli, ["agent", "list"])
    assert result.exit_code == 0, result.output
    assert "legacy-agent" in result.output
    assert "legacy-model" not in result.output
