"""Pytest fixtures for CLI tests"""

import pytest
from unittest.mock import Mock, MagicMock
from click.testing import CliRunner


@pytest.fixture
def cli_runner():
    """Provide Click CLI test runner"""
    return CliRunner()


@pytest.fixture
def mock_connection(mocker):
    """Mock Hopsworks connection"""
    connection = Mock()
    mocker.patch("hopsworks.login", return_value=connection)
    # Also mock client.get_connection() to return the same connection
    mocker.patch("hopsworks_common.client.get_connection", return_value=connection)
    return connection


@pytest.fixture
def mock_model_registry(mocker, mock_connection):
    """Mock model registry"""
    mr = Mock()
    mock_connection.get_model_registry.return_value = mr
    return mr


@pytest.fixture
def mock_model_serving(mocker, mock_connection):
    """Mock model serving"""
    ms = Mock()
    mock_connection.get_model_serving.return_value = ms
    return ms


@pytest.fixture
def mock_model():
    """Create a mock model object"""
    model = Mock(spec=[])
    model.name = "test_model"
    model.version = 1
    model.framework = "tensorflow"
    model.created = "2024-01-01T00:00:00"
    model.training_metrics = {"accuracy": 0.95, "loss": 0.05}
    model.description = "Test model description"
    model.model_schema = {"input": "test_input"}
    model.input_example = {"feature1": 1.0}
    model.output_example = None
    model.program = None
    model.environment = None
    model.tags = {}
    # Mock the download method
    model.download = Mock(return_value="/tmp/model")
    return model


@pytest.fixture
def mock_deployment():
    """Create a mock deployment object"""
    deployment = Mock(spec=[])
    deployment.name = "test_deployment"
    deployment.model_name = "test_model"
    deployment.model_version = 1
    deployment.state = "Running"
    deployment.created = "2024-01-01T00:00:00"

    # Mock predictor
    predictor = Mock(spec=[])
    predictor.requested_instances = 1
    predictor.script_file = "predictor.py"
    predictor.state = "Running"
    predictor.available_instances = 1
    predictor.resources = Mock(spec=[])
    predictor.resources.cores = 1
    predictor.resources.memory = 1024
    predictor.resources.gpus = 0
    deployment.predictor = predictor

    # Mock transformer (optional)
    deployment.transformer = None
    deployment.inference_logger = None

    # Mock start/stop methods
    deployment.start = Mock()
    deployment.stop = Mock()

    return deployment


@pytest.fixture
def temp_api_key_file(tmp_path):
    """Create a temporary API key file for testing"""
    api_key_file = tmp_path / "test_api_key"
    api_key_file.write_text("test_api_key_value")
    return str(api_key_file)


@pytest.fixture
def mock_job_api(mocker, mock_connection):
    """Mock job API"""
    job_api = Mock()
    mock_connection.get_job_api.return_value = job_api
    return job_api


@pytest.fixture
def mock_secrets_api(mocker, mock_connection):
    """Mock secrets API"""
    secrets_api = Mock()
    # Mock both the connection method and the hopsworks module function
    mock_connection.get_secrets_api.return_value = secrets_api
    mocker.patch("hopsworks.get_secrets_api", return_value=secrets_api)
    return secrets_api


@pytest.fixture
def mock_job():
    """Create a mock job object"""
    job = Mock(spec=[])
    job.name = "test_job"
    job.job_type = "PYSPARK"
    job.created = "2024-01-01T00:00:00"
    job.creator = "test_user"

    # Mock config
    config = Mock(spec=[])
    config.app_path = "/Projects/test_project/Resources/job.py"
    config.main_class = None
    job.config = config

    # Mock schedule
    schedule = Mock(spec=[])
    schedule.enabled = False
    schedule.cron_expression = "0 0 * * *"
    job.job_schedule = schedule

    # Mock executions
    job.executions = []

    # Mock methods
    job.run = Mock()
    job.delete = Mock()

    return job


@pytest.fixture
def mock_execution():
    """Create a mock job execution object"""
    execution = Mock(spec=[])
    execution.id = 123
    execution.state = "FINISHED"
    execution.submission_time = "2024-01-01T00:00:00"
    execution.final_status = "SUCCEEDED"
    return execution


@pytest.fixture
def mock_secret():
    """Create a mock secret object"""
    secret = Mock(spec=[])
    secret.name = "test_secret"
    secret.owner = "test_user"
    secret.scope = "PROJECT"
    secret.visibility = "PRIVATE"
    return secret


@pytest.fixture
def mock_project():
    """Create a mock project object"""
    project = Mock(spec=[])
    project.name = "test_project"
    project.id = 123
    project.owner = "test_user"
    project.created = "2024-01-01T00:00:00"
    project.description = "Test project description"
    project.retention_period = None
    project.payment_tier = "FREE"

    # Mock methods
    project.get_feature_store = Mock()

    return project


@pytest.fixture
def mock_feature_store(mocker, mock_connection):
    """Mock feature store"""
    fs = Mock()
    mock_connection.get_feature_store.return_value = fs
    return fs


@pytest.fixture
def mock_feature_group():
    """Create a mock feature group object"""
    fg = Mock(spec=[])
    fg.name = "test_feature_group"
    fg.version = 1
    fg.id = 456
    fg.featurestore_id = 789
    fg.online_enabled = True
    fg.deprecated = False
    fg.location = "test_location"
    fg.event_time = "timestamp"
    fg.primary_key = ["id"]
    fg.description = "Test feature group"

    # Mock features (schema)
    feature1 = Mock(spec=[])
    feature1.name = "id"
    feature1.type = "bigint"
    feature1.description = "Primary key"

    feature2 = Mock(spec=[])
    feature2.name = "timestamp"
    feature2.type = "timestamp"
    feature2.description = "Event time"

    feature3 = Mock(spec=[])
    feature3.name = "value"
    feature3.type = "double"
    feature3.description = "Feature value"

    fg.features = [feature1, feature2, feature3]

    # Mock statistics config
    stats_config = Mock(spec=[])
    stats_config.enabled = True
    fg.statistics_config = stats_config

    return fg


@pytest.fixture
def cli_context(temp_api_key_file):
    """Create a mock CLI context"""
    ctx = {
        "host": "test.hopsworks.ai",
        "port": 443,
        "project": "test_project",
        "api_key_file": temp_api_key_file,
        "output_format": "json",
        "verbose": False,
        "hostname_verification": False,
    }
    return ctx
