"""Tests for deployments CLI commands"""

import pytest
from unittest.mock import Mock, patch
from hopsworks_cli.main import cli


class TestDeploymentsCommands:
    """Test suite for deployments commands"""

    def test_deployments_list(self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment):
        """Test listing deployments"""
        # Setup mock
        mock_model_serving.get_deployments.return_value = [mock_deployment]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "deployments", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_deployment" in result.output
        mock_model_serving.get_deployments.assert_called_once()

    def test_deployments_list_empty(self, cli_runner, temp_api_key_file, mock_model_serving):
        """Test listing deployments when none exist"""
        # Setup mock
        mock_model_serving.get_deployments.return_value = []

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "deployments", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "No deployments found" in result.output

    def test_deployments_get(self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment):
        """Test getting a specific deployment"""
        # Setup mock
        mock_model_serving.get_deployment.return_value = mock_deployment

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "deployments", "get", "test_deployment"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_deployment" in result.output
        mock_model_serving.get_deployment.assert_called_once_with("test_deployment")

    def test_deployments_get_not_found(self, cli_runner, temp_api_key_file, mock_model_serving):
        """Test getting a deployment that doesn't exist"""
        # Setup mock
        mock_model_serving.get_deployment.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "deployments", "get", "nonexistent"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_deployments_start(self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment):
        """Test starting a deployment"""
        # Setup mock
        mock_deployment.state = "Stopped"
        mock_model_serving.get_deployment.return_value = mock_deployment

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "deployments", "start", "test_deployment"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "start" in result.output.lower()
        mock_deployment.start.assert_called_once()

    def test_deployments_start_already_running(
        self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment
    ):
        """Test starting a deployment that is already running"""
        # Setup mock
        mock_deployment.state = "Running"
        mock_model_serving.get_deployment.return_value = mock_deployment

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "deployments", "start", "test_deployment"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "already running" in result.output.lower()
        mock_deployment.start.assert_not_called()

    def test_deployments_stop(self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment):
        """Test stopping a deployment"""
        # Setup mock
        mock_deployment.state = "Running"
        mock_model_serving.get_deployment.return_value = mock_deployment

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "deployments", "stop", "test_deployment"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "stop" in result.output.lower()
        mock_deployment.stop.assert_called_once()

    def test_deployments_stop_already_stopped(
        self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment
    ):
        """Test stopping a deployment that is already stopped"""
        # Setup mock
        mock_deployment.state = "Stopped"
        mock_model_serving.get_deployment.return_value = mock_deployment

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "deployments", "stop", "test_deployment"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "already stopped" in result.output.lower()
        mock_deployment.stop.assert_not_called()

    def test_deployments_status(self, cli_runner, temp_api_key_file, mock_model_serving, mock_deployment):
        """Test checking deployment status"""
        # Setup mock
        mock_model_serving.get_deployment.return_value = mock_deployment

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "deployments", "status", "test_deployment"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_deployment" in result.output
        assert "Running" in result.output
