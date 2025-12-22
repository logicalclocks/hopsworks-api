"""Tests for jobs CLI commands"""

import pytest
import json
from unittest.mock import Mock, patch
from hopsworks_cli.main import cli


class TestJobsCommands:
    """Test suite for jobs commands"""

    def test_jobs_list(self, cli_runner, temp_api_key_file, mock_job_api, mock_job):
        """Test listing jobs"""
        # Setup mock
        mock_job_api.get_jobs.return_value = [mock_job]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "jobs", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_job" in result.output
        mock_job_api.get_jobs.assert_called_once()

    def test_jobs_list_empty(self, cli_runner, temp_api_key_file, mock_job_api):
        """Test listing jobs when none exist"""
        # Setup mock
        mock_job_api.get_jobs.return_value = []

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "No jobs found" in result.output

    def test_jobs_get(self, cli_runner, temp_api_key_file, mock_job_api, mock_job):
        """Test getting a specific job"""
        # Setup mock
        mock_job_api.get_job.return_value = mock_job

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "jobs", "get", "test_job"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_job" in result.output
        mock_job_api.get_job.assert_called_once_with("test_job")

    def test_jobs_get_not_found(self, cli_runner, temp_api_key_file, mock_job_api):
        """Test getting a job that doesn't exist"""
        # Setup mock
        mock_job_api.get_job.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "get", "nonexistent"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_jobs_create(self, cli_runner, temp_api_key_file, mock_job_api, mock_job, tmp_path):
        """Test creating a job from config file"""
        # Create a temporary config file
        config_file = tmp_path / "job_config.json"
        config_data = {
            "type": "PYSPARK",
            "appPath": "/Projects/test/Resources/job.py",
            "defaultArgs": "--arg1 value1"
        }
        config_file.write_text(json.dumps(config_data))

        # Setup mock
        mock_job_api.create_job.return_value = mock_job

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "create", "test_job", "--config-file", str(config_file)
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "created" in result.output.lower() or "updated" in result.output.lower()
        mock_job_api.create_job.assert_called_once()

    def test_jobs_run(self, cli_runner, temp_api_key_file, mock_job_api, mock_job, mock_execution):
        """Test running a job"""
        # Setup mock
        mock_job_api.get_job.return_value = mock_job
        mock_job.run.return_value = mock_execution

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "run", "test_job"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "started" in result.output.lower()
        mock_job.run.assert_called_once()

    def test_jobs_run_with_args(self, cli_runner, temp_api_key_file, mock_job_api, mock_job, mock_execution):
        """Test running a job with arguments"""
        # Setup mock
        mock_job_api.get_job.return_value = mock_job
        mock_job.run.return_value = mock_execution

        # Run command with args
        args_json = '{"arg1": "value1", "arg2": "value2"}'
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "run", "test_job", "--args", args_json
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "started" in result.output.lower()
        mock_job.run.assert_called_once()

    def test_jobs_run_wait(self, cli_runner, temp_api_key_file, mock_job_api, mock_job, mock_execution):
        """Test running a job and waiting for completion"""
        # Setup mock - job completes immediately
        mock_execution.state = "FINISHED"
        mock_job_with_execution = Mock(spec=[])
        mock_job_with_execution.name = "test_job"
        mock_job_with_execution.executions = [mock_execution]
        mock_job_with_execution.run = Mock(return_value=mock_execution)

        mock_job_api.get_job.return_value = mock_job_with_execution

        # Run command with --wait
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "run", "test_job", "--wait", "--timeout", "10"
            ]
        )

        # Verify
        assert result.exit_code == 0

    def test_jobs_run_invalid_args(self, cli_runner, temp_api_key_file, mock_job_api, mock_job):
        """Test running a job with invalid JSON args"""
        # Setup mock
        mock_job_api.get_job.return_value = mock_job

        # Run command with invalid JSON
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "run", "test_job", "--args", "invalid json"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "invalid" in result.output.lower() or "json" in result.output.lower()

    def test_jobs_delete(self, cli_runner, temp_api_key_file, mock_job_api, mock_job):
        """Test deleting a job with --yes flag"""
        # Setup mock
        mock_job_api.get_job.return_value = mock_job

        # Run command with --yes to skip confirmation
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "delete", "test_job", "--yes"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "deleted" in result.output.lower()
        mock_job.delete.assert_called_once()

    def test_jobs_delete_not_found(self, cli_runner, temp_api_key_file, mock_job_api):
        """Test deleting a job that doesn't exist"""
        # Setup mock
        mock_job_api.get_job.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "delete", "nonexistent", "--yes"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_jobs_delete_cancelled(self, cli_runner, temp_api_key_file, mock_job_api, mock_job):
        """Test cancelling job deletion"""
        # Setup mock
        mock_job_api.get_job.return_value = mock_job

        # Run command without --yes and answer 'n' to confirmation
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "jobs", "delete", "test_job"
            ],
            input="n\n"
        )

        # Verify
        assert result.exit_code == 0
        assert "cancelled" in result.output.lower()
        mock_job.delete.assert_not_called()

    def test_jobs_list_with_executions(self, cli_runner, temp_api_key_file, mock_job_api, mock_job, mock_execution):
        """Test listing jobs with execution history"""
        # Setup mock with executions
        mock_job.executions = [mock_execution]
        mock_job_api.get_jobs.return_value = [mock_job]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "jobs", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_job" in result.output
