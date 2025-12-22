"""Tests for projects CLI commands"""

import pytest
from unittest.mock import Mock, patch
from hopsworks_cli.main import cli


class TestProjectsCommands:
    """Test suite for projects commands"""

    def test_projects_list(self, cli_runner, temp_api_key_file, mock_connection, mock_project):
        """Test listing projects"""
        # Setup mock
        mock_connection.get_projects.return_value = [mock_project]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--output", "json",
                "projects", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_project" in result.output
        mock_connection.get_projects.assert_called_once()

    def test_projects_list_empty(self, cli_runner, temp_api_key_file, mock_connection):
        """Test listing projects when none exist"""
        # Setup mock
        mock_connection.get_projects.return_value = []

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "projects", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "No projects found" in result.output

    def test_projects_get(self, cli_runner, temp_api_key_file, mock_connection, mock_project):
        """Test getting a specific project"""
        # Setup mock
        mock_connection.get_project.return_value = mock_project

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--output", "json",
                "projects", "get", "test_project"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_project" in result.output
        assert "123" in result.output  # project ID
        mock_connection.get_project.assert_called_once_with("test_project")

    def test_projects_get_not_found(self, cli_runner, temp_api_key_file, mock_connection):
        """Test getting a project that doesn't exist"""
        # Setup mock
        mock_connection.get_project.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "projects", "get", "nonexistent"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    @patch('hopsworks.create_project')
    def test_projects_create(self, mock_create_project, cli_runner, temp_api_key_file, mock_connection, mock_project):
        """Test creating a new project"""
        # Setup mock
        mock_create_project.return_value = mock_project

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "projects", "create", "new_project", "--description", "A new project"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "created" in result.output.lower()
        mock_create_project.assert_called_once_with("new_project", description="A new project")

    @patch('hopsworks.create_project')
    def test_projects_create_without_description(self, mock_create_project, cli_runner, temp_api_key_file, mock_connection, mock_project):
        """Test creating a project without description"""
        # Setup mock
        mock_create_project.return_value = mock_project

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "projects", "create", "new_project"
            ]
        )

        # Verify
        assert result.exit_code == 0
        mock_create_project.assert_called_once_with("new_project", description=None)

    def test_projects_use(self, cli_runner, temp_api_key_file, mock_connection, mock_project, tmp_path):
        """Test setting a project as default"""
        # Setup mock
        mock_connection.get_project.return_value = mock_project

        # Create a temporary config directory
        config_dir = tmp_path / ".hopsworks"
        config_dir.mkdir()

        # Mock the CONFIG_DIR
        with patch('hopsworks_cli.config.CONFIG_DIR', config_dir):
            # Run command
            result = cli_runner.invoke(
                cli,
                [
                    "--host", "test.hopsworks.ai",
                    "--api-key-file", temp_api_key_file,
                    "projects", "use", "test_project"
                ]
            )

            # Verify
            assert result.exit_code == 0
            assert "default project" in result.output.lower()

    def test_projects_use_with_profile(self, cli_runner, temp_api_key_file, mock_connection, mock_project, tmp_path):
        """Test setting a project as default for a specific profile"""
        # Setup mock
        mock_connection.get_project.return_value = mock_project

        # Create a temporary config directory
        config_dir = tmp_path / ".hopsworks"
        config_dir.mkdir()

        # Mock the CONFIG_DIR
        with patch('hopsworks_cli.config.CONFIG_DIR', config_dir):
            # Run command
            result = cli_runner.invoke(
                cli,
                [
                    "--host", "test.hopsworks.ai",
                    "--api-key-file", temp_api_key_file,
                    "projects", "use", "test_project", "--profile", "dev"
                ]
            )

            # Verify
            assert result.exit_code == 0
            assert "default project" in result.output.lower()
            assert "dev" in result.output

    def test_projects_use_not_found(self, cli_runner, temp_api_key_file, mock_connection, tmp_path):
        """Test using a project that doesn't exist"""
        # Setup mock
        mock_connection.get_project.return_value = None

        # Create a temporary config directory
        config_dir = tmp_path / ".hopsworks"
        config_dir.mkdir()

        # Mock the CONFIG_DIR
        with patch('hopsworks_cli.config.CONFIG_DIR', config_dir):
            # Run command
            result = cli_runner.invoke(
                cli,
                [
                    "--host", "test.hopsworks.ai",
                    "--api-key-file", temp_api_key_file,
                    "projects", "use", "nonexistent"
                ]
            )

            # Verify
            assert result.exit_code == 1
            assert "not found" in result.output.lower()

    def test_projects_list_json_output(self, cli_runner, temp_api_key_file, mock_connection, mock_project):
        """Test listing projects with JSON output"""
        # Create multiple projects
        project2 = Mock(spec=[])
        project2.name = "project2"
        project2.id = 124
        project2.owner = "user2"
        project2.created = "2024-01-02T00:00:00"
        project2.description = "Second project"

        mock_connection.get_projects.return_value = [mock_project, project2]

        # Run command with JSON output
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--output", "json",
                "projects", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_project" in result.output
        assert "project2" in result.output
