"""Tests for secrets CLI commands"""

import pytest
from unittest.mock import Mock, patch
from hopsworks_cli.main import cli


class TestSecretsCommands:
    """Test suite for secrets commands"""

    def test_secrets_list(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test listing secrets"""
        # Setup mock
        mock_secrets_api.get_secrets.return_value = [mock_secret]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "secrets", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_secret" in result.output
        # Should not show values in list
        mock_secrets_api.get_secrets.assert_called_once()

    def test_secrets_list_empty(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test listing secrets when none exist"""
        # Setup mock
        mock_secrets_api.get_secrets.return_value = []

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "No secrets found" in result.output

    def test_secrets_get_metadata(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test getting secret metadata (without value)"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = mock_secret

        # Run command without --show-value
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "secrets", "get", "test_secret"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_secret" in result.output
        mock_secrets_api.get_secret.assert_called_once_with("test_secret", owner=None)

    def test_secrets_get_with_value(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test getting secret with --show-value flag"""
        # Setup mock
        mock_secrets_api.get.return_value = "super_secret_value"

        # Run command with --show-value
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "get", "test_secret", "--show-value"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "super_secret_value" in result.output
        # Should show warning about displaying secret
        assert "⚠️" in result.output or "caution" in result.output.lower()
        mock_secrets_api.get.assert_called_once_with("test_secret", owner=None)

    def test_secrets_get_with_owner(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test getting secret with owner specified"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = mock_secret

        # Run command with owner
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "get", "test_secret", "--owner", "other_user"
            ]
        )

        # Verify
        assert result.exit_code == 0
        mock_secrets_api.get_secret.assert_called_once_with("test_secret", owner="other_user")

    def test_secrets_get_not_found(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test getting a secret that doesn't exist"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "get", "nonexistent"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_secrets_create_with_value(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test creating a secret with --value"""
        # Setup mock
        mock_secrets_api.create_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "create", "new_secret", "--value", "secret_value"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "created" in result.output.lower()
        mock_secrets_api.create_secret.assert_called_once_with("new_secret", "secret_value", project=None)

    def test_secrets_create_with_value_file(self, cli_runner, temp_api_key_file, mock_secrets_api, tmp_path):
        """Test creating a secret with --value-file"""
        # Create a temporary value file
        value_file = tmp_path / "secret_value.txt"
        value_file.write_text("secret_from_file")

        # Setup mock
        mock_secrets_api.create_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "create", "new_secret", "--value-file", str(value_file)
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "created" in result.output.lower()
        mock_secrets_api.create_secret.assert_called_once_with("new_secret", "secret_from_file", project=None)

    def test_secrets_create_with_project_scope(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test creating a secret with project scope"""
        # Setup mock
        mock_secrets_api.create_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "create", "new_secret", "--value", "secret_value", "--project", "my_project"
            ]
        )

        # Verify
        assert result.exit_code == 0
        mock_secrets_api.create_secret.assert_called_once_with("new_secret", "secret_value", project="my_project")

    def test_secrets_create_no_value(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test creating a secret without providing value"""
        # Run command without value or value-file
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "create", "new_secret"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "must be provided" in result.output.lower()

    def test_secrets_create_both_value_and_file(self, cli_runner, temp_api_key_file, mock_secrets_api, tmp_path):
        """Test creating a secret with both --value and --value-file (should fail)"""
        # Create a temporary value file
        value_file = tmp_path / "secret_value.txt"
        value_file.write_text("secret_from_file")

        # Run command with both value and value-file
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "create", "new_secret", "--value", "value1", "--value-file", str(value_file)
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "cannot" in result.output.lower() and "both" in result.output.lower()

    def test_secrets_update(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test updating a secret"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = mock_secret
        mock_secrets_api.delete.return_value = None
        mock_secrets_api.create_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "update", "test_secret", "--value", "new_value"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "updated" in result.output.lower()
        # Update works by delete + create
        mock_secrets_api.delete.assert_called_once_with("test_secret")
        mock_secrets_api.create_secret.assert_called_once_with("test_secret", "new_value")

    def test_secrets_update_not_found(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test updating a secret that doesn't exist"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "update", "nonexistent", "--value", "new_value"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_secrets_delete(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test deleting a secret with --yes flag"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = mock_secret
        mock_secrets_api.delete.return_value = None

        # Run command with --yes to skip confirmation
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "delete", "test_secret", "--yes"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "deleted" in result.output.lower()
        mock_secrets_api.delete.assert_called_once_with("test_secret", owner=None)

    def test_secrets_delete_with_owner(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test deleting a secret with owner specified"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = mock_secret
        mock_secrets_api.delete.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "delete", "test_secret", "--owner", "other_user", "--yes"
            ]
        )

        # Verify
        assert result.exit_code == 0
        mock_secrets_api.delete.assert_called_once_with("test_secret", owner="other_user")

    def test_secrets_delete_not_found(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test deleting a secret that doesn't exist"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "delete", "nonexistent", "--yes"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_secrets_delete_cancelled(self, cli_runner, temp_api_key_file, mock_secrets_api, mock_secret):
        """Test cancelling secret deletion"""
        # Setup mock
        mock_secrets_api.get_secret.return_value = mock_secret

        # Run command without --yes and answer 'n' to confirmation
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "secrets", "delete", "test_secret"
            ],
            input="n\n"
        )

        # Verify
        assert result.exit_code == 0
        assert "cancelled" in result.output.lower()
        mock_secrets_api.delete.assert_not_called()

    def test_secrets_get_value_json_output(self, cli_runner, temp_api_key_file, mock_secrets_api):
        """Test getting secret value with JSON output format"""
        # Setup mock
        mock_secrets_api.get.return_value = "secret_value"

        # Run command with JSON output
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "secrets", "get", "test_secret", "--show-value"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "secret_value" in result.output
        # JSON output should include both name and value
        assert "test_secret" in result.output
