"""Tests for feature groups CLI commands"""

import pytest
from unittest.mock import Mock
from hopsworks_cli.main import cli


class TestFeatureGroupsCommands:
    """Test suite for feature groups commands"""

    def test_feature_groups_list(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test listing feature groups"""
        # Setup mock - return dict with items
        mock_feature_store.get_feature_groups.return_value = {
            "items": [mock_feature_group],
            "count": 1
        }

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_feature_group" in result.output
        mock_feature_store.get_feature_groups.assert_called_once_with(name=None)

    def test_feature_groups_list_with_name(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test listing feature groups filtered by name"""
        # Setup mock
        mock_feature_store.get_feature_groups.return_value = {
            "items": [mock_feature_group],
            "count": 1
        }

        # Run command with name filter
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "list", "--name", "test_feature_group"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_feature_group" in result.output
        mock_feature_store.get_feature_groups.assert_called_once_with(name="test_feature_group")

    def test_feature_groups_list_empty(self, cli_runner, temp_api_key_file, mock_feature_store):
        """Test listing feature groups when none exist"""
        # Setup mock
        mock_feature_store.get_feature_groups.return_value = {
            "items": [],
            "count": 0
        }

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "feature-groups", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "No feature groups found" in result.output

    def test_feature_groups_list_single_object(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test listing when API returns a single feature group object"""
        # Setup mock - return single object instead of dict
        mock_feature_store.get_feature_groups.return_value = mock_feature_group

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_feature_group" in result.output

    def test_feature_groups_get(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test getting a specific feature group"""
        # Setup mock
        mock_feature_store.get_feature_group.return_value = mock_feature_group

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "get", "test_feature_group"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_feature_group" in result.output
        mock_feature_store.get_feature_group.assert_called_once_with("test_feature_group")

    def test_feature_groups_get_with_version(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test getting a specific version of a feature group"""
        # Setup mock
        mock_feature_store.get_feature_group.return_value = mock_feature_group

        # Run command with version
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "get", "test_feature_group", "--version", "1"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_feature_group" in result.output
        mock_feature_store.get_feature_group.assert_called_once_with("test_feature_group", version=1)

    def test_feature_groups_get_not_found(self, cli_runner, temp_api_key_file, mock_feature_store):
        """Test getting a feature group that doesn't exist"""
        # Setup mock
        mock_feature_store.get_feature_group.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "feature-groups", "get", "nonexistent"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_feature_groups_schema(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test getting feature group schema"""
        # Setup mock
        mock_feature_store.get_feature_group.return_value = mock_feature_group

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "feature-groups", "schema", "test_feature_group"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "Schema for feature group" in result.output
        assert "test_feature_group" in result.output
        assert "id" in result.output  # feature name
        assert "bigint" in result.output  # feature type
        assert "timestamp" in result.output  # feature name
        assert "Total features: 3" in result.output
        assert "Primary key" in result.output
        assert "Event time" in result.output

    def test_feature_groups_schema_with_version(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test getting schema for specific version"""
        # Setup mock
        mock_feature_store.get_feature_group.return_value = mock_feature_group

        # Run command with version
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "feature-groups", "schema", "test_feature_group", "--version", "1"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "Schema for feature group" in result.output
        mock_feature_store.get_feature_group.assert_called_once_with("test_feature_group", version=1)

    def test_feature_groups_schema_json_output(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test getting schema with JSON output"""
        # Setup mock
        mock_feature_store.get_feature_group.return_value = mock_feature_group

        # Run command with JSON output
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "schema", "test_feature_group"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "id" in result.output
        assert "bigint" in result.output

    def test_feature_groups_schema_no_features(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test getting schema when feature group has no features"""
        # Setup mock - feature group with no features
        mock_feature_group.features = []
        mock_feature_store.get_feature_group.return_value = mock_feature_group

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "feature-groups", "schema", "test_feature_group"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "no features defined" in result.output.lower()

    def test_feature_groups_list_as_list(self, cli_runner, temp_api_key_file, mock_feature_store, mock_feature_group):
        """Test listing when API returns a list"""
        # Setup mock - return list
        mock_feature_store.get_feature_groups.return_value = [mock_feature_group]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "feature-groups", "list"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_feature_group" in result.output

    def test_feature_groups_list_type_error(self, cli_runner, temp_api_key_file, mock_feature_store):
        """Test listing when API raises TypeError (HSFS compatibility issue)"""
        # Setup mock to raise TypeError
        mock_feature_store.get_feature_groups.side_effect = TypeError("FeatureGroup.__init__() missing 3 required positional arguments")

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "feature-groups", "list"
            ]
        )

        # Verify - should show the compatibility error message
        assert result.exit_code == 0
        assert "HSFS library compatibility issue" in result.output
        assert "Workarounds:" in result.output
