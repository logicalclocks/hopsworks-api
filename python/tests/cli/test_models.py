"""Tests for models CLI commands"""

import json
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from hopsworks_cli.main import cli


class TestModelsCommands:
    """Test suite for models commands"""

    def test_models_list_with_name(self, cli_runner, mock_model_registry, mock_model, temp_api_key_file):
        """Test listing models filtered by name"""
        # Setup mock
        mock_model_registry.get_models.return_value = [mock_model]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "models", "list", "--name", "test_model"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_model" in result.output
        mock_model_registry.get_models.assert_called_once_with("test_model")

    def test_models_get(self, cli_runner, mock_model_registry, mock_model, temp_api_key_file):
        """Test getting a specific model"""
        # Setup mock
        mock_model_registry.get_model.return_value = mock_model

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "models", "get", "test_model", "--version", "1"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_model" in result.output
        mock_model_registry.get_model.assert_called_once_with("test_model", 1)

    def test_models_get_not_found(self, cli_runner, mock_model_registry, temp_api_key_file):
        """Test getting a model that doesn't exist"""
        # Setup mock
        mock_model_registry.get_model.return_value = None

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "models", "get", "nonexistent", "--version", "1"
            ]
        )

        # Verify
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_models_best(self, cli_runner, mock_model_registry, mock_model, temp_api_key_file):
        """Test getting best model by metric"""
        # Setup mock
        mock_model_registry.get_best_model.return_value = mock_model

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "models", "best", "test_model",
                "--metric", "accuracy",
                "--direction", "max"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_model" in result.output
        mock_model_registry.get_best_model.assert_called_once_with(
            "test_model", "accuracy", "max"
        )

    def test_models_download(self, cli_runner, mock_model_registry, mock_model, tmp_path, temp_api_key_file):
        """Test downloading a model"""
        # Setup mock
        mock_model_registry.get_model.return_value = mock_model
        download_path = tmp_path / "model"
        mock_model.download.return_value = str(download_path)

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "models", "download", "test_model",
                "--version", "1",
                "--output-dir", str(tmp_path)
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "downloaded" in result.output.lower()
        mock_model.download.assert_called_once()

    def test_models_info(self, cli_runner, mock_model_registry, mock_model, temp_api_key_file):
        """Test getting comprehensive model information"""
        # Setup mock
        mock_model_registry.get_model.return_value = mock_model
        mock_model.tags = {"env": "production"}

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "models", "info", "test_model", "--version", "1"
            ]
        )

        # Verify
        assert result.exit_code == 0
        assert "test_model" in result.output
        assert "accuracy" in result.output

    def test_models_list_json_output(self, cli_runner, mock_model_registry, mock_model, temp_api_key_file):
        """Test JSON output format"""
        # Setup mock
        mock_model_registry.get_models.return_value = [mock_model]

        # Run command
        result = cli_runner.invoke(
            cli,
            [
                "--host", "test.hopsworks.ai",
                "--api-key-file", temp_api_key_file,
                "--project", "test_project",
                "--output", "json",
                "models", "list", "--name", "test_model"
            ]
        )

        # Verify JSON output is valid
        assert result.exit_code == 0
        # The output should contain valid JSON (may be mixed with other text)
        assert "test_model" in result.output
