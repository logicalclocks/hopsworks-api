"""An omitted ``--version`` means the latest version, whatever order the SDK lists them in.

The SDK's getters default a missing version to 1, and the model registry lists
versions newest first, so taking either the getter or the list's last entry
deployed version 1 of a retrained model.
"""

from __future__ import annotations

import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import versions
from hopsworks.cli.main import cli


def _versioned(*numbers):
    items = []
    for number in numbers:
        item = mock.MagicMock(name=f"v{number}")
        item.version = number
        items.append(item)
    return items


@pytest.mark.parametrize(
    ("resolve", "lister", "getter"),
    [
        (versions.feature_group, "get_feature_groups", "get_feature_group"),
        (versions.feature_view, "get_feature_views", "get_feature_view"),
        (versions.model, "get_models", "get_model"),
    ],
)
def test_an_omitted_version_is_the_highest_and_a_given_one_is_fetched(
    resolve, lister, getter
):
    source = mock.MagicMock()
    getattr(source, lister).return_value = _versioned(3, 1, 2)
    assert resolve(source, "fraud", None).version == 3
    getattr(source, getter).assert_not_called()

    resolve(source, "fraud", 1)
    getattr(source, getter).assert_called_once_with("fraud", version=1)

    getattr(source, lister).return_value = []
    assert resolve(source, "fraud", None) is None


def test_fv_info_without_a_version_shows_the_latest(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_views.side_effect = None
    fs.get_feature_views.return_value = _versioned(2, 3, 1)
    for item in fs.get_feature_views.return_value:
        item.name = "fraud_fv"
        item.features = []
        item.labels = []
    result = CliRunner().invoke(cli, ["fv", "info", "fraud_fv", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["version"] == 3
    fs.get_feature_view.assert_not_called()


def test_model_info_without_a_version_shows_the_latest(mock_project):
    registry = mock.MagicMock()
    registry.get_models.return_value = _versioned(1, 3, 2)
    for model in registry.get_models.return_value:
        model.name, model.framework, model.description = "fraud", "PYTHON", ""
        model.training_metrics, model.created, model.environment = {}, None, None
    mock_project.get_model_registry.return_value = registry
    result = CliRunner().invoke(cli, ["model", "info", "fraud", "--json"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["version"] == 3
    registry.get_model.assert_not_called()
