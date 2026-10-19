"""Training dataset splits and the version options of `hops td`."""

from __future__ import annotations

import inspect
import json
from unittest import mock

import pandas as pd
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hsfs.feature_view import FeatureView


def _fv(mock_project):
    fv = mock.MagicMock()
    mock_project.get_feature_store.return_value.get_feature_view.return_value = fv
    return fv


def test_compute_with_validation_makes_a_three_way_split(mock_project):
    fv = _fv(mock_project)
    fv.create_train_validation_test_split.return_value = (4, mock.MagicMock())
    result = CliRunner().invoke(
        cli,
        ["td", "compute", "orders", "--fv-version", "1"]
        + ["--split", "train:0.7,validation:0.2,test:0.1"],
    )
    assert result.exit_code == 0, result.output
    kwargs = fv.create_train_validation_test_split.call_args.kwargs
    assert (kwargs["validation_size"], kwargs["test_size"]) == (0.2, 0.1)
    declared = inspect.signature(
        FeatureView.create_train_validation_test_split
    ).parameters
    assert set(kwargs) <= set(declared)
    fv.create_train_test_split.assert_not_called()


def test_read_returns_one_split_with_its_labels(mock_project):
    fv = _fv(mock_project)
    td = mock.MagicMock(version=4, splits=[mock.MagicMock()] * 3)
    fv.get_training_datasets.return_value = [td]
    frames = [pd.DataFrame({"x": [i]}) for i in range(3)]
    labels = [pd.DataFrame({"y": [10 + i]}) for i in range(3)]
    fv.get_train_validation_test_split.return_value = (*frames, *labels)
    result = CliRunner().invoke(
        cli,
        ["--json", "td", "read", "orders"]
        + ["--fv-version", "1", "--td-version", "4", "--split", "validation"],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == [{"x": 1, "y": 11}]


def test_read_refuses_a_split_the_dataset_does_not_have(mock_project):
    fv = _fv(mock_project)
    td = mock.MagicMock(version=4, splits=[mock.MagicMock()] * 2)
    fv.get_training_datasets.return_value = [td]
    result = CliRunner().invoke(
        cli,
        ["td", "read", "orders", "--td-version", "4", "--split", "validation"],
    )
    assert result.exit_code == 2
    assert "train, test" in result.output


def test_start_without_waiting_passes_zero(mock_project):
    deployment = mock.MagicMock()
    mock_project.get_model_serving.return_value.get_deployment.return_value = deployment
    result = CliRunner().invoke(cli, ["deployment", "start", "fraud", "--no-wait"])
    assert result.exit_code == 0, result.output
    deployment.start.assert_called_with(await_running=0)
