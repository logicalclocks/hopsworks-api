"""CliRunner tests for every Phase 1 read command.

Each test mocks the SDK surface through the shared ``mock_project`` fixture
and asserts that the command renders the right data in both table and JSON
modes. No network, no real SDK.
"""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.commands import connector as connector_cmd
from hopsworks.cli.commands import context as context_cmd
from hopsworks.cli.commands import fv as fv_cmd
from hopsworks.cli.commands import model as model_cmd
from hopsworks.cli.main import cli


def _feature(name, type_, primary=False, partition=False, description=None):
    f = mock.MagicMock()
    f.name = name
    f.type = type_
    f.primary = primary
    f.partition = partition
    f.description = description
    return f


def _feature_group(name, version=1, online=False, features=None, primary_key=None):
    fg = mock.MagicMock()
    fg.id = 100 + version
    fg.name = name
    fg.version = version
    fg.online_enabled = online
    fg.features = features or []
    fg.primary_key = primary_key or []
    fg.event_time = None
    fg.description = ""
    fg.stream = False
    return fg


# --- fs --------------------------------------------------------------------


def test_fs_list(mock_project):
    result = CliRunner().invoke(cli, ["fs", "list"])
    assert result.exit_code == 0, result.output
    assert "demo_featurestore" in result.output
    assert "67" in result.output


# --- fg --------------------------------------------------------------------


def test_fg_list_table(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_groups.return_value = [
        _feature_group("transactions", 1, online=True),
        _feature_group("products", 2),
    ]
    result = CliRunner().invoke(cli, ["fg", "list"])
    assert result.exit_code == 0, result.output
    assert "transactions" in result.output
    assert "products" in result.output


def test_fg_list_json(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_groups.return_value = [_feature_group("transactions", 1)]
    result = CliRunner().invoke(cli, ["--json", "fg", "list"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload[0]["NAME"] == "transactions"


def test_fg_info_renders_fields(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _feature_group(
        "transactions",
        version=3,
        online=True,
        primary_key=["id"],
        features=[_feature("id", "bigint", primary=True)],
    )
    result = CliRunner().invoke(cli, ["fg", "info", "transactions", "--version", "3"])
    assert result.exit_code == 0, result.output
    fs.get_feature_group.assert_called_with("transactions", version=3)
    assert "transactions" in result.output
    assert "id" in result.output


def test_fg_info_not_found(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.side_effect = RuntimeError("missing")
    result = CliRunner().invoke(cli, ["fg", "info", "absent"])
    assert result.exit_code != 0
    assert "absent" in result.output


def test_fg_features_table(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _feature_group(
        "transactions",
        features=[
            _feature("id", "bigint", primary=True),
            _feature("amount", "double"),
        ],
    )
    result = CliRunner().invoke(cli, ["fg", "features", "transactions"])
    assert result.exit_code == 0, result.output
    assert "id" in result.output
    assert "amount" in result.output


def test_fg_preview(mock_project):
    import pandas as pd

    fs = mock_project.get_feature_store.return_value
    fg = _feature_group("transactions", features=[_feature("id", "bigint")])
    fg.read.return_value = pd.DataFrame({"id": [1, 2, 3, 4, 5]})
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "preview", "transactions", "--n", "3"])
    assert result.exit_code == 0, result.output
    fg.read.assert_called_with(online=False, dataframe_type="pandas")
    assert "id" in result.output


# --- fv --------------------------------------------------------------------


def test_fv_list_hits_rest_endpoint(mock_project):
    fake_items = [
        {
            "id": 11,
            "name": "fraud_fv",
            "version": 1,
            "labels": ["fraud"],
            "description": "",
        },
    ]
    with mock.patch.object(fv_cmd, "_list_feature_views", return_value=fake_items):
        result = CliRunner().invoke(cli, ["fv", "list"])
    assert result.exit_code == 0, result.output
    assert "fraud_fv" in result.output


def test_fv_info(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.id, fv.name, fv.version = 11, "fraud_fv", 1
    fv.labels, fv.description, fv.features = ["fraud"], "", []
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["fv", "info", "fraud_fv"])
    assert result.exit_code == 0, result.output
    assert "fraud_fv" in result.output


# --- connector -------------------------------------------------------------


def test_connector_list(mock_project):
    items = [
        {
            "id": 1,
            "name": "my_sf",
            "storageConnectorType": "SNOWFLAKE",
            "description": "",
        }
    ]
    with mock.patch.object(connector_cmd, "_list_connectors", return_value=items):
        result = CliRunner().invoke(cli, ["connector", "list"])
    assert result.exit_code == 0, result.output
    assert "my_sf" in result.output


def test_connector_info(mock_project):
    fs = mock_project.get_feature_store.return_value
    sc = mock.MagicMock()
    sc.id, sc.name, sc.description = 1, "my_sf", ""
    ds = mock.MagicMock()
    ds.storage_connector = sc
    fs.get_data_source.return_value = ds
    result = CliRunner().invoke(cli, ["connector", "info", "my_sf"])
    assert result.exit_code == 0, result.output
    assert "my_sf" in result.output


# --- td --------------------------------------------------------------------


def test_td_list(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    td = mock.MagicMock()
    td.version, td.data_format, td.coalesce, td.train_split = 1, "parquet", 4, "train"
    fv.get_training_datasets.return_value = [td]
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["td", "list", "fraud_fv"])
    assert result.exit_code == 0, result.output
    assert "parquet" in result.output


# --- model -----------------------------------------------------------------


def test_model_list(mock_project):
    items = [
        {
            "name": "fraud_detector",
            "version": 1,
            "framework": "sklearn",
            "metrics": {"auc": 0.91},
        }
    ]
    with mock.patch.object(model_cmd, "_list_models", return_value=items):
        result = CliRunner().invoke(cli, ["model", "list"])
    assert result.exit_code == 0, result.output
    assert "fraud_detector" in result.output


def test_model_info_specific_version(mock_project):
    mr = mock.MagicMock()
    model = mock.MagicMock()
    model.name, model.version, model.framework = "fraud_detector", 2, "sklearn"
    model.training_metrics = {"auc": 0.92}
    model.created, model.description = "2026-04-01", ""
    mr.get_model.return_value = model
    mock_project.get_model_registry.return_value = mr
    result = CliRunner().invoke(
        cli, ["model", "info", "fraud_detector", "--version", "2"]
    )
    assert result.exit_code == 0, result.output
    mr.get_model.assert_called_with("fraud_detector", version=2)


# --- deployment ------------------------------------------------------------


def test_deployment_list(mock_project):
    ms = mock.MagicMock()
    d = mock.MagicMock()
    d.id, d.name = 1, "fraud_predict"
    d.model_name, d.model_version = "fraud_detector", 1
    d.serving_tool, d.model_server, d.status = "KSERVE", "PYTHON", "RUNNING"
    ms.get_deployments.return_value = [d]
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "list"])
    assert result.exit_code == 0, result.output
    assert "fraud_predict" in result.output


def test_deployment_info_not_found(mock_project):
    ms = mock.MagicMock()
    ms.get_deployment.return_value = None
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "info", "missing"])
    assert result.exit_code != 0
    assert "missing" in result.output


# --- job -------------------------------------------------------------------


def test_job_list(mock_project):
    api = mock.MagicMock()
    j = mock.MagicMock()
    j.id, j.name = 7, "train_job"
    j.job_type, j.creator, j.creation_time = "PYTHON", "jim", "2026-04-01"
    api.get_jobs.return_value = [j]
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "list"])
    assert result.exit_code == 0, result.output
    assert "train_job" in result.output


def test_job_info_not_found(mock_project):
    api = mock.MagicMock()
    api.get_job.return_value = None
    mock_project.get_job_api.return_value = api
    result = CliRunner().invoke(cli, ["job", "info", "missing"])
    assert result.exit_code != 0


# --- dataset ---------------------------------------------------------------


def test_dataset_list_strings(mock_project):
    api = mock.MagicMock()
    api.list.return_value = ["Resources", "Jupyter"]
    mock_project.get_dataset_api.return_value = api
    result = CliRunner().invoke(cli, ["dataset", "list"])
    assert result.exit_code == 0, result.output
    assert "Resources" in result.output


def test_dataset_list_inodes(mock_project):
    api = mock.MagicMock()
    inode = mock.MagicMock()
    inode.name, inode.dir, inode.size = "train.parquet", False, 1024
    api.list.return_value = [inode]
    mock_project.get_dataset_api.return_value = api
    result = CliRunner().invoke(cli, ["dataset", "list", "Resources"])
    assert result.exit_code == 0, result.output
    assert "train.parquet" in result.output


# --- context ---------------------------------------------------------------


def test_context_markdown(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_groups.return_value = [
        _feature_group("txn", features=[_feature("id", "bigint", primary=True)])
    ]
    with (
        mock.patch.object(context_cmd.fv_cmd, "_list_feature_views", return_value=[]),
        mock.patch.object(context_cmd.model_cmd, "_list_models", return_value=[]),
    ):
        mock_project.get_job_api.return_value.get_jobs.return_value = []
        mock_project.get_model_serving.return_value.get_deployments.return_value = []
        result = CliRunner().invoke(cli, ["context"])
    assert result.exit_code == 0, result.output
    assert "# Hopsworks Project" in result.output
    assert "Feature Groups (1)" in result.output


def test_context_json(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_groups.return_value = [_feature_group("txn")]
    with (
        mock.patch.object(context_cmd.fv_cmd, "_list_feature_views", return_value=[]),
        mock.patch.object(context_cmd.model_cmd, "_list_models", return_value=[]),
    ):
        mock_project.get_job_api.return_value.get_jobs.return_value = []
        mock_project.get_model_serving.return_value.get_deployments.return_value = []
        result = CliRunner().invoke(cli, ["--json", "context"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["project"]["name"] == "demo"
    assert payload["feature_groups"][0]["name"] == "txn"


# --- unauthenticated -------------------------------------------------------


def test_unauthenticated_command_suggests_setup(tmp_home):
    result = CliRunner().invoke(cli, ["fg", "list"])
    assert result.exit_code != 0
    assert "hops setup" in result.output
