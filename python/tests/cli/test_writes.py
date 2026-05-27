"""CliRunner tests for Phase 2 mutation commands.

We patch the SDK via the shared ``mock_project`` fixture, then reach into the
mock to configure per-command behavior. The aim is coverage of the happy path
plus the most common error branches — not a Hopsworks cluster integration
test.
"""

from __future__ import annotations

import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli


# --- fg delete ------------------------------------------------------------


def test_fg_delete_confirms_and_calls_sdk(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fg.name, fg.version = "txn", 1
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "delete", "txn", "--yes"])
    assert result.exit_code == 0, result.output
    fg.delete.assert_called_once()


def test_fg_delete_aborts_without_yes(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fg.name, fg.version = "txn", 1
    fs.get_feature_group.return_value = fg
    # Simulate the user answering "n" to the confirmation prompt.
    result = CliRunner().invoke(cli, ["fg", "delete", "txn"], input="n\n")
    assert result.exit_code != 0
    fg.delete.assert_not_called()


# --- fg keywords / add / remove -------------------------------------------


def test_fg_keywords_table(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fg.get_tags.return_value = {"prod": mock.Mock(value="true")}
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "keywords", "txn"])
    assert result.exit_code == 0, result.output
    assert "prod" in result.output


def test_fg_add_keyword(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "add-keyword", "txn", "prod"])
    assert result.exit_code == 0, result.output
    fg.add_tag.assert_called_with(name="prod", value="true")


def test_fg_remove_keyword(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "remove-keyword", "txn", "prod"])
    assert result.exit_code == 0, result.output
    fg.delete_tag.assert_called_with("prod")


# --- fg stats --------------------------------------------------------------


def test_fg_stats_compute(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "stats", "txn", "--compute"])
    assert result.exit_code == 0, result.output
    fg.compute_statistics.assert_called_once()


# --- fg search -------------------------------------------------------------


def test_fg_search_passes_vector_and_k(mock_project):
    fs = mock_project.get_feature_store.return_value
    fg = mock.MagicMock()
    fg.find_neighbors.return_value = [(0.99, [1, 2.0]), (0.80, [3, 4.0])]
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(
        cli, ["fg", "search", "docs", "--vector", "0.1,0.2,0.3", "--k", "2"]
    )
    assert result.exit_code == 0, result.output
    fg.find_neighbors.assert_called_with(embedding=[0.1, 0.2, 0.3], col=None, k=2)


def test_fg_search_rejects_bad_vector(mock_project):
    result = CliRunner().invoke(
        cli, ["fg", "search", "docs", "--vector", "not-a-number"]
    )
    assert result.exit_code != 0
    assert "vector" in result.output.lower()


# --- fg insert (synthetic) -------------------------------------------------


def test_fg_insert_generate_calls_sdk(mock_project):
    fs = mock_project.get_feature_store.return_value
    feat = mock.MagicMock()
    feat.name, feat.type = "id", "bigint"
    fg = mock.MagicMock()
    fg.name, fg.version = "txn", 1
    fg.features = [feat]
    fg.insert.return_value = (None, None)
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "insert", "txn", "--generate", "3"])
    assert result.exit_code == 0, result.output
    assert fg.insert.called
    df = fg.insert.call_args[0][0]
    assert len(df) == 3
    assert list(df.columns) == ["id"]


# --- fv create -------------------------------------------------------------


def test_fv_create_parses_joins(mock_project):
    fs = mock_project.get_feature_store.return_value
    base = mock.MagicMock()
    other = mock.MagicMock()
    query = mock.MagicMock()
    base.select_all.return_value = query
    query.join.return_value = query
    fs.get_feature_group.side_effect = lambda name, version=None: (
        base if name == "transactions" else other
    )
    created = mock.MagicMock()
    created.name, created.version = "enriched", 1
    fs.create_feature_view.return_value = created
    result = CliRunner().invoke(
        cli,
        [
            "fv",
            "create",
            "enriched",
            "--feature-group",
            "transactions",
            "--join",
            "products LEFT product_id=id p_",
        ],
    )
    assert result.exit_code == 0, result.output
    fs.create_feature_view.assert_called_once()


# --- fv get ----------------------------------------------------------------


def test_fv_get_parses_entry(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.get_feature_vector.return_value = [1, 2, 3]
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(
        cli, ["--json", "fv", "get", "enriched", "--entry", "id=42,region=eu"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["entry"] == {"id": 42, "region": "eu"}
    assert payload["vector"] == [1, 2, 3]


# --- fv delete -------------------------------------------------------------


def test_fv_delete_force(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.version = 1
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["fv", "delete", "enriched", "--yes", "--force"])
    assert result.exit_code == 0, result.output
    fv.delete.assert_called_with(force=True)


# --- td --------------------------------------------------------------------


def test_td_compute_without_split(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.create_training_data.return_value = (5, mock.MagicMock())
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["td", "compute", "enriched", "1"])
    assert result.exit_code == 0, result.output
    fv.create_training_data.assert_called_once()


def test_td_compute_with_split(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.create_train_test_split.return_value = (7, mock.MagicMock())
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(
        cli, ["td", "compute", "enriched", "1", "--split", "train:0.8,test:0.2"]
    )
    assert result.exit_code == 0, result.output
    fv.create_train_test_split.assert_called_once()


# --- model register --------------------------------------------------------


def test_model_register_uses_framework_section(mock_project, tmp_path):
    # Stage a fake artifact.
    artifact = tmp_path / "model.pkl"
    artifact.write_bytes(b"\x00")
    mr = mock.MagicMock()
    model = mock.MagicMock()
    model.name, model.version = "fraud", 1
    mr.sklearn.create_model.return_value = model
    mock_project.get_model_registry.return_value = mr
    result = CliRunner().invoke(
        cli,
        [
            "model",
            "register",
            "fraud",
            str(artifact),
            "--framework",
            "sklearn",
            "--metrics",
            "accuracy=0.95",
        ],
    )
    assert result.exit_code == 0, result.output
    mr.sklearn.create_model.assert_called_once()
    kwargs = mr.sklearn.create_model.call_args.kwargs
    assert kwargs["name"] == "fraud"
    assert kwargs["metrics"] == {"accuracy": 0.95}
    model.save.assert_called_with(str(artifact))


def test_model_register_rejects_unknown_framework(mock_project, tmp_path):
    artifact = tmp_path / "m.pkl"
    artifact.write_bytes(b"\x00")
    result = CliRunner().invoke(
        cli, ["model", "register", "x", str(artifact), "--framework", "zebra"]
    )
    assert result.exit_code != 0


# --- model download / delete ----------------------------------------------


def test_model_download(mock_project):
    mr = mock.MagicMock()
    model = mock.MagicMock()
    model.download.return_value = "/tmp/fraud_v1"
    mr.get_model.return_value = model
    mock_project.get_model_registry.return_value = mr
    result = CliRunner().invoke(
        cli, ["model", "download", "fraud", "--version", "1", "--output", "/tmp/out"]
    )
    assert result.exit_code == 0, result.output
    model.download.assert_called_with(local_path="/tmp/out")


def test_model_delete_requires_version(mock_project):
    result = CliRunner().invoke(cli, ["model", "delete", "fraud"])
    assert result.exit_code != 0


# --- transformation list ---------------------------------------------------


def test_transformation_list(mock_project):
    fs = mock_project.get_feature_store.return_value
    tf = mock.MagicMock()
    tf.id, tf.version = 1, 2
    udf = mock.MagicMock()
    udf.function_name, udf.output_type = "min_max_scaler", float
    tf.hopsworks_udf = udf
    fs.get_transformation_functions.return_value = [tf]
    result = CliRunner().invoke(cli, ["transformation", "list"])
    assert result.exit_code == 0, result.output
    assert "min_max_scaler" in result.output


# --- transformation create (file parsing) ----------------------------------


def test_transformation_create_from_file(mock_project, tmp_path):
    src = tmp_path / "scaler.py"
    src.write_text(
        "from hsfs.hopsworks_udf import udf\n\n"
        "@udf(float)\n"
        "def double_it(x):\n"
        "    return x * 2\n"
    )
    fs = mock_project.get_feature_store.return_value
    created_tf = mock.MagicMock()
    created_tf.version = 1
    fs.create_transformation_function.return_value = created_tf
    # Patch the @udf decorator so it returns a callable without hitting hsfs
    # decorator internals that require a live connection.
    with mock.patch("hsfs.hopsworks_udf.udf", return_value=lambda fn: fn):
        result = CliRunner().invoke(
            cli, ["transformation", "create", "--file", str(src)]
        )
    assert result.exit_code == 0, result.output
    fs.create_transformation_function.assert_called_once()


def test_transformation_create_rejects_no_source(mock_project):
    result = CliRunner().invoke(cli, ["transformation", "create"])
    assert result.exit_code != 0


@pytest.mark.parametrize(
    "src",
    [
        "def no_decorator(x): return x",
        "from hsfs.hopsworks_udf import udf\n\n@udf(float)\ndef a(x): return x\n@udf(float)\ndef b(x): return x\n",
    ],
)
def test_transformation_create_validates_single_udf(mock_project, tmp_path, src):
    f = tmp_path / "bad.py"
    f.write_text(src)
    result = CliRunner().invoke(cli, ["transformation", "create", "--file", str(f)])
    assert result.exit_code != 0
