"""CliRunner tests for tags surfaced in ``info`` output.

``hops fg/fv/model info`` read the entity's ``get_tags()`` and render the
flattened name -> value mapping alongside the description, in both table and
JSON modes. A backend error degrades to no tags rather than failing the
command.
"""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


class _Tag:
    """SDK ``Tag`` stand-in exposing ``.value``."""

    def __init__(self, value):
        self.value = value


def _tags():
    return {"team": _Tag("ml"), "pii": _Tag(False)}


# --- feature group ---------------------------------------------------------


def _fg():
    fg = mock.MagicMock()
    fg.id, fg.name, fg.version = 101, "transactions", 1
    fg.online_enabled = False
    fg.primary_key, fg.event_time, fg.description = ["id"], None, ""
    fg.features = []
    return fg


def test_fg_info_table_shows_tags(mock_project):
    fg = _fg()
    fg.get_tags.return_value = _tags()
    mock_project.get_feature_store.return_value.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "info", "transactions"])
    assert result.exit_code == 0, result.output
    assert "Tags" in result.output
    assert "team=ml" in result.output


def test_fg_info_json_shows_tags(mock_project):
    fg = _fg()
    fg.get_tags.return_value = _tags()
    mock_project.get_feature_store.return_value.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["--json", "fg", "info", "transactions"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["tags"] == {"team": "ml", "pii": False}


def test_fg_info_tags_failure_is_tolerated(mock_project):
    fg = _fg()
    fg.get_tags.side_effect = RuntimeError("backend down")
    mock_project.get_feature_store.return_value.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["--json", "fg", "info", "transactions"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["tags"] == {}


# --- feature view ----------------------------------------------------------


def test_fv_info_json_shows_tags(mock_project):
    fv = mock.MagicMock()
    fv.id, fv.name, fv.version = 11, "fraud_fv", 1
    fv.labels, fv.description, fv.features = ["fraud"], "", []
    fv.transformation_functions = []
    fv.get_tags.return_value = _tags()
    mock_project.get_feature_store.return_value.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["--json", "fv", "info", "fraud_fv"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["tags"] == {"team": "ml", "pii": False}


# --- model -----------------------------------------------------------------


def test_model_info_json_shows_tags(mock_project):
    mr = mock.MagicMock()
    model = mock.MagicMock()
    model.name, model.version, model.framework = "fraud_model", 3, "sklearn"
    model.created, model.description, model.training_metrics = "-", "", {}
    model.get_tags.return_value = _tags()
    mr.get_model.return_value = model
    mock_project.get_model_registry.return_value = mr
    result = CliRunner().invoke(
        cli, ["--json", "model", "info", "fraud_model", "--version", "3"]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["tags"] == {"team": "ml", "pii": False}
