"""CliRunner tests for the ``lineage`` subcommands.

Every entity (feature group, feature view, training dataset, model,
deployment) exposes a ``lineage`` command that flattens the SDK's
``Links`` results into a shared table / JSON rendering. These tests drive the
commands through the ``mock_project`` fixture with hand-built link objects, so
no network or real SDK is touched.
"""

from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


class _Art:
    """Minimal stand-in for an Artifact / accessible domain object."""

    def __init__(self, name: str, version: int) -> None:
        self.name = name
        self.version = version


class _Links:
    """Stand-in for ``explicit_provenance.Links`` with the four buckets."""

    def __init__(self, accessible=None, inaccessible=None, deleted=None, faulty=None):
        self.accessible = accessible or []
        self.inaccessible = inaccessible or []
        self.deleted = deleted or []
        self.faulty = faulty or []


class _Model:
    """Concrete model stand-in.

    A real ``Model`` has no ``accessible``/``deleted`` attributes, so it
    renders as a single artifact rather than being mistaken for a ``Links``
    bucket holder (which an over-eager MagicMock would be).
    """

    def __init__(self, name, version, fv_links, td_links):
        self.name = name
        self.version = version
        self._fv = fv_links
        self._td = td_links

    def get_feature_view_provenance(self):
        return self._fv

    def get_training_dataset_provenance(self):
        return self._td


# --- feature group ---------------------------------------------------------


def _fg_with_provenance():
    fg = mock.MagicMock()
    fg.name = "transactions"
    fg.version = 1
    fg.get_parent_feature_groups.return_value = _Links(accessible=[_Art("raw", 1)])
    fg.get_storage_connector_provenance.return_value = _Links()
    fg.get_data_source_provenance.return_value = _Links()
    fg.get_generated_feature_views.return_value = _Links(
        accessible=[_Art("fraud_fv", 2)]
    )
    fg.get_generated_feature_groups.return_value = _Links()
    return fg


def test_fg_lineage_table(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _fg_with_provenance()
    result = CliRunner().invoke(cli, ["fg", "lineage", "transactions"])
    assert result.exit_code == 0, result.output
    assert "raw" in result.output
    assert "fraud_fv" in result.output
    assert "upstream" in result.output
    assert "downstream" in result.output


def test_fg_lineage_json(mock_project):
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = _fg_with_provenance()
    result = CliRunner().invoke(cli, ["--json", "fg", "lineage", "transactions"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    rels = {link["relationship"] for link in payload["links"]}
    assert "parent_feature_group" in rels
    assert "feature_view" in rels
    parent = next(
        link
        for link in payload["links"]
        if link["relationship"] == "parent_feature_group"
    )
    assert parent["artifacts"][0] == {
        "name": "raw",
        "version": 1,
        "status": "accessible",
    }


def test_fg_lineage_empty(mock_project):
    fg = mock.MagicMock()
    fg.name = "transactions"
    fg.version = 1
    for accessor in (
        "get_parent_feature_groups",
        "get_storage_connector_provenance",
        "get_data_source_provenance",
        "get_generated_feature_views",
        "get_generated_feature_groups",
    ):
        getattr(fg, accessor).return_value = _Links()
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "lineage", "transactions"])
    assert result.exit_code == 0, result.output
    assert "No lineage links found" in result.output


def test_fg_lineage_surfaces_real_error(mock_project):
    # A provenance accessor raising a real error (permission/auth/server) must
    # surface as a command failure, not be swallowed and rendered as "no
    # lineage". The SDK returns None for a genuinely absent edge, so anything
    # that raises is a real failure.
    fg = _fg_with_provenance()
    fg.get_data_source_provenance.side_effect = RuntimeError("backend down")
    fs = mock_project.get_feature_store.return_value
    fs.get_feature_group.return_value = fg
    result = CliRunner().invoke(cli, ["fg", "lineage", "transactions"])
    assert result.exit_code != 0


# --- feature view ----------------------------------------------------------


def test_fv_lineage(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.name = "fraud_fv"
    fv.version = 1
    fv.get_parent_feature_groups.return_value = _Links(
        accessible=[_Art("transactions", 1)]
    )
    fv.get_models_provenance.return_value = _Links(accessible=[_Art("fraud_model", 3)])
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["fv", "lineage", "fraud_fv"])
    assert result.exit_code == 0, result.output
    assert "transactions" in result.output
    assert "fraud_model" in result.output


# --- training dataset ------------------------------------------------------


def test_td_lineage_scopes_to_td_version(mock_project):
    fs = mock_project.get_feature_store.return_value
    fv = mock.MagicMock()
    fv.name = "fraud_fv"
    fv.version = 1
    fv.get_parent_feature_groups.return_value = _Links(
        accessible=[_Art("transactions", 1)]
    )
    fv.get_models_provenance.return_value = _Links(accessible=[_Art("fraud_model", 3)])
    fs.get_feature_view.return_value = fv
    result = CliRunner().invoke(cli, ["td", "lineage", "fraud_fv", "--td-version", "5"])
    assert result.exit_code == 0, result.output
    fv.get_models_provenance.assert_called_with(training_dataset_version=5)
    assert "transactions" in result.output
    assert "fraud_model" in result.output


# --- model -----------------------------------------------------------------


def test_model_lineage(mock_project):
    mr = mock.MagicMock()
    model = _Model(
        "fraud_model",
        3,
        _Links(accessible=[_Art("fraud_fv", 1)]),
        _Links(accessible=[_Art("fraud_fv", 1)]),
    )
    mr.get_model.return_value = model
    mock_project.get_model_registry.return_value = mr
    result = CliRunner().invoke(
        cli, ["model", "lineage", "fraud_model", "--version", "3"]
    )
    assert result.exit_code == 0, result.output
    assert "fraud_fv" in result.output
    assert "feature_view" in result.output
    assert "training_dataset" in result.output


# --- deployment ------------------------------------------------------------


def test_deployment_lineage(mock_project):
    ms = mock.MagicMock()
    deployment = mock.MagicMock()
    deployment.name = "fraud"
    model = _Model("fraud_model", 3, _Links(accessible=[_Art("fraud_fv", 1)]), _Links())
    deployment.get_model.return_value = model
    ms.get_deployment.return_value = deployment
    mock_project.get_model_serving.return_value = ms
    result = CliRunner().invoke(cli, ["deployment", "lineage", "fraud"])
    assert result.exit_code == 0, result.output
    assert "fraud_model" in result.output
    assert "fraud_fv" in result.output
