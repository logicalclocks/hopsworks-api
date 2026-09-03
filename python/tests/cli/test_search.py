"""``hops search ls``: filters reach the SDK and an empty result says where it looked."""

from __future__ import annotations

from unittest import mock

from click.testing import CliRunner
from hopsworks.cli.main import cli


def _search_api(mock_project, **lists):
    api = mock_project.get_search_api.return_value
    result = mock.MagicMock(name="FeaturestoreSearchResult")
    for kind in ("feature_groups", "feature_views", "training_datasets", "features"):
        setattr(result, kind, lists.get(kind, []))
    api.feature_store.return_value = result
    return api


def test_empty_project_search_names_the_project_and_offers_global(mock_project):
    _search_api(mock_project)

    result = CliRunner().invoke(cli, ["search", "ls", "blah"])

    assert result.exit_code == 0, result.output
    assert "No results in project demo." in result.output
    assert "--global" in result.output


def test_empty_global_search_says_so(mock_project):
    api = _search_api(mock_project)

    result = CliRunner().invoke(cli, ["search", "ls", "blah", "--global"])

    assert result.exit_code == 0, result.output
    assert "No results in any project you belong to." in result.output
    assert api.feature_store.call_args.kwargs["global_search"] is True


def test_tag_and_keyword_filters_reach_the_sdk_without_a_term(mock_project):
    api = _search_api(mock_project)

    result = CliRunner().invoke(
        cli,
        [
            "search",
            "ls",
            "--tag",
            "quality:owner=risk",
            "--keyword",
            "pii",
            "--limit",
            "5",
        ],
    )

    assert result.exit_code == 0, result.output
    kwargs = api.feature_store.call_args.kwargs
    assert kwargs["search_term"] is None
    assert kwargs["tag_filter"] == [
        {"name": "quality", "key": "owner", "value": "risk"}
    ]
    assert kwargs["keyword_filter"] == ["pii"]
    assert kwargs["limit"] == 5


def test_malformed_tag_is_a_usage_error(mock_project):
    result = CliRunner().invoke(cli, ["search", "ls", "--tag", "quality"])

    assert result.exit_code == 2
    assert "name:key=value" in result.output


def test_results_render_kind_name_and_project(mock_project):
    fg = mock.MagicMock()
    fg.name = "transactions"
    fg.version = 2
    fg.project.name = "demo"
    fg.description = "Card transactions\nsecond line"
    _search_api(mock_project, feature_groups=[fg])

    result = CliRunner().invoke(cli, ["search", "ls", "transactions"])

    assert result.exit_code == 0, result.output
    assert "feature_group" in result.output
    assert "transactions" in result.output
    assert "Card transactions" in result.output
    assert "second line" not in result.output


def test_help_shows_examples(tmp_home):
    result = CliRunner().invoke(cli, ["search", "ls", "--help"])

    assert result.exit_code == 0
    assert "--tag quality:owner=risk" in result.output
    assert "--global" in result.output
