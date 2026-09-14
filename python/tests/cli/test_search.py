"""``hops search``: every result bucket is shown, filters reach the SDK, and an empty result says where it looked."""

from __future__ import annotations

from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks_common.search_results import FeaturestoreSearchResult


def _search_api(mock_project, response=None):
    api = mock_project.get_search_api.return_value
    api._search.return_value = FeaturestoreSearchResult(response or {})
    return api


def _item(name, **extra):
    return {"name": name, "parentProjectId": 119, "parentProjectName": "demo", **extra}


def test_empty_project_search_names_the_project_and_offers_global(mock_project):
    _search_api(mock_project)

    result = CliRunner().invoke(cli, ["search", "blah"])

    assert result.exit_code == 0, result.output
    assert "No results in project demo." in result.output
    assert "--global" in result.output


def test_empty_global_search_says_so(mock_project):
    api = _search_api(mock_project)

    result = CliRunner().invoke(cli, ["search", "blah", "--global"])

    assert result.exit_code == 0, result.output
    assert "No results in any project you belong to." in result.output
    assert api._search.call_args.kwargs["global_search"] is True
    assert api._search.call_args.kwargs["doc_type"] == "ALL"


def test_model_job_and_deployment_hits_are_listed(mock_project):
    _search_api(
        mock_project,
        {
            "models": [_item("all_MiniLM_L6_v2", version=1, framework="PYTHON")],
            "jobs": [_item("nightly_ingest", jobType="PYSPARK")],
            "apps": [_item("dashboard", jobType="PYTHON_APP")],
            "deployments": [_item("fraud_v3", servingTool="KSERVE")],
            "agents": [_item("support_bot", servingTool="KSERVE")],
            "featuregroups": [
                _item("transactions", version=2, description="Card rows\nmore")
            ],
        },
    )

    result = CliRunner().invoke(cli, ["search", "blah"])

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0].split() == ["KIND", "NAME", "VERSION", "PROJECT", "DETAIL"]
    kinds = [line.split()[0] for line in lines[2:]]
    assert kinds == ["feature_group", "job", "app", "model", "deployment", "agent"]
    by_kind = {line.split()[0]: line.split() for line in lines[2:]}
    assert by_kind["model"] == ["model", "all_MiniLM_L6_v2", "1", "demo", "PYTHON"]
    assert by_kind["job"] == ["job", "nightly_ingest", "-", "demo", "PYSPARK"]
    assert by_kind["feature_group"][:5] == [
        "feature_group",
        "transactions",
        "2",
        "demo",
        "Card",
    ]
    assert "more" not in result.output


def test_type_option_maps_to_the_backend_doc_type(mock_project):
    api = _search_api(mock_project)

    for spelling, doc_type in (
        ("model", "MODEL"),
        ("training_dataset", "TRAININGDATASET"),
        ("Agent", "AGENT"),
    ):
        CliRunner().invoke(cli, ["search", "x", "--type", spelling])
        assert api._search.call_args.kwargs["doc_type"] == doc_type


def test_tag_and_keyword_filters_reach_the_sdk_without_a_term(mock_project):
    api = _search_api(mock_project)

    result = CliRunner().invoke(
        cli,
        ["search", "--tag", "quality:owner=risk", "--keyword", "pii", "--limit", "5"],
    )

    assert result.exit_code == 0, result.output
    kwargs = api._search.call_args.kwargs
    assert kwargs["search_term"] is None
    assert kwargs["tag_filter"] == [
        {"name": "quality", "key": "owner", "value": "risk"}
    ]
    assert kwargs["keyword_filter"] == ["pii"]
    assert kwargs["limit"] == 5


def test_malformed_tag_is_a_usage_error(mock_project):
    result = CliRunner().invoke(cli, ["search", "--tag", "quality"])

    assert result.exit_code == 2
    assert "name:key=value" in result.output


def test_search_has_no_ls_subcommand(mock_project):
    _search_api(mock_project)

    result = CliRunner().invoke(cli, ["search", "ls"])

    assert result.exit_code == 0, result.output
    assert "No results in project demo." in result.output
    assert (
        mock_project.get_search_api.return_value._search.call_args.kwargs["search_term"]
        == "ls"
    )


def test_help_shows_examples(tmp_home):
    result = CliRunner().invoke(cli, ["search", "--help"])

    assert result.exit_code == 0
    assert "--tag quality:owner=risk" in result.output
    assert "--type model" in result.output
