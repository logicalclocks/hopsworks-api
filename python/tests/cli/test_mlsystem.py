"""`hops mlsystem`: the project's ML systems registry."""

from __future__ import annotations

import json
from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import session
from hopsworks.cli.main import cli
from hopsworks_common.core import ml_system_api


SYSTEMS = [
    {
        "id": 7,
        "name": "Churn next month",
        "owner": "meb10000",
        "ownerName": "Admin Admin",
        "pathToCode": "/Projects/churndemo/Users/meb10000/churn-example",
        "accessible": True,
        "lastUpdated": 1790582400000,
    },
    {
        "id": 8,
        "name": "Recs",
        "owner": "other",
        "ownerName": "Other User",
        "pathToCode": "https://github.com/o/recs",
        "accessible": None,
        "lastUpdated": 1790496000000,
    },
]


@pytest.fixture
def logged_in(monkeypatch):
    monkeypatch.setattr(session, "get_project", lambda ctx: mock.Mock(name="churndemo"))


def test_list_shows_each_system_with_its_code_access(monkeypatch, logged_in):
    monkeypatch.setattr(ml_system_api, "_list", lambda: SYSTEMS)
    done = CliRunner().invoke(cli, ["mlsystem", "list"])
    assert done.exit_code == 0, done.output
    assert "Churn next month" in done.output and "Admin Admin" in done.output
    assert "yes" in done.output and "repository" in done.output
    assert "2026-09-28" in done.output


def test_remove_finds_a_system_by_name_or_id(monkeypatch, logged_in):
    removed = []
    monkeypatch.setattr(ml_system_api, "_list", lambda: SYSTEMS)
    monkeypatch.setattr(ml_system_api, "_remove", removed.append)
    assert CliRunner().invoke(cli, ["mlsystem", "remove", "Recs"]).exit_code == 0
    assert CliRunner().invoke(cli, ["mlsystem", "remove", "7"]).exit_code == 0
    assert removed == [8, 7]
    missing = CliRunner().invoke(cli, ["mlsystem", "remove", "nope"])
    assert missing.exit_code != 0 and "no ML system" in missing.output


def test_register_sends_the_path_and_the_name(monkeypatch):
    client = mock.Mock(_project_id=127)
    client._send_request.return_value = {"id": 7, "name": "Churn", "pathToCode": "/p"}
    monkeypatch.setattr(ml_system_api.client, "_get_instance", lambda: client)
    ml_system_api._register("/Projects/churndemo/Users/u/churn", "Churn")
    method, path = client._send_request.call_args.args
    assert method == "POST" and path == ["project", 127, "mlsystems"]
    body = json.loads(client._send_request.call_args.kwargs["data"])
    assert body == {"pathToCode": "/Projects/churndemo/Users/u/churn", "name": "Churn"}


SPEC = {
    "system": {
        "repo": {
            "url": "https://github.com/o/churn-example",
            "host": "github.com",
            "branch": "hops/churn-example",
        }
    },
    "data": {
        "billing": {
            "connector": {"name": "acme_snowflake", "created": "2026-09-22"},
            "mounted_as": {"feature_group": "billing", "version": 1},
        },
        "shared": {"connector": {"name": "team_s3"}},
        "events": {
            "environment": {"name": "python-feature-pipeline"},
            "writes": {"feature_group": "events", "version": 1},
            "live": {"job": "churn-example-events"},
        },
    },
    "features": {
        "pipelines": [
            {
                "reads": {"feature_group": "telco_customers", "version": 1},
                "writes": {"labels": {"feature_group": "labels", "version": 2}},
                "job": {"name": "churn-example-features"},
            }
        ]
    },
    "training": {
        "environment": {
            "name": "churn-example-train-env",
            "base": "pandas-training-pipeline",
        },
        "feature_view": {"name": "churn_fv", "version": 2},
        "history": [{"feature_view": "churn_fv v1 (root events v1, joined labels v1)"}],
        "runs": [{"model": {"name": "churn_research", "version": 1}}],
        "model": {"name": "churn_model", "version": 1},
    },
    "inference": {
        "realtime": {"deployment": "churnpredictor"},
        "batch": {"writes": {"feature_group": "predictions", "version": 1}},
    },
    "app": {"name": "churn-example-app"},
}


def test_the_inventory_takes_what_the_system_created_downstream_first():
    from hopsworks.cli import teardown

    plan = [str(a) for a in teardown.inventory(SPEC, "churn-example")]
    assert plan == [
        "app churn-example-app",
        "deployment churnpredictor",
        "job churn-example-events",
        "job churn-example-features",
        "model churn_model",
        "model churn_research",
        "feature view churn_fv",
        "feature group predictions v1",
        "feature group labels v2",
        "feature group labels v1",
        "feature group events v1",
        "feature group billing v1",
        "data source acme_snowflake",
        "environment churn-example-train-env",
        "directory Resources/churn-example",
    ]
    # Read, shared or pre-existing: never deleted.
    assert not {"telco_customers", "team_s3", "python-feature-pipeline"} & {
        a.name for a in teardown.inventory(SPEC, "churn-example")
    }


@pytest.fixture
def system_dir(tmp_path, monkeypatch):
    import yaml

    home = tmp_path / "Users" / "meb10000"
    (home / "churn-example").mkdir(parents=True)
    (home / "churn-example" / "system.yaml").write_text(yaml.safe_dump(SPEC))
    monkeypatch.setenv("HOPSFS_USER_HOME_DIR", str(home))
    monkeypatch.setattr(ml_system_api, "_list", lambda: SYSTEMS)
    return home / "churn-example"


def test_a_failed_delete_keeps_the_system_registered_and_a_rerun_finishes(
    monkeypatch, logged_in, system_dir
):
    from hopsworks.cli import teardown

    deleted, removed = [], []
    broken = {"feature group labels v2"}

    def delete(self, asset):
        if str(asset) in broken:
            raise RuntimeError("HTTP 500")
        if str(asset) in deleted:
            return "gone"
        deleted.append(str(asset))
        return "deleted"

    monkeypatch.setattr(teardown.Deleter, "delete", delete)
    monkeypatch.setattr(ml_system_api, "_remove", removed.append)
    args = ["mlsystem", "delete", "7", "--assets", "--yes"]
    first = CliRunner().invoke(cli, args)
    assert first.exit_code != 0 and "still registered" in first.output
    assert "feature group labels v1" not in deleted and removed == []
    broken.clear()
    again = CliRunner().invoke(cli, args)
    assert again.exit_code == 0, again.output
    assert "gone  app churn-example-app" in again.output
    assert deleted[-1] == "directory Resources/churn-example" and removed == [7]


def test_metadata_only_needs_no_system_yaml(monkeypatch, logged_in):
    removed = []
    monkeypatch.setattr(ml_system_api, "_list", lambda: SYSTEMS)
    monkeypatch.setattr(ml_system_api, "_remove", removed.append)
    done = CliRunner().invoke(cli, ["mlsystem", "delete", "Recs", "--yes"])
    assert done.exit_code == 0, done.output
    assert removed == [8]


def test_the_repository_goes_only_when_it_is_this_systems_alone(monkeypatch):
    from hopsworks.cli import teardown

    calls = []
    branches = [{"name": "main"}, {"name": "hops/churn-example"}]

    def github(host, method, route):
        calls.append((method, route))
        return (200, branches) if method == "GET" else (204, None)

    monkeypatch.setattr(teardown, "_github", github)
    assert teardown.delete_repo(SPEC, "churn-example", "churndemo") == "deleted"
    assert calls[-1] == ("DELETE", "repos/o/churn-example")

    branches.append({"name": "hops/churn-example-churnfresh"})
    with pytest.raises(RuntimeError, match="other builds"):
        teardown.delete_repo(SPEC, "churn-example", "churndemo")
    shared = {"system": {"repo": {"url": "https://github.com/o/ml-systems"}}}
    with pytest.raises(RuntimeError, match="not named after"):
        teardown.delete_repo(shared, "churn-example", "churndemo")
    assert teardown.delete_repo({}, "churn-example", "churndemo") == "gone"
