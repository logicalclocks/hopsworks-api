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
