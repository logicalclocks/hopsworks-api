#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Project lookups by id, so two live projects can share a display name."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import hopsworks
import pytest
from hopsworks_common import connection as connection_mod
from hopsworks_common.client.exceptions import (
    ModelRegistryException,
    ProjectException,
    RestAPIError,
)
from hopsworks_common.client.external import Client as ExternalClient
from hopsworks_common.core import project_api, secret_api
from hopsworks_common.core.costs_api import CostsApi
from hopsworks_common.core.dataset_api import DatasetApi
from hopsworks_common.core.project_api import ProjectApi
from hopsworks_common.project import Project
from hsfs.core import share_api
from hsml.core.model_registry_api import ModelRegistryApi


def _conflict(usr_msg="several projects named demo: demo (7), demo (9)"):
    response = MagicMock()
    response.status_code = 409
    response.json.return_value = {"errorCode": 150124, "usrMsg": usr_msg}
    return RestAPIError("url", response)


def _project(project_id=7, name="demo"):
    project = Project.__new__(Project)
    project._id = project_id
    project._name = name
    return project


class TestProjectApi:
    @pytest.fixture
    def client(self, mocker):
        client_instance = MagicMock()
        mocker.patch(
            "hopsworks_common.core.project_api.client._get_instance",
            return_value=client_instance,
        )
        mocker.patch.object(
            project_api.project.Project,
            "from_response_json",
            side_effect=lambda j: SimpleNamespace(json=j, get_url=lambda: "url"),
        )
        return client_instance

    def test_get_project_by_id_asks_the_id_path(self, client):
        client._send_request.return_value = {"projectId": 9}

        assert ProjectApi()._get_project_by_id(9).json == {"projectId": 9}

        client._send_request.assert_called_once_with("GET", ["project", 9])

    def test_projects_are_fetched_by_the_rows_id(self, client):
        teams = [
            {
                "user": {"uid": 1},
                "project": {"id": 7, "name": "demo", "owner": {"uid": 1}},
            },
            {
                "user": {"uid": 1},
                "project": {"id": 9, "name": "demo", "owner": {"uid": 1}},
            },
        ]
        client._send_request.side_effect = lambda method, path, **kw: (
            teams if path == ["project"] else {"fetched": path}
        )

        api = ProjectApi()
        fetched = [p.json["fetched"] for p in api._get_projects()]
        owned = [p.json["fetched"] for p in api._get_owned_projects()]

        assert fetched == owned == [["project", 7], ["project", 9]]

    def test_create_fetches_the_created_project_by_id(self, client):
        client._send_request.side_effect = lambda method, path, **kw: (
            {"data": 9} if method == "POST" else {"fetched": path}
        )
        sent = client._send_request

        created = ProjectApi()._create_project("demo")

        assert created.json == {"fetched": ["project", 9]}
        assert sent.call_args.args == ("GET", ["project", 9])

    def test_exists_is_true_when_the_name_is_ambiguous(self, client):
        client._send_request.side_effect = _conflict()

        assert ProjectApi()._exists("demo") is True

    def test_exists_is_false_when_the_project_is_missing(self, client):
        response = MagicMock()
        response.status_code = 404
        client._send_request.side_effect = RestAPIError("url", response)

        assert ProjectApi()._exists("demo") is False


class TestConnectionGetProject:
    @pytest.fixture
    def connection(self, mocker):
        conn = connection_mod.Connection.__new__(connection_mod.Connection)
        conn._connected = True
        conn._project_api = MagicMock()
        mocker.patch.object(connection_mod.Connection, "_provide_project")
        return conn

    def _client(self, mocker, **attrs):
        client_instance = MagicMock(**attrs)
        mocker.patch(
            "hopsworks_common.connection.client._get_instance",
            return_value=client_instance,
        )
        return client_instance

    def test_the_connected_project_is_fetched_by_its_id(self, mocker, connection):
        self._client(mocker, _project_name="demo", _project_id="7")

        connection._get_project()

        connection._project_api._get_project_by_id.assert_called_once_with("7")
        connection._project_api._get_project.assert_not_called()

    def test_name_is_the_fallback_without_an_id(self, mocker, connection):
        self._client(mocker, _project_name="demo", _project_id=None)

        connection._get_project()

        connection._project_api._get_project.assert_called_once_with("demo")

    def test_an_id_wins_over_a_name(self, mocker, connection):
        self._client(mocker, _project_name="demo", _project_id="7")

        connection._get_project("other", project_id=9)

        connection._project_api._get_project_by_id.assert_called_once_with(9)

    def test_a_numeric_name_is_an_id(self, mocker, connection):
        self._client(mocker, _project_name="demo", _project_id="7")

        connection._get_project("9")

        connection._project_api._get_project_by_id.assert_called_once_with(9)


class TestLoginByIdAndConflict:
    def test_project_id_fetches_by_id(self):
        connection = MagicMock()

        hopsworks._prompt_project(connection, None, False, project_id=9)

        connection._project_api._get_project_by_id.assert_called_once_with(9)
        connection._project_api._get_project.assert_not_called()

    def test_unknown_project_id_is_a_project_exception(self):
        connection = MagicMock()
        connection._project_api._get_project_by_id.side_effect = _conflict()

        with pytest.raises(ProjectException, match="id 9"):
            hopsworks._prompt_project(connection, None, False, project_id=9)

    def test_ambiguous_name_reaches_the_user_with_the_ids(self):
        connection = MagicMock()
        connection._project_api._get_project.side_effect = _conflict()

        with pytest.raises(ProjectException, match=r"demo \(7\), demo \(9\)"):
            hopsworks._prompt_project(connection, "demo", False)

    def test_missing_name_stays_not_found(self):
        connection = MagicMock()
        response = MagicMock()
        response.status_code = 404
        connection._project_api._get_project.side_effect = RestAPIError("u", response)

        with pytest.raises(ProjectException, match="Could not find project demo"):
            hopsworks._prompt_project(connection, "demo", False)


class TestCertFolder:
    def test_the_cert_dir_is_keyed_by_the_backing_name(self):
        client = ExternalClient.__new__(ExternalClient)
        client._cert_folder_base = "/tmp"
        client._host = "h"
        client._project_name = "demo"
        client._project_backing_name = "demo_x1"
        client._username = "u"

        assert client._get_certs_folder() == "/tmp/h/demo_x1/u"

    def test_the_cert_dir_falls_back_to_the_name(self):
        client = ExternalClient.__new__(ExternalClient)
        client._cert_folder_base = "/tmp"
        client._host = "h"
        client._project_name = "demo"
        client._project_backing_name = None
        client._username = "u"

        assert client._get_certs_folder() == "/tmp/h/demo/u"


class TestModelRegistryById:
    @pytest.fixture
    def api(self, mocker):
        client_instance = MagicMock()
        client_instance._project_id = 1
        client_instance._project_name = "mine"
        client_instance._send_request.return_value = {
            "items": [
                {"id": 7, "name": "demo", "modelsPath": "/Projects/demo_a/Models"},
                {"id": 9, "name": "demo", "modelsPath": "/Projects/demo_b/Models"},
            ]
        }
        mocker.patch(
            "hsml.core.model_registry_api.client._get_instance",
            return_value=client_instance,
        )
        mocker.patch("hsml.core.model_registry_api.dataset_api.DatasetApi")
        return ModelRegistryApi()

    def test_the_registry_is_matched_by_id(self, api):
        registry = api._get("demo", project_id=7)

        assert registry._model_registry_id == 7
        assert registry._models_path == "/Projects/demo_a/Models"

    def test_a_shared_name_without_an_id_is_refused_not_last_wins(self, api):
        with pytest.raises(ModelRegistryException, match=r"\[7, 9\]"):
            api._get("demo")


class TestShareByProject:
    def test_feature_store_share_uses_the_project_objects_id(self, mocker):
        lookup = mocker.patch.object(project_api.ProjectApi, "_get_project")

        assert share_api._resolve_target_project_id(_project(9)) == 9
        lookup.assert_not_called()

    def test_dataset_share_sends_the_target_id_beside_the_name(self, mocker):
        client_instance = MagicMock()
        client_instance._project_id = 1
        mocker.patch(
            "hopsworks_common.core.dataset_api.client._get_instance",
            return_value=client_instance,
        )

        DatasetApi().share("Resources/d", _project(9, "demo"))
        DatasetApi().unshare("Resources/d", _project(9, "demo"))

        share_call, unshare_call = client_instance._send_request.call_args_list
        assert share_call.kwargs["query_params"] == {
            "action": "SHARE",
            "target_project": "demo",
            "target_project_id": 9,
            "permission": "READ_ONLY",
        }
        assert unshare_call.kwargs["query_params"] == {
            "action": "UNSHARE",
            "target_project": "demo",
            "target_project_id": 9,
        }


class TestIdForSecretsAndCosts:
    def test_secret_scope_takes_a_project_id(self, mocker):
        client_instance = MagicMock()
        mocker.patch(
            "hopsworks_common.core.secret_api.client._get_instance",
            return_value=client_instance,
        )
        api = secret_api.SecretsApi()
        api._project_api = MagicMock()
        mocker.patch.object(api, "get_secret")

        api.create_secret("s", "v", project=9)

        api._project_api._get_project.assert_not_called()
        body = json.loads(client_instance._send_request.call_args.kwargs["data"])
        assert body["scope"] == 9

    def test_costs_take_a_project_id(self, mocker):
        client_instance = MagicMock()
        mocker.patch(
            "hopsworks_common.core.costs_api.client._get_instance",
            return_value=client_instance,
        )
        lookup = mocker.patch(
            "hopsworks_common.core.costs_api.project_api.ProjectApi._get_project"
        )
        mocker.patch(
            "hopsworks_common.core.costs_api.NamespaceCosts.from_response_json"
        )

        CostsApi().get_project_costs("week", project=9)

        lookup.assert_not_called()
        assert client_instance._send_request.call_args.args[1][:2] == ["project", 9]


class TestLoginEnvProjectId:
    @pytest.fixture
    def prompt(self, mocker, monkeypatch):
        for key in ("REST_ENDPOINT", "HOPSWORKS_PROJECT", "HOPSWORKS_PROJECT_ID"):
            monkeypatch.delenv(key, raising=False)
        monkeypatch.setattr(hopsworks, "_hw_connection", MagicMock())
        mocker.patch.object(hopsworks, "logout")
        mocker.patch.object(hopsworks, "_set_active_project")
        mocker.patch.object(hopsworks, "_initialize_module_apis")
        return mocker.patch.object(hopsworks, "_prompt_project")

    def _login(self, **kwargs):
        hopsworks.login(host="h", api_key_value="k", **kwargs)

    def test_env_project_id_is_used_without_arguments(self, prompt, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_PROJECT_ID", "9")
        monkeypatch.setenv("HOPSWORKS_PROJECT", "demo")

        self._login()

        assert prompt.call_args.args[1:] == (None, False, 9)

    def test_name_argument_beats_env_project_id(self, prompt, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_PROJECT_ID", "9")

        self._login(project="other")

        assert prompt.call_args.args[1:] == ("other", False, None)

    def test_name_env_is_the_fallback_without_an_id(self, prompt, monkeypatch):
        monkeypatch.setenv("HOPSWORKS_PROJECT", "demo")

        self._login()

        assert prompt.call_args.args[1:] == ("demo", False, None)
