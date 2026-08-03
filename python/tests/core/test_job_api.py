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

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from hopsworks_common.core.job_api import JobApi


def _patch_client(mocker, send_request_return=None) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 1
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.job_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


def _job() -> SimpleNamespace:
    return SimpleNamespace(name="my_job")


class TestJobApiTags:
    def test_add_tag_puts_json_value_on_job_tags_path(self, mocker):
        # Arrange
        api = JobApi()
        client_instance = _patch_client(mocker)

        # Act
        api._add_tag(_job(), "meta", {"k": "v"})

        # Assert
        call = client_instance._send_request.call_args
        assert call.args[0] == "PUT"
        assert call.args[1] == ["project", 1, "jobs", "my_job", "tags", "meta"]
        assert json.loads(call.kwargs["data"]) == {"k": "v"}

    def test_delete_tag_uses_job_tags_path(self, mocker):
        # Arrange
        api = JobApi()
        client_instance = _patch_client(mocker)

        # Act
        api._delete_tag(_job(), "meta")

        # Assert
        call = client_instance._send_request.call_args
        assert call.args[0] == "DELETE"
        assert call.args[1] == ["project", 1, "jobs", "my_job", "tags", "meta"]

    def test_get_tags_returns_values(self, mocker):
        # Arrange
        api = JobApi()
        _patch_client(
            mocker,
            {
                "count": 1,
                "items": [{"name": "meta", "value": json.dumps({"k": "v"})}],
            },
        )

        # Act
        result = api._get_tags(_job())

        # Assert
        assert result == {"meta": {"k": "v"}}

    def test_get_tag_returns_value(self, mocker):
        # Arrange
        api = JobApi()
        client_instance = _patch_client(
            mocker, {"count": 1, "items": [{"name": "meta", "value": "v"}]}
        )

        # Act
        result = api._get_tag(_job(), "meta")

        # Assert
        assert result == "v"
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == ["project", 1, "jobs", "my_job", "tags", "meta"]

    def test_get_tags_metadata_keeps_tag_objects(self, mocker):
        # Arrange
        api = JobApi()
        _patch_client(
            mocker,
            {
                "count": 1,
                "items": [{"name": "meta", "value": "v", "createdOn": 1785474813000}],
            },
        )

        # Act
        result = api._get_tags_metadata(_job())

        # Assert
        assert result["meta"].created_on == datetime(
            2026, 7, 31, 5, 13, 33, tzinfo=timezone.utc
        )


class TestJobApiProjectScoping:
    def test_paths_default_to_connection_project(self, mocker):
        # Arrange
        api = JobApi()
        client_instance = _patch_client(mocker, {"count": 0, "items": []})

        # Act
        api._get_tags(_job())

        # Assert
        assert client_instance._send_request.call_args.args[1][1] == 1

    def test_explicit_project_id_overrides_connection(self, mocker):
        # A search hit can come from another authorized project; its jobs are
        # not reachable through the login project's paths.
        # Arrange
        api = JobApi(project_id=42)
        client_instance = _patch_client(mocker, {"count": 0, "items": []})

        # Act
        api._get_tags(_job())

        # Assert
        assert client_instance._send_request.call_args.args[1][1] == 42
