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
from unittest.mock import Mock

import pytest
from hopsworks_common.core.project_api import ProjectApi


class TestProjectApiCreate:
    @pytest.fixture
    def mock_client(self, mocker):
        client_mock = Mock()
        client_mock._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.project_api.client.get_instance",
            return_value=client_mock,
        )
        return client_mock

    @pytest.fixture
    def api(self, mocker):
        api = ProjectApi()
        # Skip the follow-up GET that fetches the freshly created project; we
        # only care about asserting the POST body here.
        created = Mock()
        created.get_url.return_value = "https://hopsworks.example/p/1"
        mocker.patch.object(api, "_get_project", return_value=created)
        return api

    def test_create_project_omits_namespace_by_default(self, mock_client, api):
        api._create_project("my_project", description="desc", feature_store_topic="t")

        post_call = mock_client._send_request.call_args_list[0]
        assert post_call.args[0] == "POST"
        body = json.loads(post_call.kwargs["data"])
        assert "namespace" not in body
        assert body["projectName"] == "my_project"
        assert body["description"] == "desc"
        assert body["featureStoreTopic"] == "t"

    def test_create_project_forwards_namespace_when_provided(self, mock_client, api):
        api._create_project("my_project", namespace="custom-ns")

        post_call = mock_client._send_request.call_args_list[0]
        body = json.loads(post_call.kwargs["data"])
        assert body["namespace"] == "custom-ns"
