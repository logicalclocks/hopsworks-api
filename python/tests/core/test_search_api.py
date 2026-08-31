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
from urllib.parse import quote

import pytest
from hopsworks_common.core.search_api import SearchApi
from hopsworks_common.search_results import (
    DeploymentSearchResult,
    ModelRegistrySearchResult,
    ModelSearchResult,
)


_RESPONSE = {
    "models": [
        {
            "name": "test_model",
            "version": 1,
            "parentProjectId": 1,
            "parentProjectName": "proj",
            "tags": [],
            "highlights": {},
        }
    ],
    "deployments": [
        {
            "name": "test_deployment",
            "version": 1,
            "parentProjectId": 1,
            "parentProjectName": "proj",
            "tags": [],
            "highlights": {},
        }
    ],
    "modelsFrom": 0,
    "modelsTotal": 1,
    "deploymentsFrom": 0,
    "deploymentsTotal": 1,
}


class TestSearchApiModelRegistry:
    def _patch_client(self, mocker):
        instance = mocker.MagicMock()
        instance._project_id = 119
        instance._send_request.return_value = _RESPONSE
        mocker.patch("hopsworks_common.client._get_instance", return_value=instance)
        return instance

    def test_model_registry_builds_project_scoped_request(self, mocker):
        instance = self._patch_client(mocker)

        result = SearchApi().model_registry("fraud", offset=5, limit=20)

        assert isinstance(result, ModelRegistrySearchResult)
        assert len(result.models) == 1
        assert len(result.deployments) == 1
        instance._send_request.assert_called_once()
        args, kwargs = instance._send_request.call_args
        assert args[0] == "GET"
        assert args[1] == ["project", 119, "elastic", "modelregistry"]
        query_params = kwargs["query_params"]
        assert query_params["docType"] == "ALL"
        assert query_params["searchTerm"] == "fraud"
        assert query_params["from"] == 5
        assert query_params["size"] == 20
        assert "keywords" not in query_params

    def test_models_sets_doc_type_and_returns_list(self, mocker):
        instance = self._patch_client(mocker)

        models = SearchApi().models("fraud")

        assert all(isinstance(m, ModelSearchResult) for m in models)
        assert models[0].name == "test_model"
        _, kwargs = instance._send_request.call_args
        assert kwargs["query_params"]["docType"] == "MODEL"

    def test_deployments_sets_doc_type_and_returns_list(self, mocker):
        instance = self._patch_client(mocker)

        deployments = SearchApi().deployments("fraud")

        assert all(isinstance(d, DeploymentSearchResult) for d in deployments)
        assert deployments[0].name == "test_deployment"
        _, kwargs = instance._send_request.call_args
        assert kwargs["query_params"]["docType"] == "DEPLOYMENT"

    def test_tag_filter_is_json_encoded(self, mocker):
        instance = self._patch_client(mocker)

        SearchApi().models(
            tag_filter={"name": "t", "key": "environment", "value": "production"}
        )

        _, kwargs = instance._send_request.call_args
        expected = quote(
            json.dumps([{"name": "t", "key": "environment", "value": "production"}]),
            safe="",
        )
        assert kwargs["query_params"]["tags"] == expected

    def test_requires_search_term_or_tag_filter(self, mocker):
        self._patch_client(mocker)

        with pytest.raises(ValueError, match="search_term or tag_filter"):
            SearchApi().models()
