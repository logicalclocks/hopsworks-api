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

from hopsworks_common.core.keywords_api import KeywordsApi


def _patch_client(mocker, send_request_return) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 1
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.keywords_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


# A feature-group-like metadata object: an id and no `training_data` attribute,
# so KeywordsApi._get_path takes the featuregroups/trainingdatasets branch.
def _fg_metadata() -> SimpleNamespace:
    return SimpleNamespace(id=14)


def _fv_metadata() -> SimpleNamespace:
    return SimpleNamespace(name="my_fv", version=1, training_data=lambda: None)


class TestKeywordsApi:
    def test_get_returns_keyword_list(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        client_instance = _patch_client(mocker, {"keywords": ["pii", "prod"]})

        # Act
        result = api._get(_fg_metadata())

        # Assert
        assert result == ["pii", "prod"]
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == [
            "project",
            1,
            "featurestores",
            99,
            "featuregroups",
            14,
            "keywords",
        ]

    def test_get_tolerates_missing_keywords(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(mocker, {})

        # Act & Assert
        assert api._get(_fg_metadata()) == []

    def test_get_with_metadata_parses_created_on(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(
            mocker,
            {
                "keywords": ["pii", "prod"],
                "items": [
                    {"name": "pii", "createdOn": 1785474813000},
                    {"name": "prod", "createdOn": None},
                ],
            },
        )

        # Act
        result = api._get_with_metadata(_fg_metadata())

        # Assert
        assert result == {
            "pii": datetime(2026, 7, 31, 5, 13, 33, tzinfo=timezone.utc),
            "prod": None,
        }

    def test_get_with_metadata_old_server_without_items(self, mocker):
        # An old server sends only the plain keyword list; every keyword maps to None.
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(mocker, {"keywords": ["pii", "prod"]})

        # Act
        result = api._get_with_metadata(_fg_metadata())

        # Assert
        assert result == {"pii": None, "prod": None}

    def test_replace_posts_keyword_dto(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        client_instance = _patch_client(mocker, {"keywords": ["a", "b"]})

        # Act
        result = api._replace(_fg_metadata(), ["a", "b"])

        # Assert
        assert result == ["a", "b"]
        call = client_instance._send_request.call_args
        assert call.args[0] == "POST"
        assert json.loads(call.kwargs["data"]) == {"keywords": ["a", "b"]}

    def test_delete_sends_keyword_query_param(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        client_instance = _patch_client(mocker, {"keywords": ["b"]})

        # Act
        result = api._delete(_fg_metadata(), "a")

        # Assert
        assert result == ["b"]
        call = client_instance._send_request.call_args
        assert call.args[0] == "DELETE"
        assert call.kwargs["query_params"] == {"keyword": "a"}

    def test_get_all_uses_featurestores_keywords_path(self, mocker):
        # Arrange
        api = KeywordsApi()
        client_instance = _patch_client(mocker, {"keywords": ["pii"]})

        # Act
        result = api._get_all()

        # Assert
        assert result == ["pii"]
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == ["project", 1, "featurestores", "keywords"]

    def test_feature_view_path(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        client_instance = _patch_client(mocker, {"keywords": []})

        # Act
        api._get(_fv_metadata())

        # Assert
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == [
            "project",
            1,
            "featurestores",
            99,
            "featureview",
            "my_fv",
            "version",
            1,
            "keywords",
        ]

    def test_feature_view_training_dataset_path(self, mocker):
        # Arrange
        api = KeywordsApi(feature_store_id=99, entity_type="featuregroups")
        client_instance = _patch_client(mocker, {"keywords": []})

        # Act
        api._get(_fv_metadata(), training_dataset_version=7)

        # Assert
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == [
            "project",
            1,
            "featurestores",
            99,
            "featureview",
            "my_fv",
            "version",
            1,
            "trainingdatasets",
            "version",
            7,
            "keywords",
        ]
