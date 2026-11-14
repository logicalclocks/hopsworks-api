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

from unittest.mock import MagicMock

import pytest
from hopsworks_common.core.search_api import SearchApi


def _patch_client(mocker, response) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 119
    client_instance._send_request.return_value = response
    mocker.patch(
        "hopsworks_common.core.search_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestSearchApiDocTypes:
    @pytest.mark.parametrize(
        ("doc_type", "bucket"),
        [
            ("DATASET", "datasets"),
            ("METRIC", "metrics"),
            ("SEMANTIC_PROJECT", "semantic_projects"),
            ("SAVED_QUERY", "saved_queries"),
        ],
    )
    def test_dataset_and_metric_doc_types_reach_the_backend(
        self, mocker, doc_type, bucket
    ):
        # Arrange
        key = {
            "semantic_projects": "semanticProjects",
            "saved_queries": "savedQueries",
        }.get(bucket, bucket)
        c = _patch_client(mocker, {key: [{"name": "hit"}], f"{key}Total": 1})

        # Act
        result = SearchApi()._search(doc_type=doc_type, search_term="hit")

        # Assert
        args, kwargs = c._send_request.call_args
        assert args[1] == ["project", 119, "elastic", "featurestore"]
        assert kwargs["query_params"]["docType"] == doc_type
        assert [item.name for item in getattr(result, bucket)] == ["hit"]
        assert getattr(result, f"{bucket}_total") == 1

    def test_unknown_doc_type_is_rejected_before_any_request(self, mocker):
        # Arrange
        c = _patch_client(mocker, {})

        # Act
        with pytest.raises(ValueError, match="doc_type must be one of"):
            SearchApi()._search(doc_type="SEMANTIC_MODEL", search_term="hit")

        # Assert
        c._send_request.assert_not_called()
