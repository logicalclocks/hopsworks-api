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
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core.dataset_api import DatasetApi


class TestDatasetApiTags:
    def _patch_client(self, mocker, send_request_return) -> MagicMock:
        client_instance = MagicMock()
        client_instance._project_id = 1
        client_instance._send_request.return_value = send_request_return
        mocker.patch(
            "hopsworks_common.core.dataset_api.client._get_instance",
            return_value=client_instance,
        )
        return client_instance

    @pytest.mark.parametrize("value", [{"a": 1}, 7, ["x"], True, "plain string"])
    def test_get_tags_does_not_double_decode(self, mocker, value):
        # Arrange
        api = DatasetApi()
        response = {
            "count": 1,
            "items": [{"name": "meta", "value": json.dumps(value)}],
        }
        self._patch_client(mocker, response)

        # Act
        result = api.get_tags("/Projects/p/Resources/file")

        # Assert
        assert result == {"meta": value}
