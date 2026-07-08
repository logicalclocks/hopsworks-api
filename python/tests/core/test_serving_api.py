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
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsml.core.serving_api import ServingApi


def _tags_response(items: list[tuple[str, str]]) -> dict:
    return {
        "count": len(items),
        "items": [{"name": name, "value": value} for name, value in items],
    }


def _patch_client(mocker, send_request_return) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 1
    if isinstance(send_request_return, Exception):
        client_instance._send_request.side_effect = send_request_return
    else:
        client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hsml.core.serving_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


def _deployment() -> SimpleNamespace:
    return SimpleNamespace(id=12)


class TestServingApi:
    def test_get_tag_returns_value_for_name(self, mocker):
        # Arrange
        api = ServingApi()
        value = {"owner": "team-a"}
        _patch_client(mocker, _tags_response([("meta", json.dumps(value))]))

        # Act
        result = api._get_tag(_deployment(), "meta")

        # Assert
        assert result == value

    def test_get_tag_numeric_value(self, mocker):
        # Arrange
        api = ServingApi()
        _patch_client(mocker, _tags_response([("version", json.dumps(7))]))

        # Act
        result = api._get_tag(_deployment(), "version")

        # Assert
        assert result == 7

    def test_get_tag_absent_name_returns_none(self, mocker):
        # Arrange
        api = ServingApi()
        _patch_client(mocker, {"count": 0, "items": []})

        # Act
        result = api._get_tag(_deployment(), "missing")

        # Assert
        assert result is None

    def test_get_tag_not_found_error_returns_none(self, mocker):
        # Arrange
        api = ServingApi()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.json.return_value = {
            "errorCode": 370002,
            "errorMsg": "not found",
            "usrMsg": "not found",
        }
        _patch_client(
            mocker,
            RestAPIError("/project/1/serving/12/tags/missing", mock_response),
        )

        # Act
        result = api._get_tag(_deployment(), "missing")

        # Assert
        assert result is None

    @pytest.mark.parametrize("bad_value", [{"a": 1}, 7, ["x"], True])
    def test_get_tags_does_not_double_decode(self, mocker, bad_value):
        # Arrange
        api = ServingApi()
        _patch_client(mocker, _tags_response([("t", json.dumps(bad_value))]))

        # Act
        result = api._get_tags(_deployment())

        # Assert
        assert result == {"t": bad_value}
