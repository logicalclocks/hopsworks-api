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
from hsml.core.model_api import ModelApi


def _tags_response(items: list[tuple[str, str]]) -> dict:
    return {
        "count": len(items),
        "items": [{"name": name, "value": value} for name, value in items],
    }


def _patch_client(mocker, send_request_return) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 1
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hsml.core.model_api.client.get_instance",
        return_value=client_instance,
    )
    return client_instance


def _model() -> SimpleNamespace:
    return SimpleNamespace(model_registry_id=1, id=12)


class TestModelApi:
    def testget_tags_decodes_values_without_double_decode(self, mocker):
        # Arrange
        api = ModelApi()
        value = {"owner": "team-a", "score": 3}
        _patch_client(
            mocker,
            _tags_response([("meta", json.dumps(value)), ("count", json.dumps(9))]),
        )

        # Act
        result = api.get_tags(_model())

        # Assert
        assert result == {"meta": value, "count": 9}

    def test_get_tag_returns_value_for_name(self, mocker):
        # Arrange
        api = ModelApi()
        value = {"owner": "team-a"}
        _patch_client(mocker, _tags_response([("meta", json.dumps(value))]))

        # Act
        result = api.get_tag(_model(), "meta")

        # Assert
        assert result == value

    def test_get_tag_numeric_value(self, mocker):
        # Arrange
        api = ModelApi()
        _patch_client(mocker, _tags_response([("version", json.dumps(7))]))

        # Act
        result = api.get_tag(_model(), "version")

        # Assert
        assert result == 7

    def test_get_tag_absent_name_returns_none(self, mocker):
        # Arrange
        api = ModelApi()
        _patch_client(mocker, {"count": 0, "items": []})

        # Act
        result = api.get_tag(_model(), "missing")

        # Assert
        assert result is None

    @pytest.mark.parametrize("bad_value", [{"a": 1}, 7, ["x"], True])
    def testget_tags_does_not_double_decode(self, mocker, bad_value):
        # Arrange
        api = ModelApi()
        _patch_client(mocker, _tags_response([("t", json.dumps(bad_value))]))

        # Act
        result = api.get_tags(_model())

        # Assert
        assert result == {"t": bad_value}
