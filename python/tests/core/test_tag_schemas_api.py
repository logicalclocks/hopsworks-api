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
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core.tag_schemas_api import TagSchemasApi


def _patch_client(mocker, send_request_return=None) -> MagicMock:
    client_instance = MagicMock()
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.tag_schemas_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


def _forbidden() -> RestAPIError:
    mock_response = MagicMock()
    mock_response.status_code = 403
    mock_response.json.return_value = {"errorCode": 200003}
    return RestAPIError("http://localhost/tags", mock_response)


class TestTagSchemasApiLifecycle:
    def test_usage_uses_usage_path(self, mocker):
        # Arrange
        client_instance = _patch_client(mocker, {"total": 3})

        # Act
        result = TagSchemasApi().usage("schema1")

        # Assert
        assert result == {"total": 3}
        call = client_instance._send_request.call_args
        assert call.args[0] == "GET"
        assert call.args[1] == ["tags", "schema1", "usage"]

    def test_deprecate_posts_deprecation(self, mocker):
        # Arrange
        client_instance = _patch_client(mocker, {"deprecated": True})

        # Act
        result = TagSchemasApi().deprecate("schema1")

        # Assert
        assert result == {"deprecated": True}
        call = client_instance._send_request.call_args
        assert call.args[0] == "POST"
        assert call.args[1] == ["tags", "schema1", "deprecation"]

    def test_restore_deletes_deprecation(self, mocker):
        # Arrange
        client_instance = _patch_client(mocker, {"deprecated": False})

        # Act
        result = TagSchemasApi().restore("schema1")

        # Assert
        assert result == {"deprecated": False}
        call = client_instance._send_request.call_args
        assert call.args[0] == "DELETE"
        assert call.args[1] == ["tags", "schema1", "deprecation"]

    def test_delete_without_force_sends_no_query(self, mocker):
        # Arrange
        client_instance = _patch_client(mocker)

        # Act
        TagSchemasApi().delete("schema1")

        # Assert
        call = client_instance._send_request.call_args
        assert call.args[1] == ["tags", "schema1"]
        assert call.kwargs["query_params"] is None

    def test_delete_with_force_sends_force_query(self, mocker):
        # Arrange
        client_instance = _patch_client(mocker)

        # Act
        TagSchemasApi().delete("schema1", force=True)

        # Assert
        call = client_instance._send_request.call_args
        assert call.kwargs["query_params"] == {"force": "true"}

    @pytest.mark.parametrize("method", ["usage", "deprecate", "restore"])
    def test_403_surfaces_as_permission_error(self, mocker, method):
        # Arrange
        client_instance = _patch_client(mocker)
        client_instance._send_request.side_effect = _forbidden()

        # Act & Assert
        with pytest.raises(PermissionError, match="HOPS_ADMIN"):
            getattr(TagSchemasApi(), method)("schema1")

    def test_delete_403_surfaces_as_permission_error(self, mocker):
        # Arrange
        client_instance = _patch_client(mocker)
        client_instance._send_request.side_effect = _forbidden()

        # Act & Assert
        with pytest.raises(PermissionError, match="HOPS_ADMIN"):
            TagSchemasApi().delete("schema1", force=True)

    @pytest.mark.parametrize("method", ["usage", "deprecate", "restore", "delete"])
    def test_empty_name_raises_value_error(self, mocker, method):
        _patch_client(mocker)

        with pytest.raises(ValueError):
            getattr(TagSchemasApi(), method)("")
