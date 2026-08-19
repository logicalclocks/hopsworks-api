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
from hopsworks_common.core.tag_schemas_api import TagSchemasApi


SCHEMA = {
    "type": "object",
    "properties": {"owner": {"type": "string"}},
    "required": ["owner"],
}


def _patch_client(mocker) -> MagicMock:
    client_instance = MagicMock()
    client_instance._send_request.return_value = {"name": "ownership"}
    mocker.patch(
        "hopsworks_common.core.tag_schemas_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestTagSchemasApiArchive:
    def test_create_defaults_archive_to_false(self, mocker):
        # Arrange
        c = _patch_client(mocker)

        # Act
        TagSchemasApi().create("ownership", SCHEMA)

        # Assert
        assert c._send_request.call_args.kwargs["query_params"] == {
            "name": "ownership",
            "archive": False,
        }

    def test_create_sends_archive_when_requested(self, mocker):
        # Arrange
        c = _patch_client(mocker)

        # Act
        TagSchemasApi().create("ownership", SCHEMA, archive=True)

        # Assert
        assert c._send_request.call_args.kwargs["query_params"]["archive"] is True

    # The flag rides beside the name; it must not disturb the body, which is the schema itself.
    @pytest.mark.parametrize("archive", [True, False])
    def test_archive_does_not_change_the_body(self, mocker, archive):
        # Arrange
        c = _patch_client(mocker)

        # Act
        TagSchemasApi().create("ownership", SCHEMA, archive=archive)

        # Assert
        assert json.loads(c._send_request.call_args.kwargs["data"]) == SCHEMA

    def test_archive_accepted_with_a_json_string_schema_too(self, mocker):
        # Arrange
        c = _patch_client(mocker)

        # Act
        TagSchemasApi().create("ownership", json.dumps(SCHEMA), archive=True)

        # Assert
        assert c._send_request.call_args.kwargs["query_params"]["archive"] is True
        assert json.loads(c._send_request.call_args.kwargs["data"]) == SCHEMA

    # Validation runs before the request, so a bad call cannot reach the backend carrying archive.
    @pytest.mark.parametrize("bad", ["", None])
    def test_empty_name_still_refused_with_archive_set(self, mocker, bad):
        # Arrange
        c = _patch_client(mocker)

        # Act / Assert
        with pytest.raises(ValueError):
            TagSchemasApi().create(bad, SCHEMA, archive=True)
        c._send_request.assert_not_called()
