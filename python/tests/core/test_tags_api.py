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
from hopsworks_common.core.tags_api import TagsApi


def _tags_response(name: str, value: str) -> dict:
    return {
        "count": 1,
        "items": [{"name": name, "value": value, "schema": "test_schema"}],
    }


def _patch_client(mocker, send_request_return) -> MagicMock:
    client_instance = MagicMock()
    client_instance._project_id = 1
    client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hopsworks_common.core.tags_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


# A feature-group-like metadata object: an id and no `training_data` attribute,
# so TagsApi._get_path takes the featuregroups/trainingdatasets branch.
def _fg_metadata() -> SimpleNamespace:
    return SimpleNamespace(id=14)


class TestTagsApi:
    def test_get_decodes_json_object_value(self, mocker):
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        value = {"owner": "team-a", "pii": True, "cols": [1, 2]}
        _patch_client(mocker, _tags_response("meta", json.dumps(value)))

        # Act
        result = api._get(_fg_metadata())

        # Assert
        assert result == {"meta": value}

    def test_get_decodes_numeric_value(self, mocker):
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(mocker, _tags_response("version", json.dumps(7)))

        # Act
        result = api._get(_fg_metadata())

        # Assert
        assert result == {"version": 7}

    def test_get_preserves_plain_string_value(self, mocker):
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(mocker, _tags_response("note", "just a plain string"))

        # Act
        result = api._get(_fg_metadata())

        # Assert
        assert result == {"note": "just a plain string"}

    def test_get_by_name_appends_name_to_path(self, mocker):
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        client_instance = _patch_client(
            mocker, _tags_response("meta", json.dumps({"k": "v"}))
        )

        # Act
        result = api._get(_fg_metadata(), name="meta")

        # Assert
        assert result == {"meta": {"k": "v"}}
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params[-1] == "meta"

    def test_get_empty_returns_empty_dict(self, mocker):
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(mocker, {"count": 0, "items": []})

        # Act
        result = api._get(_fg_metadata())

        # Assert
        assert result == {}

    def test_get_feature_view_uses_featureview_path(self, mocker):
        # A feature-view-like object has a `training_data` attribute (featureview path).
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        fv_metadata = SimpleNamespace(
            name="my_fv", version=1, training_data=lambda: None
        )
        client_instance = _patch_client(
            mocker, _tags_response("meta", json.dumps({"k": "v"}))
        )

        # Act
        result = api._get(fv_metadata)

        # Assert
        assert result == {"meta": {"k": "v"}}
        path_params = client_instance._send_request.call_args.args[1]
        assert "featureview" in path_params

    @pytest.mark.parametrize("bad_value", [{"a": 1}, 7, ["x"], True])
    def test_get_does_not_double_decode(self, mocker, bad_value):
        # Arrange
        api = TagsApi(feature_store_id=99, entity_type="featuregroups")
        _patch_client(mocker, _tags_response("t", json.dumps(bad_value)))

        # Act
        result = api._get(_fg_metadata())

        # Assert
        assert result == {"t": bad_value}
