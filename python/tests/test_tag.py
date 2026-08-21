#
#   Copyright 2024 Hopsworks AB
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

import json as json_module
from datetime import datetime, timezone

import humps
import pytest
from hsml import tag


class TestTag:
    # from response json

    def test_from_response_json(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["tag"]["get"]["response"]
        json_camelized = humps.camelize(json)

        # Act
        t_list = tag.Tag.from_response_json(json_camelized)

        # Assert
        assert len(t_list) == 1
        t = t_list[0]
        assert t.name == "test_name"
        assert t.value == "test_value"

    def test_from_response_json_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["tag"]["get_empty"]["response"]
        json_camelized = humps.camelize(json)

        # Act
        t_list = tag.Tag.from_response_json(json_camelized)

        # Assert
        assert len(t_list) == 0

    def test_from_response_json_decodes_json_value(self):
        # Arrange
        value = {"owner": "team-a", "cols": [1, 2]}
        response = {
            "count": 1,
            "items": [{"name": "meta", "value": json_module.dumps(value)}],
        }

        # Act
        t_list = tag.Tag.from_response_json(humps.camelize(response))

        # Assert
        assert len(t_list) == 1
        assert t_list[0].value == value

    @pytest.mark.parametrize(
        "stored_value, expected",
        [
            (json_module.dumps(7), 7),
            (json_module.dumps(True), True),
            (json_module.dumps(["a", "b"]), ["a", "b"]),
            (json_module.dumps("quoted"), "quoted"),
            ("plain text", "plain text"),
        ],
    )
    def test_from_response_json_value_decoding(self, stored_value, expected):
        # Arrange
        response = {"count": 1, "items": [{"name": "t", "value": stored_value}]}

        # Act
        t_list = tag.Tag.from_response_json(humps.camelize(response))

        # Assert
        assert t_list[0].value == expected

    # constructor

    def test_constructor(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["tag"]["get"]["response"]["items"][0]
        tag_name = json.pop("name")
        tag_value = json.pop("value")

        # Act
        t = tag.Tag(name=tag_name, value=tag_value, **json)

        # Assert
        assert t.name == "test_name"
        assert t.value == "test_value"

    def test_constructor_none_name_raises_error(self):
        # Act & Assert
        with pytest.raises(ValueError, match="Tag name cannot be None"):
            tag.Tag(name=None, value="test_value")

    def test_constructor_none_value_raises_error(self):
        # Act & Assert
        with pytest.raises(ValueError, match="Tag value cannot be None"):
            tag.Tag(name="test_name", value=None)

    def test_setter_none_name_raises_error(self):
        # Arrange
        t = tag.Tag(name="test_name", value="test_value")

        # Act & Assert
        with pytest.raises(ValueError, match="Tag name cannot be None"):
            t.name = None

    def test_setter_none_value_raises_error(self):
        # Arrange
        t = tag.Tag(name="test_name", value="test_value")

        # Act & Assert
        with pytest.raises(ValueError, match="Tag value cannot be None"):
            t.value = None

    def test_normalize_with_none_name_raises_error(self):
        # Act & Assert
        with pytest.raises(ValueError, match="Tag name cannot be None"):
            tag.Tag._normalize([{"name": None, "value": "test_value"}])

    def test_normalize_with_none_value_raises_error(self):
        # Act & Assert
        with pytest.raises(ValueError, match="Tag value cannot be None"):
            tag.Tag._normalize([{"name": "test_name", "value": None}])

    def test_created_on_defaults_to_none(self):
        # Arrange
        t = tag.Tag(name="test_name", value="test_value")

        # Assert
        assert t.created_on is None

    def test_from_response_json_parses_epoch_millis(self):
        # Arrange
        json_dict = {
            "count": 1,
            "items": [
                {
                    "name": "test_name",
                    "value": "test_value",
                    "createdOn": 1785474813000,
                }
            ],
        }

        # Act
        tags = tag.Tag.from_response_json(json_dict)

        # Assert
        assert tags[0].created_on == datetime(
            2026, 7, 31, 5, 13, 33, tzinfo=timezone.utc
        )

    def test_from_response_json_parses_iso_8601(self):
        # Arrange
        json_dict = {
            "count": 1,
            "items": [
                {
                    "name": "test_name",
                    "value": "test_value",
                    "createdOn": "2026-07-31T05:13:33Z",
                }
            ],
        }

        # Act
        tags = tag.Tag.from_response_json(json_dict)

        # Assert
        assert tags[0].created_on == datetime(
            2026, 7, 31, 5, 13, 33, tzinfo=timezone.utc
        )

    def test_from_response_json_missing_created_on_is_none(self):
        # Arrange
        json_dict = {
            "count": 1,
            "items": [{"name": "test_name", "value": "test_value"}],
        }

        # Act
        tags = tag.Tag.from_response_json(json_dict)

        # Assert
        assert tags[0].created_on is None

    def test_from_response_json_unparseable_created_on_is_none(self):
        # A timestamp we cannot read must not stop the tag from being read.
        # Arrange
        json_dict = {
            "count": 1,
            "items": [
                {
                    "name": "test_name",
                    "value": "test_value",
                    "createdOn": "not a date",
                }
            ],
        }

        # Act
        tags = tag.Tag.from_response_json(json_dict)

        # Assert
        assert tags[0].name == "test_name"
        assert tags[0].created_on is None

    def test_to_dict_omits_created_on(self):
        # It is server-assigned, so sending it back would be meaningless.
        # Arrange
        t = tag.Tag(
            name="test_name",
            value="test_value",
            created_on=datetime(2026, 7, 31, tzinfo=timezone.utc),
        )

        # Act
        result = t.to_dict()

        # Assert
        assert result == {"name": "test_name", "value": "test_value"}


class TestTagSingleObjectResponse:
    def test_from_response_json_single_tag(self):
        # A request naming one tag answers with that tag rather than a collection.
        tags = tag.Tag.from_response_json(
            {
                "name": "owner",
                "value": '{"team": "risk"}',
                "createdOn": 1785736991000,
            }
        )
        assert len(tags) == 1
        assert tags[0].name == "owner"
        assert tags[0].value == {"team": "risk"}
        assert tags[0].created_on is not None

    def test_from_response_json_single_without_value_is_empty(self):
        assert tag.Tag.from_response_json({"name": "owner"}) == []
