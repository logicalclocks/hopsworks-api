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

import humps
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
