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
            tag.Tag.normalize([{"name": None, "value": "test_value"}])

    def test_normalize_with_none_value_raises_error(self):
        # Act & Assert
        with pytest.raises(ValueError, match="Tag value cannot be None"):
            tag.Tag.normalize([{"name": "test_name", "value": None}])

    # merge

    def test_merge_no_overlap(self):
        # Arrange
        base = [tag.Tag("a", 1), tag.Tag("b", 2)]
        overrides = [tag.Tag("c", 3)]

        # Act
        result = tag.Tag.merge(base, overrides)

        # Assert
        names = {t.name for t in result}
        assert names == {"a", "b", "c"}

    def test_merge_override_replaces_base_by_name(self):
        # Arrange
        base = [tag.Tag("a", "old"), tag.Tag("b", 2)]
        overrides = [tag.Tag("a", "new")]

        # Act
        result = tag.Tag.merge(base, overrides)

        # Assert
        by_name = {t.name: t for t in result}
        assert by_name["a"].value == "new"
        assert len(result) == 2

    def test_merge_empty_base(self):
        # Arrange
        overrides = [tag.Tag("x", 10)]

        # Act
        result = tag.Tag.merge([], overrides)

        # Assert
        assert len(result) == 1
        assert result[0].name == "x"

    def test_merge_empty_overrides(self):
        # Arrange
        base = [tag.Tag("x", 10)]

        # Act
        result = tag.Tag.merge(base, [])

        # Assert
        assert len(result) == 1
        assert result[0].value == 10

    # diff

    def test_diff_added(self):
        # Arrange
        before = [tag.Tag("a", 1)]
        after = [tag.Tag("a", 1), tag.Tag("b", 2)]

        # Act
        added, removed, changed = tag.Tag.diff(before, after)

        # Assert
        assert len(added) == 1
        assert added[0].name == "b"
        assert removed == []
        assert changed == []

    def test_diff_removed(self):
        # Arrange
        before = [tag.Tag("a", 1), tag.Tag("b", 2)]
        after = [tag.Tag("a", 1)]

        # Act
        added, removed, changed = tag.Tag.diff(before, after)

        # Assert
        assert added == []
        assert len(removed) == 1
        assert removed[0].name == "b"
        assert changed == []

    def test_diff_changed(self):
        # Arrange
        before = [tag.Tag("a", "old")]
        after = [tag.Tag("a", "new")]

        # Act
        added, removed, changed = tag.Tag.diff(before, after)

        # Assert
        assert added == []
        assert removed == []
        assert len(changed) == 1
        assert changed[0].value == "new"

    def test_diff_no_change(self):
        # Arrange
        before = [tag.Tag("a", 1)]
        after = [tag.Tag("a", 1)]

        # Act
        added, removed, changed = tag.Tag.diff(before, after)

        # Assert
        assert added == []
        assert removed == []
        assert changed == []
