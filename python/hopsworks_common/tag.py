#
#   Copyright 2020 Logical Clocks AB
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

from __future__ import annotations

import contextlib
import json
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util


@public("hopsworks.tag.Tag", "hsfs.tag.Tag", "hsml.tag.Tag")
class Tag:
    """Represents a tag in Hopsworks.

    Each tag is a name-value pair, where the name is a string and the value is a JSON-serializable object of the tag's schema.
    Tags are used to attach metadata to various entities in Hopsworks; namely feature groups, feature views, training datasets, and models.
    """

    NOT_FOUND_ERROR_CODE = 370002

    def __init__(
        self,
        name: str,
        value: dict[str, Any] | str,
        schema=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        **kwargs,
    ):
        self._name = name
        self._value = value

    def to_dict(self):
        # Backend expects value to always be a string
        # If value is a dict, serialize it to JSON string
        value = self._value
        if isinstance(value, dict):
            value = json.dumps(value)
        return {
            "name": self._name,
            "value": value,
        }

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    @staticmethod
    def normalize(
        tags: Tag | dict[str, Any] | list[Tag | dict[str, Any]] | None,
    ) -> list[Tag]:
        """Normalize tags input to a list of Tag objects.

        Parameters:
            tags: Tags in various formats (single Tag, dict, or list of Tags/dicts), or None.

        Returns:
            List of Tag objects.
        """
        normalized_tags = []
        if tags is not None:
            if isinstance(tags, list):
                for t in tags:
                    if isinstance(t, Tag):
                        normalized_tags.append(t)
                    elif isinstance(t, dict):
                        normalized_tags.append(Tag(**t))
            elif isinstance(tags, Tag):
                normalized_tags.append(tags)
            elif isinstance(tags, dict):
                normalized_tags.append(Tag(**tags))
        return normalized_tags

    @staticmethod
    def tags_to_dict(tags: list[Tag] | None) -> dict[str, Any] | None:
        """Convert a list of tags to API format with count and items.

        Parameters:
            tags: List of Tag objects, or None.

        Returns:
            Dictionary with count and items, or None if no tags.
        """
        if not tags:
            return None
        return {"count": len(tags), "items": [t.to_dict() for t in tags]}

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized or json_decamelized["count"] == 0:
            return []
        tags = []
        for tag_dict in json_decamelized["items"]:
            # Skip tags that don't have name and value (e.g., schema-only entries)
            if "name" not in tag_dict or "value" not in tag_dict:
                continue

            # Try to deserialize value from JSON string if it's a string
            if isinstance(tag_dict["value"], str):
                with contextlib.suppress(json.JSONDecodeError, ValueError):
                    tag_dict["value"] = json.loads(tag_dict["value"])

            # Only pass name and value to avoid issues with extra fields like schema
            tags.append(cls(name=tag_dict["name"], value=tag_dict["value"]))
        return tags

    @property
    def name(self) -> str:
        """Name of the tag."""
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @property
    def value(self) -> Any:
        """Value of the tag."""
        return self._value

    @value.setter
    def value(self, value: Any):
        self._value = value

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Tag({self._name!r}, {self._value!r})"
