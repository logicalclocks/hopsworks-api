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
from datetime import datetime, timezone
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
        created_on: datetime | None = None,
        schema=None,
        href=None,
        expand=None,
        items=None,
        count=None,
        type=None,
        **kwargs,
    ):
        if name is None:
            raise ValueError("Tag name cannot be None")
        if value is None:
            raise ValueError("Tag value cannot be None")
        self._name = name
        self._value = value
        self._created_on = created_on

    def to_dict(self):
        # Backend expects value to always be a string
        # If value is a dict, serialize it to JSON string
        # created_on is server-assigned, so it is deliberately not sent back
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
    def _normalize(
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
    def _tags_to_dict(tags: list[Tag] | None) -> dict[str, Any] | None:
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
        # A request naming one tag answers with that tag, not with a collection: the dataset tag
        # endpoints do this, and reading it as an empty collection lost the tag entirely.
        if "count" not in json_decamelized:
            if "name" in json_decamelized and "value" in json_decamelized:
                json_decamelized = {"count": 1, "items": [json_decamelized]}
            else:
                return []
        if json_decamelized["count"] == 0:
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

            tags.append(
                cls(
                    name=tag_dict["name"],
                    value=tag_dict["value"],
                    created_on=cls._parse_created_on(tag_dict.get("created_on")),
                )
            )
        return tags

    @staticmethod
    def _parse_created_on(raw: Any) -> datetime | None:
        """Parse the attachment time the backend sent, in whichever form it sent it.

        Both epoch milliseconds and ISO-8601 are accepted because the backend's JSON date format
        is not pinned across versions, and a client that only understood one of them would report
        no attachment time at all against the other.
        Anything unrecognized becomes None rather than raising: an unreadable timestamp must not
        stop a tag from being read.
        """
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
        if isinstance(raw, (int, float)):
            return datetime.fromtimestamp(raw / 1000, tz=timezone.utc)
        if isinstance(raw, str):
            with contextlib.suppress(ValueError):
                return datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
            with contextlib.suppress(ValueError):
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

    @public
    @property
    def name(self) -> str:
        """Name of the tag."""
        return self._name

    @name.setter
    def name(self, name: str):
        if name is None:
            raise ValueError("Tag name cannot be None")
        self._name = name

    @public
    @property
    def value(self) -> Any:
        """Value of the tag."""
        return self._value

    @value.setter
    def value(self, value: Any):
        if value is None:
            raise ValueError("Tag value cannot be None")
        self._value = value

    @public
    @property
    def created_on(self) -> datetime | None:
        """When the tag was attached, as an aware UTC datetime.

        `None` for a tag attached before Hopsworks recorded attachment times, and for legacy
        per-file dataset tags, which are stored as HopsFS extended attributes and carry no
        timestamp.
        `None` therefore means the attachment time is unknown, not that the tag is new.
        """
        return self._created_on

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"Tag({self._name!r}, {self._value!r})"
