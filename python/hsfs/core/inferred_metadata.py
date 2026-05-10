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
from __future__ import annotations

from typing import Any

import humps
from hopsworks_apigen import public


@public
class InferredFeature:
    """A single feature suggested by `DataSource.infer_metadata`.

    Wraps the LLM-generated rename, type, and description for one source column.
    """

    def __init__(
        self,
        original_name: str | None = None,
        new_name: str | None = None,
        type: str | None = None,
        description: str | None = None,
        **kwargs,
    ):
        self._original_name = original_name
        self._new_name = new_name
        self._type = type
        self._description = description

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> InferredFeature:
        """Build an InferredFeature from a single backend JSON object.

        Parameters:
            json_dict: The (camelCase) JSON dictionary from the API response.

        Returns:
            The created object, or `None` if the input was `None`.
        """
        if json_dict is None:
            return None
        return cls(**humps.decamelize(json_dict))

    @public
    @property
    def original_name(self) -> str | None:
        """The exact column name from the source table."""
        return self._original_name

    @public
    @property
    def new_name(self) -> str | None:
        """The suggested human-readable feature name in snake_case."""
        return self._new_name

    @public
    @property
    def type(self) -> str | None:
        """The suggested Hopsworks data type, e.g. `bigint`, `string`, `timestamp`."""
        return self._type

    @public
    @property
    def description(self) -> str | None:
        """The suggested business-focused description of the feature."""
        return self._description

    def to_dict(self) -> dict[str, Any]:
        """Serialize this InferredFeature back to the camelCase JSON shape.

        Returns:
            A camelCase dictionary matching the backend wire format.
        """
        return {
            "originalName": self._original_name,
            "newName": self._new_name,
            "type": self._type,
            "description": self._description,
        }

    def __repr__(self) -> str:
        return (
            f"InferredFeature(original_name={self._original_name!r}, "
            f"new_name={self._new_name!r}, type={self._type!r})"
        )


@public
class InferredMetadata:
    """The metadata suggestions returned by `DataSource.infer_metadata`.

    Holds per-feature renames, types, and descriptions, plus suggestions for the
    primary key and event-time columns.
    """

    def __init__(
        self,
        features: list[InferredFeature] | list[dict] | None = None,
        suggested_primary_key: list[str] | None = None,
        suggested_event_time: str | None = None,
        **kwargs,
    ):
        if features is None:
            self._features: list[InferredFeature] = []
        else:
            self._features = [
                f if isinstance(f, InferredFeature) else InferredFeature(**f)
                for f in features
            ]
        self._suggested_primary_key = suggested_primary_key or []
        self._suggested_event_time = suggested_event_time

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> InferredMetadata:
        """Build an InferredMetadata from the backend JSON response.

        Parameters:
            json_dict: The (camelCase) JSON dictionary from the API response.

        Returns:
            The created object, or `None` if the input was `None`.
        """
        if json_dict is None:
            return None
        return cls(**humps.decamelize(json_dict))

    @public
    @property
    def features(self) -> list[InferredFeature]:
        """The suggested renames, types, and descriptions, one per source column."""
        return self._features

    @public
    @property
    def suggested_primary_key(self) -> list[str]:
        """Feature names (using `new_name`) suggested for the primary key."""
        return self._suggested_primary_key

    @public
    @property
    def suggested_event_time(self) -> str | None:
        """The feature name (using `new_name`) suggested as the event time, or `None`."""
        return self._suggested_event_time

    def to_dict(self) -> dict[str, Any]:
        """Serialize this InferredMetadata back to the camelCase JSON shape.

        Returns:
            A camelCase dictionary matching the backend wire format.
        """
        return {
            "features": [f.to_dict() for f in self._features],
            "suggestedPrimaryKey": self._suggested_primary_key,
            "suggestedEventTime": self._suggested_event_time,
        }

    def __repr__(self) -> str:
        return (
            f"InferredMetadata(features={len(self._features)}, "
            f"suggested_primary_key={self._suggested_primary_key!r}, "
            f"suggested_event_time={self._suggested_event_time!r})"
        )
