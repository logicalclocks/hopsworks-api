#
#   Copyright 2025 Hopsworks AB
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

from typing import TYPE_CHECKING

from pydantic import BaseModel


if TYPE_CHECKING:
    from datetime import date, datetime


class FeatureGroup(BaseModel):
    """Model representing a feature group in Hopsworks MCP."""

    id: int | None = None
    name: str | None
    version: int | None
    featurestore_id: int | None
    location: str | None
    event_time: str | int | date | datetime | None = None
    online_enabled: bool = False
    online_topic_name: str | None = None
    topic_name: str | None = None
    notification_topic_name: str | None = None
    deprecated: bool = False


class FeatureGroups(BaseModel):
    """Model representing a collection of feature groups in Hopsworks MCP."""

    feature_groups: list[FeatureGroup]
    total: int
    offset: int
    limit: int
