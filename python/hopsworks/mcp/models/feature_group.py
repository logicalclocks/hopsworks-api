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

from datetime import date, datetime
from typing import Optional, Union

from pydantic import BaseModel


class FeatureGroup(BaseModel):
    """Model representing a feature group in Hopsworks MCP."""

    id: Optional[int] = None
    name: Optional[str]
    version: Optional[int]
    featurestore_id: Optional[int]
    location: Optional[str]
    event_time: Optional[Union[str, int, date, datetime]] = None
    online_enabled: bool = False
    online_topic_name: Optional[str] = None
    topic_name: Optional[str] = None
    notification_topic_name: Optional[str] = None
    deprecated: bool = False


class FeatureGroups(BaseModel):
    """Model representing a collection of feature groups in Hopsworks MCP."""

    feature_groups: list[FeatureGroup]
    total: int
    offset: int
    limit: int
