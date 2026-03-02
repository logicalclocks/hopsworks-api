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

from datetime import date, datetime  # noqa: TC003

from pydantic import BaseModel


class FeatureGroup(BaseModel):
    id: int
    name: str
    version: int
    description: str | None = None
    location: str | None = None
    event_time: str | int | date | datetime | None = None
    online_enabled: bool | None = None
    topic_name: str | None = None
    notification_topic_name: str | None = None
    deprecated: bool | None = None


class Feature(BaseModel):
    name: str
    type: str | None = None
    description: str | None = None
    primary: bool | None = None
    event_time: bool | None = None
