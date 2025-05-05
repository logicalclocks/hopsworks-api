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

import json
from typing import Optional

import humps
from hopsworks_common import util


class AlertRoute:
    def __init__(
        self,
        groupBy=None,
        groupWait=None,
        groupInterval=None,
        repeatInterval=None,
        receiver=None,
        match=None,
        matchRe=None,
        **kwargs,
    ):
        self._groupBy = groupBy
        self._groupWait = groupWait
        self._groupInterval = groupInterval
        self._repeatInterval = repeatInterval
        self._receiver = receiver
        self._match = match
        self._matchRe = matchRe

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if "items" in json_decamelized:
                return [cls(**route) for route in json_decamelized["items"]]
            else:
                return []
        else:
            return cls(**json_decamelized)

    @property
    def groupBy(self) -> Optional[list]:
        """The labels by which incoming alerts are grouped together."""
        return self._groupBy

    @property
    def groupWait(self) -> Optional[str]:
        """How long to initially wait to send a notification for a group."""
        return self._groupWait

    @property
    def groupInterval(self) -> Optional[str]:
        """How long to wait before sending a notification about new alerts that are added to a group."""
        return self._groupInterval

    @property
    def repeatInterval(self) -> Optional[str]:
        """How long to wait before sending a notification again if it has already been sent successfully."""
        return self._repeatInterval

    @property
    def receiver(self) -> Optional[str]:
        """The receiver to send notifications to."""
        return self._receiver

    @property
    def match(self) -> Optional[dict]:
        """A set of equality matchers an alert has to fulfill to match the node."""
        return self._match

    @property
    def matchRe(self) -> Optional[dict]:
        """A set of regex-matchers an alert has to fulfill to match the node."""
        return self._matchRe

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        """return the alert route as a dictionary"""
        dict = {
            "group_by": self._groupBy,
            "group_wait": self._groupWait,
            "group_interval": self._groupInterval,
            "repeat_interval": self._repeatInterval,
            "receiver": self._receiver,
            "match": self._match,
            "match_re": self._matchRe,
        }
        return dict

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"AlertRoute({self._receiver!r}, {self._match!r})"
