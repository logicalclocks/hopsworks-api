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

import humps
from hopsworks_common import util


class AlertStatus:
    def __init__(self, inhibited_by=None, silenced_by=None, state=None, **kwargs):
        self._inhibitedBy = inhibited_by
        self._silencedBy = silenced_by
        self._state = state

    @classmethod
    def from_response_json(cls, json_dict):
        if json_dict:
            json_decamelized = humps.decamelize(json_dict)
            return cls(**json_decamelized)
        else:
            return None

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "inhibitedBy": self._inhibitedBy,
            "silencedBy": self._silencedBy,
            "state": self._state,
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return (
            f"AlertStatus({self._state!r}, {self._inhibitedBy!r}, {self._silencedBy!r})"
        )


class TriggeredAlert:
    def __init__(
        self,
        labels=None,
        annotations=None,
        receivers=None,
        status=None,
        fingerprint=None,
        starts_at=None,
        ends_at=None,
        updated_at=None,
        generatorurl=None,
        **kwargs,
    ):
        self._labels = labels
        self._annotations = annotations
        self._receivers = receivers
        self._fingerprint = fingerprint
        self._starts_at = starts_at
        self._ends_at = ends_at
        self._updated_at = updated_at
        self._generatorurl = generatorurl
        self._status = AlertStatus.from_response_json(status)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if "items" in json_decamelized:
                return [cls(**receiver) for receiver in json_decamelized["items"]]
            else:
                return []
        else:
            return cls(**json_decamelized)

    @property
    def labels(self):
        """return the labels of the alert"""
        return self._labels

    @property
    def annotations(self):
        """return the annotations of the alert"""
        return self._annotations

    @property
    def receivers(self):
        """return the receivers of the alert"""
        return self._receivers

    @property
    def fingerprint(self):
        """return the fingerprint of the alert"""
        return self._fingerprint

    @property
    def starts_at(self):
        """return the start time of the alert"""
        return self._starts_at

    @property
    def ends_at(self):
        """return the end time of the alert"""
        return self._ends_at

    @property
    def updated_at(self):
        """return the update time of the alert"""
        return self._updated_at

    @property
    def generatorurl(self):
        """return the generator URL of the alert"""
        return self._generatorurl

    @property
    def status(self):
        """return the status of the alert"""
        return self._status

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "labels": self._labels,
            "annotations": self._annotations,
            "receivers": self._receivers,
            "fingerprint": self._fingerprint,
            "starts_at": self._starts_at,
            "ends_at": self._ends_at,
            "updated_at": self._updated_at,
            "generatorurl": self._generatorurl,
            "status": self._status.json(),
        }

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"TriggeredAlert({self._labels!r}, {self._annotations!r}, {self._receivers!r}, {self._status!r}, {self._fingerprint!r}, {self._starts_at!r}, {self._ends_at!r}, {self._updated_at!r}, {self._generatorurl!r})"
