#
#   Copyright 2022 Logical Clocks AB
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

from datetime import datetime

import humps
from hopsworks_common import util


class DeployableComponentLogs:
    """Server logs of a deployable component (predictor or transformer).

    Parameters:
        name: Deployment instance name.
        content: Actual logs.
        timestamp: ISO-8601 timestamp of the underlying log line. Populated
            on the OpenSearch ``source`` path; ``None`` for live Kubernetes
            pod-tailing where the line has no canonical timestamp on the
            wire.
        doc_id: OpenSearch document id. Combined with ``timestamp`` it forms
            the dedupe key that :py:meth:`Deployment.tail_logs` uses to
            avoid yielding the same line on overlapping polls.
    """

    def __init__(
        self,
        instance_name: str,
        content: str,
        timestamp: "str | None" = None,
        doc_id: "str | None" = None,
        **kwargs,
    ):
        self._instance_name = instance_name
        self._content = content
        self._timestamp = timestamp
        self._doc_id = doc_id
        self._created_at = datetime.now()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if len(json_decamelized) == 0:
            return []
        return [cls.from_json(logs) for logs in json_decamelized]

    @classmethod
    def from_json(cls, json_decamelized):
        return DeployableComponentLogs(*cls.extract_fields_from_json(json_decamelized))

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        instance_name = util.extract_field_from_json(json_decamelized, "instance_name")
        content = util.extract_field_from_json(json_decamelized, "content")
        # ``timestamp`` and ``doc_id`` are missing on the legacy Kubernetes
        # source response. ``extract_field_from_json`` already returns
        # ``None`` for absent keys so this stays back-compat with old
        # backends.
        timestamp = util.extract_field_from_json(json_decamelized, "timestamp")
        doc_id = util.extract_field_from_json(json_decamelized, "doc_id")
        return instance_name, content, timestamp, doc_id

    def to_dict(self):
        return {
            "instance_name": self._instance_name,
            "content": self._content,
            "timestamp": self._timestamp,
            "doc_id": self._doc_id,
        }

    @property
    def instance_name(self):
        """Name of the deployment instance containing these server logs."""
        return self._instance_name

    @property
    def content(self):
        """Content of the server logs of the current deployment instance."""
        return self._content

    @property
    def created_at(self):
        """Datetime when the current server logs chunk was retrieved."""
        return self._created_at

    @property
    def timestamp(self):
        """ISO-8601 timestamp of the log line (OpenSearch source only)."""
        return self._timestamp

    @property
    def doc_id(self):
        """OpenSearch document id of the log line (OpenSearch source only)."""
        return self._doc_id

    @property
    def component(self):
        """Component of the deployment containing these server logs."""
        return self._component

    @component.setter
    def component(self, component: str):
        self._component = component

    @property
    def tail(self):
        """Number of lines of server logs."""
        return self._tail

    @tail.setter
    def tail(self, tail: int):
        self._tail = tail

    def __repr__(self):
        return f"DeployableComponentLogs(instance_name: {self._instance_name!r}, date: {self._created_at!r}) \n{self._content!s}"
