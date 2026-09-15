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
        truncated: The read stopped at the backend's byte budget, so unread
            lines remain. A reader resuming by cursor needs this: without it
            a response that could not clear its own overlap is
            indistinguishable from one that genuinely had nothing new.
        skipped: The per-request replica cap left this instance unread.
            ``content`` is a human note, not log lines.
        read_failed: The kubelet read failed for this instance. ``content`` is
            a human note, not log lines.
        pod_uid: Identity of the pod the lines came from.
        restart_count: Container restart count. With ``pod_uid`` this
            identifies the container instance, so a cursor is never carried
            across a restart into a different log.
    """

    def __init__(
        self,
        instance_name: str,
        content: str,
        timestamp: "str | None" = None,
        doc_id: "str | None" = None,
        truncated: bool = False,
        skipped: bool = False,
        read_failed: bool = False,
        pod_uid: "str | None" = None,
        restart_count: "int | None" = None,
        **kwargs,
    ):
        self._instance_name = instance_name
        self._content = content
        self._timestamp = timestamp
        self._doc_id = doc_id
        self._truncated = bool(truncated)
        self._skipped = bool(skipped)
        self._read_failed = bool(read_failed)
        self._pod_uid = pod_uid
        self._restart_count = restart_count
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
        instance_name = util._extract_field_from_json(json_decamelized, "instance_name")
        content = util._extract_field_from_json(json_decamelized, "content")
        # ``timestamp`` and ``doc_id`` are missing on the legacy Kubernetes
        # source response. ``extract_field_from_json`` already returns
        # ``None`` for absent keys so this stays back-compat with old
        # backends.
        timestamp = util._extract_field_from_json(json_decamelized, "timestamp")
        doc_id = util._extract_field_from_json(json_decamelized, "doc_id")
        # Absent on old backends, which is why each defaults to a falsy value
        # rather than being required.
        truncated = util._extract_field_from_json(json_decamelized, "truncated")
        skipped = util._extract_field_from_json(json_decamelized, "skipped")
        read_failed = util._extract_field_from_json(json_decamelized, "read_failed")
        pod_uid = util._extract_field_from_json(json_decamelized, "pod_uid")
        restart_count = util._extract_field_from_json(json_decamelized, "restart_count")
        return (
            instance_name,
            content,
            timestamp,
            doc_id,
            bool(truncated),
            bool(skipped),
            bool(read_failed),
            pod_uid,
            restart_count,
        )

    def to_dict(self):
        return {
            "instance_name": self._instance_name,
            "content": self._content,
            "timestamp": self._timestamp,
            "doc_id": self._doc_id,
            "truncated": self._truncated,
            "skipped": self._skipped,
            "read_failed": self._read_failed,
            "pod_uid": self._pod_uid,
            "restart_count": self._restart_count,
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
    def truncated(self):
        """Whether the read stopped at the backend's byte budget."""
        return self._truncated

    @property
    def skipped(self):
        """Whether the per-request replica cap left this instance unread."""
        return self._skipped

    @property
    def read_failed(self):
        """Whether the kubelet read failed for this instance."""
        return self._read_failed

    @property
    def pod_uid(self):
        """Uid of the pod these lines came from."""
        return self._pod_uid

    @property
    def restart_count(self):
        """Restart count of the container instance these lines came from."""
        return self._restart_count

    @property
    def instance_key(self):
        """Identity of the container instance, for per-instance cursoring.

        A pod name alone is not the instance: a restarted container starts a
        new log from zero, and resuming it from the dead instance's cursor
        would silently skip everything the new one printed.
        """
        return (self._instance_name, self._pod_uid, self._restart_count)

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
