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

import humps
from hopsworks_apigen import public


@public("hopsworks.core.ExecutionPodLog")
class ExecutionPodLog:
    """The live log of an execution's pod, or of one table's pod in a multi-table ingestion.

    Returned by [`Execution.get_pod_logs`][hopsworks.execution.Execution.get_pod_logs].
    Unlike the archived stdout/stderr of
    [`Execution.download_logs`][hopsworks.execution.Execution.download_logs], this reads
    the running pod directly, so it reflects a table that is still in progress.
    """

    def __init__(
        self,
        status: str | None = None,
        available: bool | None = None,
        message: str | None = None,
        log: str | None = None,
        pod_name: str | None = None,
        container_name: str | None = None,
        **kwargs,
    ):
        self._status = status
        self._available = available
        self._message = message
        self._log = log
        self._pod_name = pod_name
        self._container_name = container_name

    @classmethod
    def from_response_json(cls, json_dict: dict) -> ExecutionPodLog:
        """Build an ExecutionPodLog from a backend response.

        Parameters:
            json_dict: The JSON dictionary from the API response.

        Returns:
            The parsed pod log.
        """
        return cls(**humps.decamelize(json_dict))

    @public
    @property
    def status(self) -> str | None:
        """Why the log is or is not available.

        One of `AVAILABLE`, `WAITING_FOR_POD`, `MULTIPLE_ACTIVE_PODS`,
        `UNSUPPORTED_WORKLOAD`, or `LOGS_UNAVAILABLE`.
        """
        return self._status

    @public
    @property
    def available(self) -> bool | None:
        """Whether the log content is available."""
        return self._available

    @public
    @property
    def message(self) -> str | None:
        """A human-readable explanation when the log is not available."""
        return self._message

    @public
    @property
    def log(self) -> str | None:
        """The tail of the pod log, or `None` when it is not available."""
        return self._log

    @public
    @property
    def pod_name(self) -> str | None:
        """Name of the pod the log was read from."""
        return self._pod_name

    @public
    @property
    def container_name(self) -> str | None:
        """Name of the container the log was read from."""
        return self._container_name

    def __repr__(self) -> str:
        return f"ExecutionPodLog({self._status!r}, available={self._available!r}, pod={self._pod_name!r})"
