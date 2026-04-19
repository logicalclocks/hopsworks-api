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
#

import json
from datetime import datetime

from hopsworks_apigen import also_available_as
from hopsworks_common import client, execution


@also_available_as("hopsworks.core.execution_api.ExecutionApi")
class ExecutionApi:
    def _start(
        self,
        job,
        args: str = None,
        *,
        start_time: datetime = None,
        end_time: datetime = None,
        logical_date: datetime = None,
        env_vars: dict = None,
    ):
        """Start an execution.

        If any of start_time/end_time/logical_date/env_vars is provided, POSTs a JSON body
        (endpoint accepts `application/json`); otherwise falls back to the original plain-text
        args body for backwards compatibility.
        """
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", job.name, "executions"]

        use_json = (
            start_time is not None
            or end_time is not None
            or logical_date is not None
            or env_vars is not None
        )
        if not use_json:
            return execution.Execution.from_response_json(
                _client._send_request("POST", path_params, data=args), job
            )

        # Merge start/end_time into envVars if supplied separately; the backend treats the
        # run envVars as the source of truth when set (overrides scheduler-provided HOPS_*).
        merged_env = dict(env_vars) if env_vars else {}
        if start_time is not None:
            merged_env.setdefault("HOPS_START_TIME", _to_iso(start_time))
        if end_time is not None:
            merged_env.setdefault("HOPS_END_TIME", _to_iso(end_time))

        body = {}
        if args is not None:
            body["args"] = args
        if merged_env:
            body["envVars"] = merged_env
        if logical_date is not None:
            body["logicalDate"] = _to_iso(logical_date)
        if end_time is not None:
            # data interval end mirrors HOPS_END_TIME for a one-shot backfill run.
            body["dataIntervalEnd"] = _to_iso(end_time)

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "POST", path_params, headers=headers, data=json.dumps(body)
            ),
            job,
        )

    def _get(self, job, id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            job.name,
            "executions",
            id,
        ]

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request("GET", path_params, headers=headers), job
        )

    def _get_all(self, job):
        _client = client.get_instance()
        path_params = ["project", _client._project_id, "jobs", job.name, "executions"]

        query_params = {"sort_by": "submissiontime:desc"}

        headers = {"content-type": "application/json"}
        return execution.Execution.from_response_json(
            _client._send_request(
                "GET", path_params, headers=headers, query_params=query_params
            ),
            job,
        )

    def _delete(self, job_name, id):
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            job_name,
            "executions",
            id,
        ]
        _client._send_request("DELETE", path_params)

    def _stop(self, job_name: str, id: int) -> None:
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "jobs",
            job_name,
            "executions",
            id,
            "status",
        ]
        _client._send_request(
            "PUT",
            path_params=path_params,
            data={"state": "stopped"},
            headers={"Content-Type": "application/json"},
        )


def _to_iso(dt: datetime) -> str:
    """ISO-8601 UTC string, matching what the backend parses into Instant."""
    if dt.tzinfo is None:
        # Treat naive as UTC rather than silently shift by the local offset.
        from datetime import timezone as _tz

        dt = dt.replace(tzinfo=_tz.utc)
    return dt.isoformat()
