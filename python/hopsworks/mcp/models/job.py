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
    from hopsworks_common.job import Job as BaseJob


class Execution(BaseModel):
    id: int
    state: str | None = None
    final_status: str | None = None
    submission_time: str | None = None
    stdout_path: str | None = None
    stderr_path: str | None = None
    app_id: str | None = None
    hdfs_user: str | None = None
    args: list[str] | None = None
    progress: float | None = None
    user: str | None = None
    files_to_remove: list[str] | None = None
    duration: float | None = None
    flink_master_url: str | None = None
    monitoring: dict | None = None
    type: str | None = None


class Job(BaseModel):
    """Model representing a job in Hopsworks MCP."""

    id: int
    name: str
    creation_time: str | None = None
    config: int
    job_type: str
    creator: str
    executions: list[Execution] | None = None
    type: str | None = None
    job_schedule: str | None = None


class Jobs(BaseModel):
    """Model representing a collection of jobs in Hopsworks MCP."""

    jobs: list[Job]
    total: int


def to_base_model_job(job: BaseJob) -> Job:
    """Convert a Job instance to a BaseModel."""
    return Job(
        id=job["id"],
        name=job["name"],
        creation_time=job["creation_time"],
        config=job["config"],
        job_type=job["job_type"],
        creator=job["creator"],
        executions=[
            Execution(
                id=exec["id"],
                state=exec["state"],
                final_status=exec["final_status"],
                submission_time=exec.get("submission_time", None),
                stderr_path=exec.get("stderr_path", None),
                stdout_path=exec.get("stdout_path", None),
                app_id=exec.get("app_id", None),
                hdfs_user=exec.get("hdfs_user", None),
                args=exec.get("args", None),
                progress=exec.get("progress", None),
                user=exec.get("user", None),
                files_to_remove=exec.get("files_to_remove", None),
                duration=exec.get("duration", None),
                flink_master_url=exec.get("flink_master_url", None),
                monitoring=exec.get("monitoring", None),
                type=exec.get("type", None),
            )
            for exec in job["executions"]
        ],
        type=job["type"],
        job_schedule=job["job_schedule"],
    )
