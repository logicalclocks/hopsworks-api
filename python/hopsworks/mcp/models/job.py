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

from hopsworks_common.job import Job as BaseJob
from pydantic import BaseModel


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
                submission_time=exec["submission_time"]
                if "submission_time" in exec
                else None,
                stderr_path=exec["stderr_path"] if "stderr_path" in exec else None,
                stdout_path=exec["stdout_path"] if "stdout_path" in exec else None,
                app_id=exec["app_id"] if "app_id" in exec else None,
                hdfs_user=exec["hdfs_user"] if "hdfs_user" in exec else None,
                args=exec["args"] if "args" in exec else None,
                progress=exec["progress"] if "progress" in exec else None,
                user=exec["user"] if "user" in exec else None,
                files_to_remove=exec["files_to_remove"]
                if "files_to_remove" in exec
                else None,
                duration=exec["duration"] if "duration" in exec else None,
                flink_master_url=exec["flink_master_url"]
                if "flink_master_url" in exec
                else None,
                monitoring=exec["monitoring"] if "monitoring" in exec else None,
                type=exec["type"] if "type" in exec else None,
            )
            for exec in job["executions"]
        ],
        type=job["type"],
        job_schedule=job["job_schedule"],
    )
