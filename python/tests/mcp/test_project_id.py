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
"""MCP tools that take a project name accept its id too."""

import asyncio
from unittest import mock

from hopsworks.mcp.tools.jobs import JobTools
from hopsworks.mcp.tools.project import ProjectTools
from hopsworks_common.connection import Connection


def test_jobs_tool_reaches_a_project_by_numeric_id():
    conn = Connection.__new__(Connection)
    conn._connected = True
    conn._project_api = mock.MagicMock()
    client_instance = mock.MagicMock(_project_name="demo", _project_id="1")
    jobs = mock.MagicMock()
    jobs.__iter__.return_value = iter([])
    conn._project_api._get_project_by_id.return_value.get_job_api.return_value.get_jobs.return_value = jobs
    with (
        mock.patch(
            "hopsworks.mcp.tools.jobs.client._get_connection", return_value=conn
        ),
        mock.patch(
            "hopsworks_common.connection.client._get_instance",
            return_value=client_instance,
        ),
    ):
        asyncio.run(JobTools(mock.MagicMock()).get_jobs("9"))

    conn._project_api._get_project_by_id.assert_called_once_with(9)
    conn._project_api._get_project.assert_not_called()


def test_use_project_logs_in_by_numeric_id():
    conn = mock.MagicMock()
    project = mock.MagicMock(id=9, owner="o", description="d", created="c")
    project.name = "demo"
    with (
        mock.patch(
            "hopsworks.mcp.tools.project.client._get_connection", return_value=conn
        ),
        mock.patch(
            "hopsworks.mcp.tools.project.hopsworks.login", return_value=project
        ) as login,
    ):
        asyncio.run(ProjectTools(mock.MagicMock()).use_project("9"))

    assert login.call_args.kwargs["project_id"] == 9
    assert login.call_args.kwargs["project"] is None
