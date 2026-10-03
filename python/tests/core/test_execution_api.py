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

from unittest.mock import Mock

import pytest
from hopsworks_common.core.execution_api import ExecutionApi
from hopsworks_common.core.job_api import JobApi


class TestExecutionApiStart:
    @pytest.fixture
    def mock_client(self, mocker):
        client_mock = Mock()
        client_mock._project_id = 42
        client_mock._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.execution_api.client.get_instance",
            return_value=client_mock,
        )
        return client_mock

    @pytest.fixture
    def mock_from_response(self, mocker):
        return mocker.patch(
            "hopsworks_common.core.execution_api.execution.Execution.from_response_json",
            return_value=Mock(),
        )

    @pytest.fixture
    def job(self):
        j = Mock()
        j.name = "my_job"
        return j

    def test_start_sets_text_plain_content_type(
        self, mock_client, mock_from_response, job
    ):
        # The 5.0 backend has two @POST handlers on /executions (text/plain and
        # application/json); without an explicit Content-Type Jersey can't
        # dispatch and returns 415. Lock in that the args body sends text/plain.
        ExecutionApi()._start(job, args="--flag value")

        mock_client._send_request.assert_called_once_with(
            "POST",
            ["project", 42, "jobs", "my_job", "executions"],
            headers={"content-type": "text/plain"},
            data="--flag value",
        )

    def test_start_without_args_sets_text_plain_content_type(
        self, mock_client, mock_from_response, job
    ):
        # Without args the backend falls back to the job's default args; the
        # header must still be sent so the request routes to the legacy handler.
        ExecutionApi()._start(job)

        mock_client._send_request.assert_called_once_with(
            "POST",
            ["project", 42, "jobs", "my_job", "executions"],
            headers={"content-type": "text/plain"},
            data=None,
        )


class TestJobApiLaunch:
    @pytest.fixture
    def mock_client(self, mocker):
        client_mock = Mock()
        client_mock._project_id = 42
        client_mock._send_request.return_value = {}
        mocker.patch(
            "hopsworks_common.core.job_api.client.get_instance",
            return_value=client_mock,
        )
        return client_mock

    def test_launch_sets_text_plain_content_type(self, mock_client):
        # Same dual-@POST dispatch issue as ExecutionApi._start: without an
        # explicit Content-Type Jersey can't pick between the text/plain and
        # application/json handlers and returns 415.
        JobApi().launch("my_job", args="--flag value")

        mock_client._send_request.assert_called_once_with(
            "POST",
            ["project", 42, "jobs", "my_job", "executions"],
            headers={"content-type": "text/plain"},
            data="--flag value",
        )
