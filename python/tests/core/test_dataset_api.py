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

import pytest
from unittest.mock import MagicMock

from hopsworks_common.client.exceptions import DatasetException, RestAPIError
from hopsworks_common.core.dataset_api import Chunk, DatasetApi


def _make_rest_api_error(error_code: int, status_code: int = 500) -> RestAPIError:
    """Build a RestAPIError with the given backend error code and HTTP status."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.reason = "Internal Server Error"
    mock_response.content = b"{}"
    mock_response.json.return_value = {
        "errorCode": error_code,
        "errorMsg": "some error",
        "usrMsg": "some user message",
    }
    return RestAPIError("http://localhost/upload", mock_response)


class TestDatasetApiUploadChunk:
    def test_disk_space_error_raises_dataset_exception(self, mocker):
        # Arrange
        api = DatasetApi()
        error = _make_rest_api_error(DatasetApi.DATASET_ERROR_CODE_UPLOAD_DISK_SPACE)
        mocker.patch.object(api, "_upload_request", side_effect=error)
        chunk = Chunk(b"data", 1, "pending")

        # Act & Assert
        with pytest.raises(DatasetException) as exc_info:
            api._upload_chunk({}, "/test/path", "file.txt", chunk, None, 0, 1)

        assert "storage is full" in str(exc_info.value).lower()
        assert chunk.status == "failed"

    def test_disk_space_error_is_not_retried(self, mocker):
        # Disk-space is a permanent error (HTTP 500) — _upload_request should be
        # called exactly once, with no retry attempts.
        api = DatasetApi()
        error = _make_rest_api_error(DatasetApi.DATASET_ERROR_CODE_UPLOAD_DISK_SPACE)
        mock_upload = mocker.patch.object(api, "_upload_request", side_effect=error)
        chunk = Chunk(b"data", 1, "pending")

        with pytest.raises(DatasetException):
            api._upload_chunk({}, "/test/path", "file.txt", chunk, None, 0, 1)

        assert mock_upload.call_count == 1

    def test_other_500_error_raises_rest_api_error(self, mocker):
        # A different 500 error (e.g. generic UPLOAD_ERROR = 110043) must still
        # propagate as RestAPIError so callers can inspect it.
        api = DatasetApi()
        error = _make_rest_api_error(110043)  # UPLOAD_ERROR
        mocker.patch.object(api, "_upload_request", side_effect=error)
        chunk = Chunk(b"data", 1, "pending")

        with pytest.raises(RestAPIError):
            api._upload_chunk({}, "/test/path", "file.txt", chunk, None, 0, 1)

        assert chunk.status == "failed"

    def test_transient_error_is_retried(self, mocker):
        # A non-permanent status (e.g. 503) with retries > 0 should retry
        # before eventually failing.
        api = DatasetApi()
        mock_response = MagicMock()
        mock_response.status_code = 503
        mock_response.reason = "Service Unavailable"
        mock_response.content = b"{}"
        mock_response.json.return_value = {"errorCode": 0, "errorMsg": "unavailable"}
        error = RestAPIError("http://localhost/upload", mock_response)

        mock_upload = mocker.patch.object(api, "_upload_request", side_effect=error)
        chunk = Chunk(b"data", 1, "pending")
        max_retries = 2

        with pytest.raises(RestAPIError):
            api._upload_chunk({}, "/test/path", "file.txt", chunk, None, max_retries, 0)

        # Initial attempt + max_retries retries
        assert mock_upload.call_count == max_retries + 1

    def test_dataset_error_code_upload_disk_space_constant(self):
        # Sanity-check the constant value matches the backend enum:
        # DatasetErrorCode range=110000, UPLOAD_DISK_SPACE_ERROR code=55
        assert DatasetApi.DATASET_ERROR_CODE_UPLOAD_DISK_SPACE == 110055
