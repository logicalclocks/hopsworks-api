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

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
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


class TestDatasetApiTags:
    def _patch_client(self, mocker, send_request_return) -> MagicMock:
        client_instance = MagicMock()
        client_instance._project_id = 1
        client_instance._send_request.return_value = send_request_return
        mocker.patch(
            "hopsworks_common.core.dataset_api.client._get_instance",
            return_value=client_instance,
        )
        return client_instance

    @pytest.mark.parametrize("value", [{"a": 1}, 7, ["x"], True, "plain string"])
    def test_get_tags_does_not_double_decode(self, mocker, value):
        # Arrange
        api = DatasetApi()
        response = {
            "count": 1,
            "items": [{"name": "meta", "value": json.dumps(value)}],
        }
        self._patch_client(mocker, response)

        # Act
        result = api.get_tags("/Projects/p/Resources/file")

        # Assert
        assert result == {"meta": value}

    def test_get_tags_metadata_keeps_tag_objects(self, mocker):
        # Arrange
        api = DatasetApi()
        response = {
            "count": 1,
            "items": [{"name": "meta", "value": "v", "createdOn": 1785474813000}],
        }
        client_instance = self._patch_client(mocker, response)

        # Act
        result = api.get_tags_metadata("my_dataset")

        # Assert
        assert result["meta"].value == "v"
        assert result["meta"].created_on == datetime(
            2026, 7, 31, 5, 13, 33, tzinfo=timezone.utc
        )
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == ["project", 1, "dataset", "tags", "all", "my_dataset"]

    def test_get_tag_metadata_by_name_uses_schema_path(self, mocker):
        # Arrange
        api = DatasetApi()
        response = {"count": 1, "items": [{"name": "meta", "value": "v"}]}
        client_instance = self._patch_client(mocker, response)

        # Act
        result = api.get_tag_metadata("my_dataset", "meta")

        # Assert
        assert result.name == "meta"
        path_params = client_instance._send_request.call_args.args[1]
        assert path_params == [
            "project",
            1,
            "dataset",
            "tags",
            "schema",
            "meta",
            "my_dataset",
        ]

    def test_get_tag_metadata_missing_is_none(self, mocker):
        # Arrange
        api = DatasetApi()
        self._patch_client(mocker, {"count": 0, "items": []})

        # Act & Assert
        assert api.get_tag_metadata("my_dataset", "meta") is None


class TestDatasetApiMkdir:
    def _patch_client(self, mocker) -> MagicMock:
        client_instance = MagicMock()
        client_instance._project_id = 1
        client_instance._send_request.return_value = {
            "attributes": {"path": "/Projects/p/my_dataset"}
        }
        mocker.patch(
            "hopsworks_common.core.dataset_api.client._get_instance",
            return_value=client_instance,
        )
        return client_instance

    def test_mkdir_without_tags_sends_no_body(self, mocker):
        # Existing clients and old servers rely on the body staying absent.
        # Arrange
        api = DatasetApi()
        client_instance = self._patch_client(mocker)

        # Act
        result = api.mkdir("my_dataset")

        # Assert
        assert result == "/Projects/p/my_dataset"
        call = client_instance._send_request.call_args
        assert call.args[0] == "POST"
        assert call.kwargs["data"] is None

    def test_mkdir_with_tags_sends_tags_dto_body(self, mocker):
        # Arrange
        api = DatasetApi()
        client_instance = self._patch_client(mocker)

        # Act
        api.mkdir(
            "my_dataset",
            tags=[{"name": "sensitivity", "value": {"level": "internal"}}],
        )

        # Assert
        call = client_instance._send_request.call_args
        body = json.loads(call.kwargs["data"])
        assert body["count"] == 1
        # Non-string values are JSON-serialized the way tag values are elsewhere.
        assert body["items"] == [
            {"name": "sensitivity", "value": json.dumps({"level": "internal"})}
        ]

    def test_mkdir_with_empty_tags_sends_no_body(self, mocker):
        # Arrange
        api = DatasetApi()
        client_instance = self._patch_client(mocker)

        # Act
        api.mkdir("my_dataset", tags=[])

        # Assert
        assert client_instance._send_request.call_args.kwargs["data"] is None
