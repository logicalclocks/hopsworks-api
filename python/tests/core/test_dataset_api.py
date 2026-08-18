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
from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import DatasetException, RestAPIError
from hopsworks_common.core.dataset_api import Chunk, DatasetApi
from hopsworks_common.user import User


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

    def test_upload_not_allowed_error_names_the_policy(self, mocker):
        # Arrange
        api = DatasetApi()
        error = _make_rest_api_error(
            DatasetApi.DATASET_ERROR_CODE_UPLOAD_NOT_ALLOWED, status_code=403
        )
        mocker.patch.object(api, "_upload_request", side_effect=error)
        chunk = Chunk(b"data", 1, "pending")

        # Act & Assert
        with pytest.raises(DatasetException) as exc_info:
            api._upload_chunk({}, "/test/path", "file.txt", chunk, None, 0, 1)

        assert "not allowed on this cluster" in str(exc_info.value)

    def test_dataset_error_code_upload_not_allowed_constant(self):
        # DatasetErrorCode range=110000, UPLOAD_NOT_ALLOWED code=56
        assert DatasetApi.DATASET_ERROR_CODE_UPLOAD_NOT_ALLOWED == 110056

    def test_overwrite_does_not_remove_when_policy_forbids_upload(
        self, mocker, tmp_path
    ):
        # The whole point of the pre-flight check: a refusal must leave the destination intact.
        api = DatasetApi()
        local_file = tmp_path / "model.pkl"
        local_file.write_bytes(b"payload")

        mocker.patch.object(api, "exists", return_value=True)
        mocker.patch.object(api, "_get", return_value={})
        mock_remove = mocker.patch.object(api, "remove")
        mock_upload_file = mocker.patch.object(api, "_upload_file")
        mocker.patch.object(
            api,
            "_assert_upload_allowed",
            side_effect=DatasetException(
                "Uploading files is not allowed on this cluster"
            ),
        )

        with pytest.raises(DatasetException):
            api.upload(str(local_file), "Resources", overwrite=True)

        mock_remove.assert_not_called()
        mock_upload_file.assert_not_called()

    def test_overwrite_removes_then_uploads_when_policy_allows(self, mocker, tmp_path):
        api = DatasetApi()
        local_file = tmp_path / "model.pkl"
        local_file.write_bytes(b"payload")

        mocker.patch.object(api, "exists", return_value=True)
        mocker.patch.object(api, "_get", return_value={})
        mock_remove = mocker.patch.object(api, "remove")
        mock_upload_file = mocker.patch.object(api, "_upload_file")
        mocker.patch.object(api, "_assert_upload_allowed")

        api.upload(str(local_file), "Resources", overwrite=True)

        mock_remove.assert_called_once()
        mock_upload_file.assert_called_once()

    @pytest.mark.parametrize("policy", ["disabled", "DISABLED", "  Disabled  "])
    def test_assert_upload_allowed_blocks_everyone_when_disabled(self, mocker, policy):
        api = DatasetApi()
        mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_upload_policy",
            return_value=policy,
        )
        mock_profile = mocker.patch(
            "hopsworks_common.core.users_api.UsersApi._get_current_user"
        )

        with pytest.raises(DatasetException) as exc_info:
            api._assert_upload_allowed("Resources")

        assert "left unchanged" in str(exc_info.value)
        # `disabled` applies to everyone, so the caller's role is irrelevant and must not be read.
        mock_profile.assert_not_called()

    def test_assert_upload_allowed_permits_admin_under_admins_only(self, mocker):
        api = DatasetApi()
        mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_upload_policy",
            return_value="admins_only",
        )
        mocker.patch(
            "hopsworks_common.core.users_api.UsersApi._get_current_user",
            return_value=User(roles=["HOPS_ADMIN"]),
        )

        api._assert_upload_allowed("Resources")

    def test_assert_upload_allowed_blocks_non_admin_under_admins_only(self, mocker):
        api = DatasetApi()
        mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_upload_policy",
            return_value="admins_only",
        )
        mocker.patch(
            "hopsworks_common.core.users_api.UsersApi._get_current_user",
            return_value=User(roles=["HOPS_USER"]),
        )

        with pytest.raises(DatasetException) as exc_info:
            api._assert_upload_allowed("Resources")

        assert "administrators" in str(exc_info.value)

    def test_assert_upload_allowed_refuses_when_role_is_unreadable(self, mocker):
        # Refusing costs the caller an overwrite; guessing costs them the file.
        api = DatasetApi()
        mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_upload_policy",
            return_value="admins_only",
        )
        mocker.patch(
            "hopsworks_common.core.users_api.UsersApi._get_current_user",
            side_effect=_make_rest_api_error(160002, status_code=403),
        )

        with pytest.raises(DatasetException) as exc_info:
            api._assert_upload_allowed("Resources")

        assert "could not be determined" in str(exc_info.value)

    @pytest.mark.parametrize("policy", ["enabled", "", None, "disabeld"])
    def test_assert_upload_allowed_permits_when_not_restricted(self, mocker, policy):
        # An unrecognised value permits the upload, matching the backend's tolerant parsing.
        api = DatasetApi()
        mocker.patch(
            "hopsworks_common.core.variable_api.VariableApi._get_upload_policy",
            return_value=policy,
        )
        mock_profile = mocker.patch(
            "hopsworks_common.core.users_api.UsersApi._get_current_user"
        )

        api._assert_upload_allowed("Resources")

        mock_profile.assert_not_called()

    def test_user_profile_parses_cluster_roles(self):
        # The profile endpoint sends roles as `role`, a list of group objects.
        user = User.from_response_json(
            {"username": "meb10000", "role": [{"groupName": "HOPS_ADMIN"}]}
        )

        assert user.roles == ["HOPS_ADMIN"]


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


class TestDatasetApiShare:
    def _patch_client(self, mocker) -> MagicMock:
        client_instance = MagicMock()
        client_instance._project_id = 119
        client_instance._project_name = "my_project"
        mocker.patch(
            "hopsworks_common.core.dataset_api.client._get_instance",
            return_value=client_instance,
        )
        return client_instance

    def test_share_sends_post_with_action_share(self, mocker):
        api = DatasetApi()
        client_instance = self._patch_client(mocker)

        api.share("Resources/my_dir", "other_project")

        call = client_instance._send_request.call_args_list[0]
        assert call.args[0] == "POST"
        assert call.args[1] == ["project", 119, "dataset", "Resources/my_dir"]
        assert call.kwargs["query_params"] == {
            "action": "SHARE",
            "target_project": "other_project",
            "permission": "READ_ONLY",
        }

    def test_unshare_sends_delete_with_action_unshare(self, mocker):
        api = DatasetApi()
        client_instance = self._patch_client(mocker)

        api.unshare("Resources/my_dir", "other_project")

        call = client_instance._send_request.call_args_list[0]
        assert call.args[0] == "DELETE"
        assert call.args[1] == ["project", 119, "dataset", "Resources/my_dir"]
        assert call.kwargs["query_params"] == {
            "action": "UNSHARE",
            "target_project": "other_project",
        }

    def test_unshare_403_raises_permission_error(self, mocker):
        api = DatasetApi()
        client_instance = self._patch_client(mocker)
        response = MagicMock()
        response.status_code = 403
        client_instance._send_request.side_effect = RestAPIError("url", response)

        with pytest.raises(PermissionError, match="Data Owner"):
            api.unshare("Resources/my_dir", "other_project")

    def test_unshare_empty_target_project_raises_value_error(self, mocker):
        api = DatasetApi()
        client_instance = self._patch_client(mocker)

        with pytest.raises(ValueError, match="target_project"):
            api.unshare("Resources/my_dir", "")

        client_instance._send_request.assert_not_called()
