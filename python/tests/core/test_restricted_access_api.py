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

from unittest.mock import MagicMock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hsfs.core.restricted_access_api import RestrictedAccessApi


def _patch_client(mocker, send_request_return=None, side_effect=None):
    client_instance = MagicMock()
    client_instance._project_id = 119
    client_instance._project_name = "my_project"
    if side_effect is not None:
        client_instance._send_request.side_effect = side_effect
    else:
        client_instance._send_request.return_value = send_request_return
    mocker.patch(
        "hsfs.core.restricted_access_api.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


def _forbidden_error():
    response = MagicMock()
    response.status_code = 403
    return RestAPIError("url", response)


class TestRestrictedAccessApi:
    def test_grant_restricted_access_sends_expected_request(self, mocker):
        api = RestrictedAccessApi(feature_store_id=67)
        client_instance = _patch_client(mocker)

        api._grant_restricted_access(5, "restricted@example.com")

        call = client_instance._send_request.call_args_list[0]
        assert call.args[0] == "POST"
        assert call.args[1] == [
            "project",
            119,
            "featurestores",
            67,
            "featuregroups",
            5,
            "restrictedaccess",
        ]
        assert call.kwargs["query_params"] == {"user": "restricted@example.com"}

    def test_grant_restricted_access_with_features_sends_feature_list(self, mocker):
        api = RestrictedAccessApi(feature_store_id=67)
        client_instance = _patch_client(mocker)

        api._grant_restricted_access(
            5, "restricted@example.com", features=["amount", "country"]
        )

        call = client_instance._send_request.call_args_list[0]
        assert call.kwargs["query_params"] == {
            "user": "restricted@example.com",
            "feature": ["amount", "country"],
        }

    def test_grant_restricted_access_403_raises_permission_error(self, mocker):
        api = RestrictedAccessApi(feature_store_id=67)
        _patch_client(mocker, side_effect=_forbidden_error())

        with pytest.raises(PermissionError, match="Data Owner"):
            api._grant_restricted_access(5, "restricted@example.com")

    def test_revoke_restricted_access_sends_delete(self, mocker):
        api = RestrictedAccessApi(feature_store_id=67)
        client_instance = _patch_client(mocker)

        api._revoke_restricted_access(5, "restricted@example.com")

        call = client_instance._send_request.call_args_list[0]
        assert call.args[0] == "DELETE"
        assert call.args[1] == [
            "project",
            119,
            "featurestores",
            67,
            "featuregroups",
            5,
            "restrictedaccess",
        ]
        assert call.kwargs["query_params"] == {"user": "restricted@example.com"}

    def test_get_restricted_access_returns_items(self, mocker):
        api = RestrictedAccessApi(feature_store_id=67)
        _patch_client(
            mocker,
            send_request_return={
                "items": [
                    {
                        "grantedToUser": {"email": "restricted@example.com"},
                        "grantedEntirely": True,
                        "features": [],
                    }
                ],
                "count": 1,
            },
        )

        result = api._get_restricted_access(5)

        assert len(result) == 1
        assert result[0]["grantedToUser"]["email"] == "restricted@example.com"

    def test_get_restricted_access_handles_empty_response(self, mocker):
        api = RestrictedAccessApi(feature_store_id=67)
        _patch_client(mocker, send_request_return=None)

        assert api._get_restricted_access(5) == []
