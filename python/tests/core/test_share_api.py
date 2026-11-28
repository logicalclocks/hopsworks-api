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
from hsfs.core.share_api import ShareApi


FS_SHARE_PATH = ["project", 119, "featurestores", "share"]
FG_SHARE_PATH = ["project", 119, "featurestores", "share", "featuregroups", 7]

# One case per share/unshare path: the call, its HTTP method and its path.
SHARE_CALLS = [
    pytest.param(
        lambda api, target: api._share_feature_store(target),
        "POST",
        FS_SHARE_PATH,
        id="share_feature_store",
    ),
    pytest.param(
        lambda api, target: api._unshare_feature_store(target),
        "DELETE",
        FS_SHARE_PATH,
        id="unshare_feature_store",
    ),
    pytest.param(
        lambda api, target: api._share_feature_group(7, target),
        "POST",
        FG_SHARE_PATH,
        id="share_feature_group",
    ),
    pytest.param(
        lambda api, target: api._unshare_feature_group(7, target),
        "DELETE",
        FG_SHARE_PATH,
        id="unshare_feature_group",
    ),
]


class TestShareApi:
    def _patch_client(self, mocker) -> MagicMock:
        client_instance = MagicMock()
        client_instance._project_id = 119
        client_instance._project_name = "kommun_a"
        # The sharer is not a member of the target, so a member-only lookup would 403.
        mocker.patch(
            "hopsworks_common.client._get_instance", return_value=client_instance
        )
        return client_instance

    @staticmethod
    def _assert_no_project_lookup(client_instance: MagicMock) -> None:
        for call in client_instance._send_request.call_args_list:
            assert "getProjectInfo" not in call.args[1]

    @pytest.mark.parametrize(("call", "method", "path"), SHARE_CALLS)
    def test_name_is_sent_as_target_project(self, mocker, call, method, path):
        client_instance = self._patch_client(mocker)

        call(ShareApi(), "kommun_b")

        self._assert_no_project_lookup(client_instance)
        client_instance._send_request.assert_called_once()
        sent = client_instance._send_request.call_args
        assert sent.args[0] == method
        assert sent.args[1] == path
        assert sent.kwargs["query_params"] == {"target_project": "kommun_b"}

    @pytest.mark.parametrize(("call", "method", "path"), SHARE_CALLS)
    def test_id_is_sent_as_project(self, mocker, call, method, path):
        client_instance = self._patch_client(mocker)

        call(ShareApi(), 120)

        self._assert_no_project_lookup(client_instance)
        client_instance._send_request.assert_called_once()
        sent = client_instance._send_request.call_args
        assert sent.args[0] == method
        assert sent.args[1] == path
        assert sent.kwargs["query_params"] == {"project": 120}

    def test_feature_subset_is_sent_with_the_name(self, mocker):
        client_instance = self._patch_client(mocker)

        ShareApi()._share_feature_group(7, "kommun_b", features=["a", "b"])

        client_instance._send_request.assert_called_once()
        assert client_instance._send_request.call_args.kwargs["query_params"] == {
            "target_project": "kommun_b",
            "feature": ["a", "b"],
        }
