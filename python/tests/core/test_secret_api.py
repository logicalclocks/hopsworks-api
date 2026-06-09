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

import base64
import json
from unittest.mock import MagicMock

import pytest
from hopsworks_common.core.secret_api import MAX_SECRET_VALUE_LENGTH, SecretsApi


def _secret_response(name: str, value: str, visibility: str = "PRIVATE") -> dict:
    return {
        "items": [{"name": name, "secret": value, "visibility": visibility}],
        "type": "secretDTO",
    }


def _patch_client(mocker, send_request_return):
    client_instance = MagicMock()
    client_instance._send_request.return_value = send_request_return
    # Secret.get_url -> util.get_hostname_replaced_url calls urljoin(_base_url, ...),
    # which raises if _base_url is a MagicMock instead of a string.
    client_instance._base_url = "https://localhost"
    mocker.patch(
        "hopsworks_common.core.secret_api.client._get_instance",
        return_value=client_instance,
    )
    mocker.patch(
        "hopsworks_common.util.client._get_instance",
        return_value=client_instance,
    )
    return client_instance


class TestSecretsApi:
    def test_create_secret_posts_value_as_string(self, mocker):
        api = SecretsApi()
        client_instance = _patch_client(mocker, _secret_response("MY_KEY", "sk-1"))
        result = api.create_secret("MY_KEY", "sk-1")
        assert result.name == "MY_KEY"
        assert result.value == "sk-1"
        post_calls = [
            call
            for call in client_instance._send_request.call_args_list
            if call.args[0] == "POST"
        ]
        assert len(post_calls) == 1
        body = json.loads(post_calls[0].kwargs["data"])
        assert body["name"] == "MY_KEY"
        assert body["secret"] == "sk-1"
        assert body["visibility"] == "PRIVATE"
        assert "scope" not in body

    def test_create_secret_from_file_base64_encodes_and_creates(self, mocker, tmp_path):
        api = SecretsApi()
        path = tmp_path / "key.pem"
        path.write_bytes(b"hello-from-a-file")
        create_secret = mocker.patch.object(SecretsApi, "create_secret")
        api.create_secret_from_file("KEY_PEM", str(path))
        create_secret.assert_called_once_with(
            "KEY_PEM",
            base64.b64encode(b"hello-from-a-file").decode("ascii"),
            project=None,
        )

    def test_create_secret_from_file_at_limit_accepted(self, mocker, tmp_path):
        # base64 length is a multiple of 4 and equals ceil(N / 3) * 4.
        # MAX_SECRET_VALUE_LENGTH = 9000 chars -> raw 6750 bytes is exactly at limit.
        api = SecretsApi()
        raw_size = (MAX_SECRET_VALUE_LENGTH // 4) * 3
        path = tmp_path / "at-limit.bin"
        path.write_bytes(b"\x00" * raw_size)
        create_secret = mocker.patch.object(SecretsApi, "create_secret")
        api.create_secret_from_file("AT_LIMIT", str(path))
        encoded = create_secret.call_args.args[1]
        assert len(encoded) == MAX_SECRET_VALUE_LENGTH

    def test_create_secret_from_file_over_limit_raises(self, mocker, tmp_path):
        api = SecretsApi()
        raw_size = (MAX_SECRET_VALUE_LENGTH // 4) * 3 + 1
        path = tmp_path / "too-big.bin"
        path.write_bytes(b"\x00" * raw_size)
        create_secret = mocker.patch.object(SecretsApi, "create_secret")
        with pytest.raises(ValueError, match="too large after base64 encoding"):
            api.create_secret_from_file("TOO_BIG", str(path))
        create_secret.assert_not_called()

    def test_create_secret_from_file_missing_file_raises(self, mocker, tmp_path):
        api = SecretsApi()
        create_secret = mocker.patch.object(SecretsApi, "create_secret")
        with pytest.raises(FileNotFoundError):
            api.create_secret_from_file("MISSING", str(tmp_path / "nope.bin"))
        create_secret.assert_not_called()
