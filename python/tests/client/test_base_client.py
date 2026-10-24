#
#   Copyright 2024 Hopsworks AB
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

import io
import os
import stat
import threading
import time

import pytest
import requests
from hopsworks_common import constants
from hsfs.client.base import Client
from hsfs.client.exceptions import RestAPIError

from tests.util import changes_environ


class TestBaseClient:
    @changes_environ
    def test_valid_token_no_retires(self, mocker):
        # Arrange
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        ok_response = requests.Response()
        ok_response.status_code = 200
        ok_response._content = ""
        mocker.patch("requests.sessions.Session.send", return_value=ok_response)

        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")

        # Act
        client._send_request("GET", path_params)

        # Assert
        assert spy_retry_token_expired.call_count == 0

    @changes_environ
    def test_invalid_token_retires(self, mocker):
        # Arrange
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        unauthorized_response = requests.Response()
        unauthorized_response.status_code = 401
        unauthorized_response.raw = io.BytesIO()
        unauthorized_response._content = b""
        mocker.patch(
            "requests.sessions.Session.send", return_value=unauthorized_response
        )

        # Mock and spy the client
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")

        # Act
        with pytest.raises(RestAPIError):
            client._send_request("GET", path_params)

        # Assert
        assert spy_retry_token_expired.call_count == 10

    @changes_environ
    def test_invalid_token_retires_backoff_break(self, mocker):
        # Arrange
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        # setup unauthorized response
        unauthorized_response = requests.Response()
        unauthorized_response.status_code = 401
        unauthorized_response.raw = io.BytesIO()
        unauthorized_response._content = b""

        # setup ok response
        ok_response = requests.Response()
        ok_response.status_code = 200
        ok_response._content = ""

        mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[unauthorized_response] * 5 + [ok_response],
        )

        # Mock and spy the client
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")

        # Act
        client._send_request("GET", path_params)

        # Assert
        assert spy_retry_token_expired.call_count == 5

    def test_an_error_answer_under_a_deadline_is_closed_unread(self, mocker):
        client = self._init_test_client()

        class UnreadBody(io.RawIOBase):
            def readable(self):
                return True

            def readinto(self, b):
                raise AssertionError("the error body was read")

        mocker.patch("requests.sessions.Session.prepare_request")
        failed = requests.Response()
        failed.status_code = 500
        failed.reason = "Internal Server Error"
        failed.raw = UnreadBody()
        mocker.patch("requests.sessions.Session.send", return_value=failed)

        with pytest.raises(requests.HTTPError, match="HTTP 500 Internal Server Error"):
            client._send_request(
                "GET", ["logs"], stream=True, deadline=time.monotonic() + 30
            )

        assert failed.raw.closed

    @changes_environ
    def test_no_token_retry_starts_past_the_deadline(self, mocker):
        os.environ[Client.REST_ENDPOINT] = "True"
        client = self._init_test_client()
        client.TOKEN_EXPIRED_RETRY_INTERVAL = 0.6

        mocker.patch("requests.sessions.Session.prepare_request")
        unauthorized_response = requests.Response()
        unauthorized_response.status_code = 401
        unauthorized_response.raw = io.BytesIO()
        send = mocker.patch(
            "requests.sessions.Session.send", return_value=unauthorized_response
        )
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")

        with pytest.raises(TimeoutError):
            client._send_request(
                "GET", ["logs"], stream=True, deadline=time.monotonic() + 0.5
            )

        assert send.call_count == 1
        assert unauthorized_response.raw.closed

    def _init_test_client(self):
        client = Client()
        client._connected = True
        client._base_url = ""
        client._auth = None
        client._verify = False
        client._session = requests.session()
        client.TOKEN_EXPIRED_RETRY_INTERVAL = 0  # Disable wait for tests

        return client

    @pytest.mark.skipif(
        os.name == "nt", reason="Windows cannot replace a file a reader has open"
    )
    def test_replace_file_is_never_seen_partial(self, tmp_path):
        path = tmp_path / "ca_chain.pem"
        # Two contents of one length, so every write replaces the file and a partial read shows as a length.
        contents = ["A" * 200_000, "B" * 200_000]
        Client._replace_file(str(path), contents[0])
        stop = threading.Event()
        seen = set()

        def write():
            turn = 0
            while not stop.is_set():
                turn += 1
                Client._replace_file(str(path), contents[turn % 2])

        writer = threading.Thread(target=write)
        writer.start()
        try:
            for _ in range(2000):
                seen.add(len(path.read_text()))
        finally:
            stop.set()
            writer.join()

        assert seen == {200_000}
        assert sorted(p.name for p in tmp_path.iterdir()) == ["ca_chain.pem"]

    @pytest.mark.skipif(os.name == "nt", reason="Windows has no POSIX file modes")
    def test_replace_file_keeps_the_file_private(self, tmp_path):
        path = tmp_path / "client_key.pem"
        path.write_text("old")
        os.chmod(path, 0o600)
        previous = os.umask(0o022)
        try:
            Client._replace_file(str(path), "new")
        finally:
            os.umask(previous)

        assert path.read_text() == "new"
        assert stat.S_IMODE(path.stat().st_mode) == 0o600

    def test_replace_file_leaves_identical_content_alone(self, tmp_path, mocker):
        path = tmp_path / "ca_chain.pem"
        path.write_text("same")
        replace = mocker.patch("os.replace")

        Client._replace_file(str(path), "same")

        replace.assert_not_called()
        assert list(tmp_path.iterdir()) == [path]

    def test_replace_file_retries_while_the_target_is_held(self, tmp_path, mocker):
        path = tmp_path / "ca_chain.pem"
        path.write_text("old")
        real_replace = os.replace
        held = [PermissionError("in use"), PermissionError("in use")]

        def replace_once_released(src, dst):
            if held:
                raise held.pop()
            real_replace(src, dst)

        replace = mocker.patch("os.replace", side_effect=replace_once_released)
        mocker.patch("time.sleep")

        Client._replace_file(str(path), "new")

        assert replace.call_count == 3
        assert path.read_text() == "new"
        assert list(tmp_path.iterdir()) == [path]

    def test_replace_file_removes_the_temporary_file_on_failure(self, tmp_path, mocker):
        path = tmp_path / "ca_chain.pem"
        mocker.patch("os.replace", side_effect=OSError("disk full"))

        with pytest.raises(OSError, match="disk full"):
            Client._replace_file(str(path), "content")

        assert list(tmp_path.iterdir()) == []

    def test_write_pem_writes_to_temp_dir(self, mocker):
        mocker.patch.object(Client, "_load_jks", return_value=([], []))
        mocker.patch.object(Client, "_write_ca_chain")
        mocker.patch.object(Client, "_write_client_cert")
        mocker.patch.object(Client, "_write_client_key")

        paths = Client()._write_pem("ks", "pw", "ts", "pw", "kafka_sc_1")

        assert paths == tuple(
            os.path.join(constants._temp_dir(), f"kafka_sc_1_{name}.pem")
            for name in ("ca_chain", "client_cert", "client_key")
        )
        assert all(os.path.isdir(os.path.dirname(path)) for path in paths)
