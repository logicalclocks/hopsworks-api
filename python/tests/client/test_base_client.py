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
import json
import os
import stat
import threading

import pytest
import requests
from hopsworks_common import constants
from hsfs.client.base import Client
from hsfs.client.exceptions import RestAPIError

from tests.util import changes_environ


class TestBaseClient:
    @pytest.mark.parametrize("status", [429, 401])
    def test_request_deadline_includes_http_and_retries(self, mocker, status):
        client = self._init_test_client()
        client._base_url = "https://example.test"
        now = [0]
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic", side_effect=lambda: now[0]
        )
        mocker.patch(
            "hopsworks_common.client.base.time.sleep",
            side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds),
        )
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        mocker.patch.object(client, "_read_jwt", return_value="new-token")
        client.TOKEN_EXPIRED_RETRY_INTERVAL = 1
        response = (
            self._rate_limited_response(mocker, retry_after="1")
            if status == 429
            else self._ok_response()
        )
        response.status_code = status
        response.raw = mocker.Mock()

        def send(*args, **kwargs):
            budget = kwargs["timeout"].total
            assert 0 < budget <= 2 - now[0]
            now[0] += budget
            return response

        mocked_send = mocker.patch("requests.sessions.Session.send", side_effect=send)
        with pytest.raises(requests.exceptions.Timeout):
            client._send_request("POST", ["commands", "custom"], timeout=60, deadline=2)

        assert now[0] == 2
        mocked_send.assert_called_once()

    def test_success_completed_at_the_deadline_is_returned(self, mocker):
        client = self._init_test_client()
        client._base_url = "https://example.test"
        now = [0]
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic", side_effect=lambda: now[0]
        )
        response = self._ok_response()
        response._content = b'{"id": 42}'

        def send(*args, **kwargs):
            now[0] += kwargs["timeout"].total
            return response

        mocker.patch("requests.sessions.Session.send", side_effect=send)

        result = client._send_request(
            "POST", ["commands", "custom"], timeout=60, deadline=2
        )

        assert result == {"id": 42}
        assert now[0] == 2

    def test_rate_limit_retry_uses_only_the_remaining_request_budget(self, mocker):
        client = self._init_test_client()
        client._base_url = "https://example.test"
        now = [0]
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic", side_effect=lambda: now[0]
        )
        mocker.patch(
            "hopsworks_common.client.base.time.sleep",
            side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds),
        )
        budgets = []

        def send(*args, **kwargs):
            budgets.append(kwargs["timeout"].total)
            now[0] += 1
            return (
                self._rate_limited_response(mocker, retry_after="1")
                if len(budgets) == 1
                else self._ok_response()
            )

        mocker.patch("requests.sessions.Session.send", side_effect=send)
        client._send_request("GET", ["commands"], timeout=60, deadline=4)

        assert budgets == [4, 2]
        assert now[0] == 3

    @changes_environ
    def test_valid_token_no_retires(self, mocker):
        # Arrange
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
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
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        client = self._init_test_client()

        path_params = [
            "variables",
            "versions",
        ]

        # Mock the requests library
        mocker.patch("requests.sessions.Session.prepare_request")
        unauthorized_response = requests.Response()
        unauthorized_response.status_code = 401
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
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
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

    def _rate_limited_response(self, mocker, retry_after="2", error_code=120015):
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = retry_after
        response._content = json.dumps({"errorCode": error_code}).encode()
        response.raw = mocker.Mock()
        mocker.spy(response, "close")
        return response

    def _ok_response(self):
        response = requests.Response()
        response.status_code = 200
        response._content = b""
        return response

    def _mock_clock(self, mocker, monotonic=1000.0):
        sleep = mocker.patch("hopsworks_common.client.base.time.sleep")
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic", return_value=monotonic
        )
        return sleep

    def test_rate_limited_request_waits_and_retries(self, mocker):
        client = self._init_test_client()
        refused = self._rate_limited_response(mocker, retry_after="3")
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[refused, self._ok_response()],
        )
        sleep = self._mock_clock(mocker)

        client._send_request("GET", ["variables", "versions"])

        assert send.call_count == 2
        sleep.assert_called_once_with(3)
        refused.close.assert_called_once()

    def test_other_too_many_requests_is_not_retried(self, mocker):
        client = self._init_test_client()
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            return_value=self._rate_limited_response(mocker, error_code=270999),
        )
        sleep = self._mock_clock(mocker)

        with pytest.raises(RestAPIError) as error:
            client._send_request("GET", ["variables", "versions"])

        assert (
            error.value.response.status_code
            == RestAPIError.STATUS_CODE_TOO_MANY_REQUESTS
        )
        assert send.call_count == 1
        sleep.assert_not_called()

    def test_rate_limited_wait_beyond_budget_raises(self, mocker):
        client = self._init_test_client()
        first = self._rate_limited_response(mocker, retry_after="30")
        second = self._rate_limited_response(mocker, retry_after="30")
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send", side_effect=[first, second]
        )
        sleep = mocker.patch("hopsworks_common.client.base.time.sleep")
        # Deadline at 1060; the first 30 s wait ends at 1030, and at 1031 only 29 s remain for the second.
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic",
            side_effect=[1000.0, 1000.0, 1030.0, 1031.0],
        )

        with pytest.raises(RestAPIError) as error:
            client._send_request("GET", ["variables", "versions"])

        assert error.value.response is second
        assert send.call_count == 2
        sleep.assert_called_once_with(30)
        second.close.assert_not_called()

    @changes_environ
    def test_rate_limited_then_unauthorized_refreshes_token(self, mocker):
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        client = self._init_test_client()
        unauthorized = requests.Response()
        unauthorized.status_code = 401
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[
                self._rate_limited_response(mocker),
                unauthorized,
                self._ok_response(),
            ],
        )
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        spy_retry_token_expired = mocker.spy(client, "_retry_token_expired")
        self._mock_clock(mocker)

        client._send_request("GET", ["variables", "versions"])

        assert send.call_count == 3
        assert spy_retry_token_expired.call_count == 1

    def test_token_refresh_after_a_rate_limit_stops_at_the_deadline(self, mocker):
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        client = self._init_test_client()
        unauthorized = requests.Response()
        unauthorized.status_code = 401
        mocker.patch("requests.sessions.Session.prepare_request")
        mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[self._rate_limited_response(mocker)] + [unauthorized] * 20,
        )
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        clock = [1000.0]
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic", side_effect=lambda: clock[0]
        )
        mocker.patch(
            "hopsworks_common.client.base.time.sleep",
            side_effect=lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )

        with pytest.raises(RestAPIError):
            client._send_request("GET", ["variables", "versions"])

        assert clock[0] - 1000.0 <= Client.RATE_LIMITED_RETRY_BUDGET_SECONDS

    @changes_environ
    def test_token_refresh_overshooting_the_deadline_is_not_sent(self, mocker):
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        client = self._init_test_client()
        unauthorized = self._unauthorized_response()
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[
                self._rate_limited_response(mocker),
                unauthorized,
                self._ok_response(),
            ],
        )
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        clock = [1000.0]
        mocker.patch(
            "hopsworks_common.client.base.time.monotonic", side_effect=lambda: clock[0]
        )

        def sleep(seconds):
            # The token refresh wait is the only sub-second sleep; it overshoots by 120 s.
            clock[0] += seconds + (120 if seconds < 1 else 0)

        mocker.patch("hopsworks_common.client.base.time.sleep", side_effect=sleep)

        with pytest.raises(RestAPIError) as error:
            client._send_request("GET", ["variables", "versions"])

        assert error.value.response is unauthorized
        assert send.call_count == 2

    def _unauthorized_response(self):
        response = requests.Response()
        response.status_code = 401
        return response

    @changes_environ
    @pytest.mark.parametrize("unauthorized_count", [1, 2])
    def test_rate_limited_token_refresh_is_retried(self, mocker, unauthorized_count):
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        client = self._init_test_client()
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[
                self._unauthorized_response() for _ in range(unauthorized_count)
            ]
            + [
                self._rate_limited_response(mocker, retry_after="3"),
                self._ok_response(),
            ],
        )
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        sleep = self._mock_clock(mocker)

        client._send_request("GET", ["variables", "versions"])

        assert send.call_count == unauthorized_count + 2
        sleep.assert_any_call(3)

    @changes_environ
    def test_token_refresh_returns_the_refreshed_error(self, mocker):
        mocker.patch.dict(os.environ, {Client.REST_ENDPOINT: "True"})
        client = self._init_test_client()
        forbidden = requests.Response()
        forbidden.status_code = 403
        mocker.patch("requests.sessions.Session.prepare_request")
        mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[self._unauthorized_response(), forbidden],
        )
        mocker.patch("hopsworks_common.client.base.Client._read_jwt")
        self._mock_clock(mocker)

        with pytest.raises(RestAPIError) as error:
            client._send_request("GET", ["variables", "versions"])

        assert error.value.response is forbidden

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"data": io.BytesIO(b"payload")},
            {"data": (chunk for chunk in [b"payload"])},
            {"data": {"field": iter(["a", "b"])}},
            {"query_params": {"expand": iter(["commands"])}},
            {"files": {"file": ("name", io.BytesIO(b"payload"))}},
        ],
    )
    def test_non_replayable_body_is_not_retried(self, mocker, kwargs):
        client = self._init_test_client()
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            return_value=self._rate_limited_response(mocker),
        )
        sleep = self._mock_clock(mocker)

        with pytest.raises(RestAPIError):
            client._send_request("POST", ["variables", "versions"], **kwargs)

        assert send.call_count == 1
        sleep.assert_not_called()

    def test_in_memory_upload_is_retried(self, mocker):
        client = self._init_test_client()
        mocker.patch("requests.sessions.Session.prepare_request")
        send = mocker.patch(
            "requests.sessions.Session.send",
            side_effect=[self._rate_limited_response(mocker), self._ok_response()],
        )
        self._mock_clock(mocker)

        client._send_request(
            "POST",
            ["variables", "versions"],
            data={"flowChunkNumber": 1},
            files={"file": ("name", b"payload")},
        )

        assert send.call_count == 2

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
