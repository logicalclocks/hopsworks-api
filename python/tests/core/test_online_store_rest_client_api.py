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
import concurrent.futures
import json
import threading
import time

import pytest
import requests
import urllib3
from furl import furl
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.client.online_store_rest_client import (
    OnlineStoreRestClientSingleton,
)
from hsfs.core import online_store_rest_client_api


class TestOnlineStoreRestClientApi:
    def test_handle_rdrs_feature_store_response_bad_request_primary_key_or_passed_features(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps(
            backend_fixtures["rondb_server"][
                "bad_request_primary_key_or_passed_features_error"
            ]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api._handle_rdrs_feature_store_response(response)

    def test_handle_rdrs_feature_store_response_bad_request_metadata(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 400
        response._content = json.dumps(
            backend_fixtures["rondb_server"]["bad_request_feature_store_view_not_exist"]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api._handle_rdrs_feature_store_response(response)

    def test_handle_rdrs_feature_store_response_unauthorized_request_error(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 401
        response._content = json.dumps(
            backend_fixtures["rondb_server"]["unauthorized_request_error"]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api._handle_rdrs_feature_store_response(response)

    def test_handle_rdrs_feature_store_response_internal_server_error(
        self, backend_fixtures
    ):
        # Arrange
        response = requests.Response()
        response.status_code = 500
        response._content = json.dumps(
            backend_fixtures["rondb_server"]["internal_server_error"]
        ).encode("utf-8")
        online_rest_api = online_store_rest_client_api.OnlineStoreRestClientApi()

        # Act
        with pytest.raises(online_store_rest_client_api.exceptions.RestAPIError):
            online_rest_api._handle_rdrs_feature_store_response(response)


def _streamed(body: bytes = b"{}", chunks: int = 1):
    """A response the client can read the way it reads a real one."""
    response = requests.Response()
    response.status_code = 200
    response.raw = None
    size = max(len(body) // chunks, 1)
    pieces = [body[i : i + size] for i in range(0, len(body), size)] or [b""]
    response.iter_content = lambda chunk_size=None: iter(pieces)
    return response


class TestRestTimeoutAndUrlCaching:
    """The call's timeout reaches the socket, and endpoints are rendered once."""

    def _client(self, mocker):
        from hopsworks_common.client.online_store_rest_client import (
            OnlineStoreRestClientSingleton,
        )

        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = 2
        client._session = mocker.Mock()
        client._session.send.return_value = _streamed()
        # These exercise the Requests path; the urllib3 one has its own class.
        client._transport = OnlineStoreRestClientSingleton.TRANSPORT_REQUESTS
        client._auth = None
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        return client

    def test_an_endpoint_url_is_rendered_once(self, mocker):
        client = self._client(mocker)
        built = mocker.spy(client._base_url, "copy")

        first = client._endpoint_url(["batch_feature_store"])
        second = client._endpoint_url(["batch_feature_store"])

        assert (
            first
            == second
            == ("https://rdrs.example.invalid:4406/0.1.0/batch_feature_store")
        )
        assert built.call_count == 1, "the URL was rebuilt for the second call"

    def test_each_endpoint_gets_its_own_url(self, mocker):
        client = self._client(mocker)

        assert client._endpoint_url(["feature_store"]).endswith("/feature_store")
        assert client._endpoint_url(["ping"]).endswith("/ping")
        assert len(client._endpoint_urls) == 2

    def test_a_call_timeout_reaches_the_send(self, mocker):
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}", timeout=0.25)

        # The deadline covers the wait for a connection too, so what reaches
        # the socket is what is left of it.
        assert client._session.send.call_args.kwargs["timeout"] == pytest.approx(
            0.25, abs=0.02
        )

    def test_the_configured_default_applies_when_no_timeout_is_given(self, mocker):
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}")

        assert client._session.send.call_args.kwargs["timeout"] == pytest.approx(
            2, abs=0.02
        )

    @pytest.mark.parametrize(
        "configured, seconds",
        [(2, 2), (499, 499), (500, 0.5), (2000, 2.0)],
    )
    def test_the_configured_timeout_keeps_its_historical_reading(
        self, configured, seconds
    ):
        """A configured value of 500 or more has always meant milliseconds."""
        from hopsworks_common.client.online_store_rest_client import (
            OnlineStoreRestClientSingleton,
        )

        assert OnlineStoreRestClientSingleton._as_seconds(configured) == seconds

    def test_a_call_timeout_is_seconds_and_is_not_rescaled(self, mocker):
        """Only the configured default carries the millisecond reading."""
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}", timeout=600)

        assert client._session.send.call_args.kwargs["timeout"] == pytest.approx(
            600, abs=0.02
        )


class TestBoundedConnectionWait:
    """Reads in flight are bounded, and waiting for a turn ends."""

    def _client(self, mocker, slots, timeout_seconds=0.2):
        from hopsworks_common.client.online_store_rest_client import (
            OnlineStoreRestClientSingleton,
        )

        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = timeout_seconds
        client._session = mocker.Mock()
        client._session.send.return_value = _streamed()
        # These exercise the Requests path; the urllib3 one has its own class.
        client._transport = OnlineStoreRestClientSingleton.TRANSPORT_REQUESTS
        client._auth = None
        client._connection_slots = threading.BoundedSemaphore(slots)
        client._max_connections = slots
        return client

    def test_a_request_over_the_bound_gives_up_instead_of_waiting(self, mocker):
        client = self._client(mocker, slots=1)
        client._connection_slots.acquire()

        with pytest.raises(TimeoutError, match="waiting for one of 1"):
            client._send_request("POST", ["feature_store"], data="{}")

    def test_the_slot_is_returned_when_the_request_raises(self, mocker):
        client = self._client(mocker, slots=1)
        client._session.send.side_effect = requests.ConnectionError("refused")

        with pytest.raises(requests.ConnectionError):
            client._send_request("POST", ["feature_store"], data="{}")

        assert client._connection_slots.acquire(timeout=0), (
            "a failed request kept its connection slot"
        )

    def test_concurrent_reads_never_exceed_the_bound(self, mocker):
        client = self._client(mocker, slots=2, timeout_seconds=5)
        in_flight = 0
        peak = 0
        lock = threading.Lock()

        def send(*_args, **_kwargs):
            nonlocal in_flight, peak
            with lock:
                in_flight += 1
                peak = max(peak, in_flight)
            time.sleep(0.02)
            with lock:
                in_flight -= 1
            return _streamed()

        client._session.send.side_effect = send
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            list(
                pool.map(
                    lambda _: client._send_request(
                        "POST", ["feature_store"], data="{}"
                    ),
                    range(16),
                )
            )

        assert peak == 2


class TestResetDuringAReadReleasesTheRightSlots:
    """A reset replaces the slots; a call in flight still owns one of the old ones.

    Releasing whatever the attribute names when the call ends gives a slot to a
    pool the call never took one from. A bounded semaphore raises on that, so a
    request that succeeded failed anyway, and the waiters on the old pool are
    left one short.
    """

    def _client(self, mocker):
        from hopsworks_common.client.online_store_rest_client import (
            OnlineStoreRestClientSingleton,
        )

        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = 5
        client._session = mocker.Mock()
        client._session.send.return_value = _streamed()
        # These exercise the Requests path; the urllib3 one has its own class.
        client._transport = OnlineStoreRestClientSingleton.TRANSPORT_REQUESTS
        client._auth = None
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        return client

    def test_a_reset_mid_request_does_not_break_the_request(self, mocker):
        client = self._client(mocker)
        sending = threading.Event()
        may_finish = threading.Event()

        def send(*_args, **_kwargs):
            sending.set()
            may_finish.wait(timeout=5)
            return _streamed()

        client._session.send.side_effect = send
        outcome = {}

        def call():
            try:
                client._send_request("POST", ["feature_store"], data="{}")
                outcome["ok"] = True
            except Exception as error:  # noqa: BLE001 - the failure is the point
                outcome["error"] = error

        caller = threading.Thread(target=call)
        caller.start()
        assert sending.wait(timeout=5)
        # What a reset does to the slots while a read holds one.
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        may_finish.set()
        caller.join(timeout=5)

        assert outcome.get("ok"), f"the request failed with {outcome.get('error')!r}"


class TestTheDeadlineIsTheDeadline:
    """The timeout bounds the call, not each socket operation.

    Requests' timeout is an inactivity timeout: a server that keeps sending a
    byte before it expires holds the caller, and one of the connection slots,
    for as long as it likes. A prediction waiting on a feature vector needs the
    number it was given to be the longest it waits.
    """

    def _client(self, mocker, timeout_seconds=0.2):
        from hopsworks_common.client.online_store_rest_client import (
            OnlineStoreRestClientSingleton,
        )

        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = timeout_seconds
        client._session = mocker.Mock()
        client._auth = None
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        # These exercise the Requests path; the urllib3 one has its own class.
        client._transport = OnlineStoreRestClientSingleton.TRANSPORT_REQUESTS
        return client

    def test_a_trickling_answer_still_ends(self, mocker):
        client = self._client(mocker)

        def trickle(*_args, **_kwargs):
            response = requests.Response()
            response.status_code = 200

            def chunks(chunk_size=None):
                while True:
                    time.sleep(0.015)
                    yield b"x"

            response.iter_content = chunks
            response.close = lambda: None
            return response

        client._session.send.side_effect = trickle

        started = time.monotonic()
        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.04)
        elapsed = time.monotonic() - started

        assert elapsed < 0.5, f"the call ran for {elapsed:.2f}s past a 0.04s deadline"

    def test_a_slot_is_returned_when_the_deadline_ends_the_read(self, mocker):
        client = self._client(mocker)

        def trickle(*_args, **_kwargs):
            response = requests.Response()
            response.status_code = 200
            response.iter_content = lambda chunk_size=None: iter(
                lambda: (time.sleep(0.02), b"x")[1], None
            )
            response.close = lambda: None
            return response

        client._session.send.side_effect = trickle

        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.04)

        assert client._connection_slots.acquire(timeout=0), (
            "a call that ran out of time kept its connection slot"
        )

    @pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), "soon"])
    def test_a_timeout_that_is_not_a_length_of_time_is_refused(self, mocker, bad):
        client = self._client(mocker)
        client._session.send.return_value = _streamed()
        # These exercise the Requests path; the urllib3 one has its own class.
        client._transport = OnlineStoreRestClientSingleton.TRANSPORT_REQUESTS

        with pytest.raises(ValueError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=bad)

    def test_the_body_is_still_readable_after_a_streamed_read(self, mocker):
        client = self._client(mocker, timeout_seconds=5)
        client._session.send.return_value = _streamed(b'{"features": [1, 2]}', chunks=4)

        response = client._send_request("POST", ["feature_store"], data="{}")

        assert response.json() == {"features": [1, 2]}
        assert response.content == b'{"features": [1, 2]}'

    def test_a_socket_timeout_is_reported_the_same_way(self, mocker):
        client = self._client(mocker)
        client._session.send.side_effect = requests.exceptions.ReadTimeout("slow")

        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.2)


class TestTheTransportIsSelectable:
    """urllib3 by default, Requests on request, and the caller cannot tell.

    urllib3 is the pool Requests is a layer over, and going to it directly costs
    the calling thread roughly half the CPU per request for the small responses
    a feature vector read returns. Everything above `_send_request` reads
    `.status_code`, `.json()`, `.content` and `.url`, so both answer with a
    `requests.Response` and nothing downstream changes.
    """

    def _client(self, mocker, transport="urllib3", timeout_seconds=5):
        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = timeout_seconds
        client._session = mocker.Mock()
        client._session.send.return_value = _streamed(b'{"features": [1]}')
        client._auth = None
        client._auth_header_cache = {"X-API-KEY": "secret"}
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        client._transport = transport
        client._pool = mocker.Mock()
        client._pool.request.return_value = mocker.Mock(
            status=200,
            headers={"Content-Type": "application/json"},
            data=b'{"features": [1]}',
        )
        return client

    def test_urllib3_is_the_default(self):
        config = OnlineStoreRestClientSingleton._get_default_static_parameters_config(
            OnlineStoreRestClientSingleton
        )

        assert config[OnlineStoreRestClientSingleton.TRANSPORT] == "urllib3"

    def test_both_transports_answer_the_same_way(self, mocker):
        through_urllib3 = self._client(mocker, transport="urllib3")._send_request(
            "POST", ["feature_store"], data="{}"
        )
        through_requests = self._client(mocker, transport="requests")._send_request(
            "POST", ["feature_store"], data="{}"
        )

        for response in (through_urllib3, through_requests):
            assert response.status_code == 200
            assert response.json() == {"features": [1]}
            assert response.content == b'{"features": [1]}'

    def test_the_api_key_reaches_the_pool(self, mocker):
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}")

        assert client._pool.request.call_args.kwargs["headers"]["X-API-KEY"] == "secret"

    def test_a_call_timeout_bounds_the_pool_request(self, mocker):
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}", timeout=0.5)

        assert client._pool.request.call_args.kwargs["timeout"].total == pytest.approx(
            0.5, abs=0.05
        )

    def test_a_pool_timeout_is_reported_as_one(self, mocker):
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.ReadTimeoutError(
            None, "url", "too slow"
        )

        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.2)

    def test_a_connection_failure_keeps_its_requests_type(self, mocker):
        """Callers already handle the Requests exception, so it stays that."""
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.ProtocolError("gone")

        with pytest.raises(requests.exceptions.ConnectionError):
            client._send_request("POST", ["feature_store"], data="{}")

    def test_an_unknown_transport_is_refused(self):
        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)

        with pytest.raises(FeatureStoreException, match="transport"):
            client._setup_transport("curl")
