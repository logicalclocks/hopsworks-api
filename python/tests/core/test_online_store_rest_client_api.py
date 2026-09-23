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


def _answered(body: bytes = b"{}", status: int = 200):
    """What the pool hands back for one request."""

    class _PoolResponse:
        def __init__(self):
            self.status = status
            self.headers = {"Content-Type": "application/json"}
            self._left = body
            self.released = False
            self.closed = False

        def read(self, amt=None):
            chunk, self._left = self._left[:amt], self._left[amt:]
            return chunk

        def release_conn(self):
            self.released = True

        def close(self):
            self.closed = True

    return _PoolResponse()


def _pool(mocker, body: bytes = b"{}"):
    pool = mocker.Mock()
    pool.request.return_value = _answered(body)
    return pool


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
        client._pool = _pool(mocker)
        client._auth_header_cache = {}
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
        assert client._pool.request.call_args.kwargs["timeout"].total == pytest.approx(
            0.25, abs=0.02
        )

    def test_the_configured_default_applies_when_no_timeout_is_given(self, mocker):
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}")

        # Bounds each connection attempt and socket read, as Requests' timeout did.
        bound = client._pool.request.call_args.kwargs["timeout"]
        assert bound.total is None
        assert bound.connect_timeout == 2
        assert bound.read_timeout == 2

    def test_a_large_body_is_not_cut_off_by_the_configured_default(self, mocker):
        """Each read arrives in time; only a caller's own timeout bounds the total."""
        client = self._client(mocker)
        client._timeout_seconds = 0.08
        response = _answered(b"x" * (3 * client._READ_CHUNK_BYTES))
        read = response.read

        def slow_read(amt=None):
            time.sleep(0.04)
            return read(amt)

        response.read = slow_read
        client._pool.request.return_value = response

        answered = client._send_request("POST", ["feature_store"], data="{}")

        assert len(answered.content) == 3 * client._READ_CHUNK_BYTES

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

        assert client._pool.request.call_args.kwargs["timeout"].total == pytest.approx(
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
        client._pool = _pool(mocker)
        client._auth_header_cache = {}
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
        client._pool.request.side_effect = urllib3.exceptions.ProtocolError("refused")

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
            return _answered()

        client._pool.request.side_effect = send
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
        client._pool = _pool(mocker)
        client._auth_header_cache = {}
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
            return _answered()

        client._pool.request.side_effect = send
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
    for as long as it likes. urllib3 takes a total, which is what a prediction
    waiting on a feature vector needs, and the call checks the deadline again
    once the pool returns.
    """

    def _client(self, mocker, timeout_seconds=0.2):
        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = timeout_seconds
        client._auth = None
        client._auth_header_cache = {}
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        client._pool = _pool(mocker)
        return client

    def test_the_whole_call_is_bounded_not_each_read(self, mocker):
        client = self._client(mocker)

        client._send_request("POST", ["feature_store"], data="{}", timeout=0.5)

        bound = client._pool.request.call_args.kwargs["timeout"]
        assert bound.total == pytest.approx(0.5, abs=0.05), (
            "the pool was given something other than a total deadline"
        )

    def test_an_answer_that_arrives_too_late_is_still_too_late(self, mocker):
        """The deadline is checked again once the pool returns."""
        client = self._client(mocker)

        def slow(*_args, **_kwargs):
            time.sleep(0.25)
            return _answered()

        client._pool.request.side_effect = slow

        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.05)

    def test_a_slot_is_returned_when_the_deadline_ends_the_read(self, mocker):
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.ReadTimeoutError(
            None, "url", "too slow"
        )

        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.04)

        assert client._connection_slots.acquire(timeout=0), (
            "a call that ran out of time kept its connection slot"
        )

    @pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), "soon"])
    def test_a_timeout_that_is_not_a_length_of_time_is_refused(self, mocker, bad):
        client = self._client(mocker)

        with pytest.raises(ValueError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=bad)

    def test_the_body_is_readable(self, mocker):
        client = self._client(mocker, timeout_seconds=5)
        client._pool.request.return_value = _answered(b'{"features": [1, 2]}')

        response = client._send_request("POST", ["feature_store"], data="{}")

        assert response.json() == {"features": [1, 2]}
        assert response.content == b'{"features": [1, 2]}'
        assert response.status_code == 200

    def test_a_pool_timeout_is_reported_as_one(self, mocker):
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.ConnectTimeoutError(
            None, "too slow"
        )

        with pytest.raises(TimeoutError):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.2)

    def test_a_refused_connection_is_a_connection_error_not_a_timeout(self, mocker):
        """urllib3's NewConnectionError subclasses its connect timeout, so it is checked first."""
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.NewConnectionError(
            None, "refused"
        )

        with pytest.raises(requests.exceptions.ConnectionError) as raised:
            client._send_request("POST", ["feature_store"], data="{}")
        assert not isinstance(raised.value, TimeoutError)

    @pytest.mark.parametrize(
        "error, requests_type",
        [
            (
                urllib3.exceptions.ReadTimeoutError(None, "url", "slow"),
                requests.exceptions.ReadTimeout,
            ),
            (
                urllib3.exceptions.ConnectTimeoutError(None, "slow"),
                requests.exceptions.ConnectTimeout,
            ),
        ],
    )
    def test_a_timeout_is_both_the_requests_type_and_the_builtin(
        self, mocker, error, requests_type
    ):
        """Callers that caught the Requests timeout keep catching it, and so do callers of the documented TimeoutError."""
        client = self._client(mocker)
        client._pool.request.side_effect = error

        with pytest.raises(requests_type) as raised:
            client._send_request("POST", ["feature_store"], data="{}")
        assert isinstance(raised.value, TimeoutError)
        assert isinstance(raised.value, requests.exceptions.RequestException)

    def test_a_tls_failure_keeps_its_requests_type(self, mocker):
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.SSLError("bad cert")

        with pytest.raises(requests.exceptions.SSLError):
            client._send_request("POST", ["feature_store"], data="{}")

    def test_waiting_for_a_connection_slot_raises_both_timeout_types(self, mocker):
        client = self._client(mocker)
        client._connection_slots = threading.BoundedSemaphore(1)
        client._connection_slots.acquire()

        with pytest.raises(requests.exceptions.Timeout) as raised:
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.05)
        assert isinstance(raised.value, TimeoutError)

    def test_a_body_that_outlasts_the_deadline_is_cut_off(self, mocker):
        """urllib3 bounds each socket read, so the body is read in chunks against the deadline."""
        client = self._client(mocker)
        response = _answered(b"x" * (3 * client._READ_CHUNK_BYTES))
        read = response.read

        def slow_read(amt=None):
            time.sleep(0.05)
            return read(amt)

        response.read = slow_read
        client._pool.request.return_value = response

        with pytest.raises(TimeoutError, match="still answering"):
            client._send_request("POST", ["feature_store"], data="{}", timeout=0.08)
        assert response.closed, "a connection left mid-body was handed back open"
        assert response.released

    def test_a_connection_failure_keeps_its_requests_type(self, mocker):
        """Callers already handle the Requests exception, so it stays that."""
        client = self._client(mocker)
        client._pool.request.side_effect = urllib3.exceptions.ProtocolError("gone")

        with pytest.raises(requests.exceptions.ConnectionError):
            client._send_request("POST", ["feature_store"], data="{}")


class TestEveryRequestGoesThroughThePool:
    """One transport, and what that has to keep doing.

    urllib3 is the pool Requests is a layer over, and going to it directly
    costs the calling thread roughly half the CPU per request for the small
    responses a feature vector read returns.

    What Requests did for free still has to happen: proxies come from the
    environment, and a Requests transport adapter has nowhere to go, so it is
    refused rather than accepted and ignored.
    """

    def _client(self, mocker, *, verify=False):
        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._max_connections = 4
        client._current_config = {
            OnlineStoreRestClientSingleton.VERIFY_CERTS: verify,
            OnlineStoreRestClientSingleton.CA_CERTS: "/ca.pem" if verify else None,
        }
        return client

    def test_a_pool_manager_without_a_proxy(self, mocker):
        client = self._client(mocker)
        mocker.patch("requests.utils.get_environ_proxies", return_value={})

        client._setup_pool()

        assert isinstance(client._pool, urllib3.PoolManager)
        assert not isinstance(client._pool, urllib3.ProxyManager)

    def test_a_configured_proxy_is_used(self, mocker):
        """Requests read these variables; dropping them would change behaviour."""
        client = self._client(mocker)
        mocker.patch(
            "requests.utils.get_environ_proxies",
            return_value={"https": "http://proxy.example:3128"},
        )

        client._setup_pool()

        assert isinstance(client._pool, urllib3.ProxyManager)

    def test_certificate_verification_reaches_the_pool(self, mocker):
        client = self._client(mocker, verify=True)
        mocker.patch("requests.utils.get_environ_proxies", return_value={})

        client._setup_pool()

        assert client._pool.connection_pool_kw["cert_reqs"] == "CERT_REQUIRED"
        assert client._pool.connection_pool_kw["ca_certs"] == "/ca.pem"

    def test_a_reset_closes_the_pool_it_replaces(self, mocker):
        client = self._client(mocker)
        mocker.patch("requests.utils.get_environ_proxies", return_value={})
        client._setup_pool()
        first = client._pool
        clear = mocker.spy(first, "clear")

        client._setup_pool()

        assert client._pool is not first
        clear.assert_called_once_with()

    def test_the_session_property_refuses_rather_than_being_ignored(self, mocker):
        client = self._client(mocker)

        with pytest.raises(FeatureStoreException, match="Requests session"):
            _ = client.session

    def test_a_requests_adapter_is_refused(self, mocker):
        """Accepting one and sending elsewhere would be a silent change."""
        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._current_config = {
            OnlineStoreRestClientSingleton.MAX_CONNECTIONS: 4,
            OnlineStoreRestClientSingleton.VERIFY_CERTS: False,
            OnlineStoreRestClientSingleton.CA_CERTS: None,
        }
        mocker.patch.object(client, "_set_auth")

        with pytest.raises(FeatureStoreException, match="transport adapter"):
            client._setup_rest_client(
                transport=requests.adapters.HTTPAdapter(), optional_config=None
            )

    def test_the_api_key_reaches_the_pool(self, mocker):
        client = OnlineStoreRestClientSingleton.__new__(OnlineStoreRestClientSingleton)
        client._base_url = furl("https://rdrs.example.invalid:4406/0.1.0")
        client._endpoint_urls = {}
        client._timeout_seconds = 5
        client._auth = None
        client._auth_header_cache = {"X-API-KEY": "secret"}
        client._connection_slots = threading.BoundedSemaphore(2)
        client._max_connections = 2
        client._pool = _pool(mocker, b'{"features": [1]}')

        response = client._send_request("POST", ["feature_store"], data="{}")

        assert client._pool.request.call_args.kwargs["headers"]["X-API-KEY"] == "secret"
        assert response.json() == {"features": [1]}
