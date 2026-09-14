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

from __future__ import annotations

import logging
import math
import threading
import time
from typing import Any
from warnings import warn

import requests
import requests.adapters
import urllib3
from furl import furl
from hopsworks_apigen import also_available_as
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import variable_api


_logger = logging.getLogger(__name__)

_online_store_rest_client = None


@also_available_as(
    "hsfs.client.online_store_rest_client._init_or_reset_online_store_rest_client"
)
def _init_or_reset_online_store_rest_client(
    transport: requests.adapters.HTTPAdapter
    | requests.adapters.BaseAdapter
    | None = None,
    optional_config: dict[str, Any] | None = None,
    reset_client: bool = False,
):
    global _online_store_rest_client
    if not _online_store_rest_client:
        _online_store_rest_client = OnlineStoreRestClientSingleton(
            transport=transport, optional_config=optional_config
        )
    elif reset_client:
        _online_store_rest_client._reset_client(
            transport=transport, optional_config=optional_config
        )
    else:
        if _logger.isEnabledFor(logging.WARNING):
            _logger.warning(
                "Online Store Rest Client is already initialised. To reset connection or/and override configuration, "
                "use reset_online_store_rest_client flag.",
                stacklevel=2,
            )


@also_available_as("hsfs.client.online_store_rest_client._get_instance")
def _get_instance() -> OnlineStoreRestClientSingleton:
    global _online_store_rest_client
    if _online_store_rest_client is None:
        if _logger.isEnabledFor(logging.WARNING):
            _logger.warning(
                "Online Store Rest Client is not initialised. Initialising with default configuration."
            )
        _online_store_rest_client = OnlineStoreRestClientSingleton()
    if _logger.isEnabledFor(logging.DEBUG):
        _logger.debug("Accessing global Online Store Rest Client instance.")
    return _online_store_rest_client


@also_available_as(
    "hsfs.client.online_store_rest_client.OnlineStoreRestClientSingleton"
)
class OnlineStoreRestClientSingleton:
    HOST = "host"
    PORT = "port"
    VERIFY_CERTS = "verify_certs"
    USE_SSL = "use_ssl"
    CA_CERTS = "ca_certs"
    HTTP_AUTHORIZATION = "http_authorization"
    TIMEOUT = "timeout"
    SERVER_API_VERSION = "server_api_version"
    API_KEY = "api_key"
    MAX_CONNECTIONS = "max_connections"
    _DEFAULT_ONLINE_STORE_REST_CLIENT_PORT = 4406
    _DEFAULT_ONLINE_STORE_REST_CLIENT_MAX_CONNECTIONS = 16
    # Read size for pulling a response body under the call's deadline.
    _READ_CHUNK_BYTES = 65536
    _DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_SECOND = 2
    _DEFAULT_ONLINE_STORE_REST_CLIENT_VERIFY_CERTS = True
    _DEFAULT_ONLINE_STORE_REST_CLIENT_USE_SSL = True
    _DEFAULT_ONLINE_STORE_REST_CLIENT_SERVER_API_VERSION = "0.1.0"
    _DEFAULT_ONLINE_STORE_REST_CLIENT_HTTP_AUTHORIZATION = "X-API-KEY"

    def __init__(
        self,
        transport: requests.adapters.HTTPAdapter
        | requests.adapters.BaseAdapter
        | None = None,
        optional_config: dict[str, Any] | None = None,
    ):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Initialising Online Store Rest Client {'with optional configuration' if optional_config else ''}."
            )
        if optional_config and _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Optional Config: {optional_config!r}")
        self._check_hopsworks_connection()
        self.variable_api = variable_api.VariableApi()
        self._auth: client.auth.OnlineStoreKeyAuth
        self._session: requests.Session
        self._current_config: dict[str, Any]
        self._base_url: furl
        self._endpoint_urls: dict[tuple[str, ...], str] = {}
        self._timeout_seconds: float = (
            self._DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_SECOND
        )
        self._setup_rest_client(
            transport=transport,
            optional_config=optional_config,
            use_current_config=False,
        )
        self._is_connected()

    def _reset_client(
        self,
        transport: requests.adapters.HTTPAdapter
        | requests.adapters.BaseAdapter
        | None = None,
        optional_config: dict[str, Any] | None = None,
    ):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Resetting Online Store Rest Client {'with optional configuration' if optional_config else ''}."
            )
        if optional_config and _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Optional Config: {optional_config}")
        self._check_hopsworks_connection()
        if hasattr(self, "_session") and self._session:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Closing existing session.")
            self._session.close()
            delattr(self, "_session")
        self._setup_rest_client(
            transport=transport,
            optional_config=optional_config,
            use_current_config=not optional_config,
        )

    def _setup_rest_client(
        self,
        transport: requests.adapters.HTTPAdapter
        | requests.adapters.BaseAdapter
        | None = None,
        optional_config: dict[str, Any] | None = None,
        use_current_config: bool = True,
    ):
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Setting up Online Store Rest Client.")
        if optional_config and not isinstance(optional_config, dict):
            raise ValueError(
                "optional_config must be a dictionary. See documentation for allowed keys and values."
            )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Optional Config: %s", optional_config)
        if not use_current_config:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Retrieving default configuration for Online Store REST Client."
                )
            self._current_config = self._get_default_client_config()
        if optional_config:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Updating default configuration with provided optional configuration."
                )
            self._current_config.update(optional_config)

        self._set_auth(optional_config)
        if not hasattr(self, "_session") or not self._session:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Initialising new requests session.")
            self._session = requests.Session()
        else:
            raise ValueError(
                "Use the init_or_reset_online_store_connection method with reset_connection flag set "
                "to True to reset the online_store_client_connection"
            )
        max_connections = int(self._current_config[self.MAX_CONNECTIONS])
        if max_connections < 1:
            raise FeatureStoreException(
                f"{self.MAX_CONNECTIONS} must be at least 1, got {max_connections}."
            )
        # The pool is sized to the number of reads allowed in flight below, so
        # it is never the thing that overflows.
        self._max_connections = max_connections
        self._connection_slots = threading.BoundedSemaphore(max_connections)
        if transport is not None:
            # Requests mounts a transport adapter on a session; requests are
            # sent through a urllib3 pool, which has nowhere to put one.
            # Refusing is better than accepting it and sending elsewhere.
            raise FeatureStoreException(
                "A Requests transport adapter cannot be applied: online store "
                "requests are sent through urllib3. Configure the pool with the "
                f"`{self.MAX_CONNECTIONS}`, `{self.VERIFY_CERTS}` and "
                f"`{self.CA_CERTS}` options instead."
            )

        if not self._current_config[self.VERIFY_CERTS] and _logger.isEnabledFor(
            logging.WARNING
        ):
            _logger.warning(
                "Disabling SSL certificate verification. This is not recommended for production environments."
            )

        # Set base_url
        scheme = "https" if self._current_config[self.USE_SSL] else "http"
        self._base_url = furl(
            f"{scheme}://{self._current_config[self.HOST]}:{self._current_config[self.PORT]}/{self._current_config[self.SERVER_API_VERSION]}"
        )
        # Every request used to copy the furl, extend its path and render it
        # again. The endpoints are fixed once host, port, scheme and API version
        # are, so they are rendered here and rebuilt whenever this runs again.
        self._endpoint_urls = {}
        # Seconds, resolved once. The wire value has historically been read as
        # milliseconds when it is 500 or more, and that reading is kept for
        # configurations that rely on it rather than changed under them.
        self._timeout_seconds = self._as_seconds(self._current_config[self.TIMEOUT])
        self._setup_pool()
        # The auth object rewrites a request's headers; urllib3 is handed the
        # result instead, worked out once rather than per call.
        self._auth_header_cache = self._auth_headers()

        assert self._session is not None, (
            "Online Store REST Client failed to initialise."
        )
        assert self._auth is not None, (
            "Online Store REST Client Authentication failed to initialise. Check API Key."
        )
        assert self._base_url is not None, (
            "Online Store REST Client Base URL failed to initialise. Check host and port parameters."
        )
        assert self._current_config is not None, (
            "Online Store REST Client Configuration failed to initialise."
        )

    def _get_default_client_config(self) -> dict[str, Any]:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Retrieving default configuration for Online Store REST Client."
            )
        default_config = self._get_default_static_parameters_config()
        default_config.update(self._get_default_dynamic_parameters_config())
        return default_config

    def _get_default_static_parameters_config(self) -> dict[str, Any]:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Retrieving default static configuration for Online Store REST Client."
            )
        return {
            self.TIMEOUT: self._DEFAULT_ONLINE_STORE_REST_CLIENT_TIMEOUT_SECOND,
            self.MAX_CONNECTIONS: self._DEFAULT_ONLINE_STORE_REST_CLIENT_MAX_CONNECTIONS,
            self.VERIFY_CERTS: self._DEFAULT_ONLINE_STORE_REST_CLIENT_VERIFY_CERTS,
            self.USE_SSL: self._DEFAULT_ONLINE_STORE_REST_CLIENT_USE_SSL,
            self.SERVER_API_VERSION: self._DEFAULT_ONLINE_STORE_REST_CLIENT_SERVER_API_VERSION,
            self.HTTP_AUTHORIZATION: self._DEFAULT_ONLINE_STORE_REST_CLIENT_HTTP_AUTHORIZATION,
        }

    def _get_default_dynamic_parameters_config(
        self,
    ) -> dict[str, Any]:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Retrieving default dynamic configuration for Online Store REST Client."
            )
        url = furl(self._get_rondb_rest_server_endpoint())
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Default RonDB Rest Server host and port: {url.host}:{url.port}"
            )
            _logger.debug(
                f"Using CA Certs from Hopsworks Client: {client._get_instance()._get_ca_chain_path()}"
            )
        return {
            self.HOST: url.host,
            self.PORT: url.port,
            self.CA_CERTS: client._get_instance()._get_ca_chain_path(),
        }

    def _get_rondb_rest_server_endpoint(self) -> str:
        """Retrieve RonDB Rest Server endpoint based on whether the client is running internally or externally.

        If the client is running externally, the endpoint is retrieved via the loadbalancer.
        If the client is running internally, the endpoint is retrieved via (consul) service discovery.
        The default port for the RonDB Rest Server is 4406 and always used unless specifying a different port
        in the configuration.

        Returns:
            str: RonDB Rest Server endpoint with default port.
        """
        if client._is_external():
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "External Online Store REST Client : Retrieving RonDB Rest Server endpoint via loadbalancer."
                )
            external_domain = self.variable_api._get_loadbalancer_external_domain(
                "online_store_rest_server"
            )
            default_url = f"https://{external_domain}:{self._DEFAULT_ONLINE_STORE_REST_CLIENT_PORT}"
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"External Online Store REST Client : Default RonDB Rest Server endpoint: {default_url}"
                )
            return default_url
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Internal Online Store REST Client : Retrieving RonDB Rest Server endpoint via service discovery."
            )
        service_discovery_domain = self.variable_api._get_service_discovery_domain()
        if service_discovery_domain == "":
            raise FeatureStoreException(
                "Client could not get Online Store hostname from service_discovery_domain. "
                "The variable is either not set or empty in Hopsworks cluster configuration."
            )
        default_url = f"https://rdrs.service.{service_discovery_domain}:{self._DEFAULT_ONLINE_STORE_REST_CLIENT_PORT}"
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Internal Online Store REST Client : Default RonDB Rest Server endpoint: {default_url}"
            )
        return default_url

    def _setup_pool(self) -> None:
        """Build the connection pool every request goes through.

        urllib3 is the pool Requests is a layer over, and going to it directly
        costs the calling thread roughly half the CPU per request for the small
        responses a feature vector read returns.

        Proxies are honoured. Requests reads `HTTP_PROXY`, `HTTPS_PROXY` and
        `NO_PROXY` from the environment, so the same rules are applied here
        rather than dropped: going straight to the host on a deployment whose
        operator configured a proxy would be a silent change of behaviour.
        """
        verify = self._current_config[self.VERIFY_CERTS]
        options = {
            "maxsize": self._max_connections,
            "cert_reqs": "CERT_REQUIRED" if verify else "CERT_NONE",
            "ca_certs": self._current_config[self.CA_CERTS] if verify else None,
            # The pool is sized to the reads allowed in flight, so it never has
            # to make a connection it will throw away.
            "block": False,
        }
        proxy = self._environment_proxy()
        if proxy:
            _logger.debug("Sending online store requests through proxy %s", proxy)
            self._pool = urllib3.ProxyManager(proxy, **options)
        else:
            self._pool = urllib3.PoolManager(num_pools=2, **options)

    def _environment_proxy(self) -> str | None:
        """The proxy the environment names for the online store host, if any.

        Delegated to Requests because `NO_PROXY` has more rules than reading one
        variable, and this client already depends on it.
        """
        url = self._base_url.url
        proxies = requests.utils.get_environ_proxies(url)
        return proxies.get(self._base_url.scheme) or proxies.get("all")

    def _auth_headers(self) -> dict[str, str]:
        """The headers the auth object would have added, resolved once.

        The auth object is a Requests callable, and all it does is write
        headers, so it is given something with headers to write rather than a
        prepared request it would otherwise need a URL to build.
        """
        if self._auth is None:
            return {}

        class _HeadersOnly:
            headers: dict[str, str] = {}

        carrier = _HeadersOnly()
        carrier.headers = {}
        self._auth(carrier)
        return dict(carrier.headers)

    @staticmethod
    def _as_seconds(timeout: float) -> float:
        """Read a configured timeout as seconds, keeping its historical interpretation.

        A configured value of 500 or more has always been taken as
        milliseconds. Resolved once at configuration time rather than on every
        request, and not applied to a timeout a caller passes per call, which is
        seconds.
        """
        return timeout if timeout < 500 else timeout / 1000

    def _endpoint_url(self, path_params: list[str]) -> str:
        """The URL for one endpoint, rendered once per client configuration."""
        key = tuple(path_params)
        url = self._endpoint_urls.get(key)
        if url is None:
            built = self._base_url.copy()
            built.path.segments.extend(path_params)
            url = built.url
            self._endpoint_urls[key] = url
        return url

    def _send_request(
        self,
        method: str,
        path_params: list[str],
        headers: dict[str, Any] | None = None,
        data: str | None = None,
        timeout: float | None = None,
    ) -> requests.Response:
        # The clock starts before anything this call does, because all of it is
        # time the caller is waiting: admission, preparing the request, the
        # round trip, and reading the body off the socket.
        started = time.monotonic()
        deadline = self._resolve_timeout(timeout)

        def remaining() -> float:
            return deadline - (time.monotonic() - started)

        # Read once and released by name. A reset replaces the whole set of
        # slots, and a call that acquired from the old one must give it back to
        # the old one: releasing whatever the attribute names by then hands a
        # slot to a pool this call never took one from, which raises on a
        # bounded semaphore and leaves the waiters on the old one short.
        slots = self._connection_slots
        if not slots.acquire(timeout=max(remaining(), 0.0)):
            raise TimeoutError(
                f"Timed out after {deadline} seconds waiting for one of "
                f"{self._max_connections} online store connections. Raise "
                "`max_connections` in the online store REST client "
                "configuration, or lower the number of concurrent reads."
            )
        try:
            url = self._endpoint_url(path_params)
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(f"Sending {method} request to {url}.")
                _logger.debug(f"Provided Data: {data}")
                _logger.debug(f"Provided Headers: {headers}")
            return self._send_through_pool(
                method, url, headers, data, remaining, deadline
            )
        finally:
            slots.release()

    def _send_through_pool(self, method, url, headers, data, remaining, deadline):
        """Send through the pool, and answer with what callers already read.

        Callers read `.status_code`, `.json()`, `.content`, `.text` and `.url`,
        so the result is a `requests.Response`; building one costs a few
        microseconds and keeps every caller unchanged.
        """
        self._raise_if_spent(remaining(), deadline)
        sent = dict(self._auth_header_cache)
        if headers:
            sent.update(headers)
        body = data.encode() if isinstance(data, str) else data
        try:
            raw = self._pool.request(
                method,
                url,
                body=body,
                headers=sent,
                timeout=urllib3.Timeout(total=max(remaining(), 0.001)),
                retries=False,
                preload_content=True,
            )
        except urllib3.exceptions.TimeoutError as error:
            raise TimeoutError(
                f"The online store did not answer within {deadline} seconds."
            ) from error
        except urllib3.exceptions.HTTPError as error:
            # Requests wraps urllib3 the same way, so a caller that handles a
            # connection failure keeps handling it.
            raise requests.exceptions.ConnectionError(error) from error
        if remaining() <= 0:
            raise TimeoutError(
                f"The online store was still answering after {deadline} seconds."
            )
        response = requests.Response()
        response.status_code = raw.status
        response.headers.update(raw.headers)
        response.url = url
        response.encoding = "utf-8"
        response._content = raw.data
        response._content_consumed = True
        return response

    def _resolve_timeout(self, timeout: float | None) -> float:
        """The deadline for one call, in seconds.

        A caller's timeout is seconds and is taken as given; only the configured
        default carries the historical millisecond reading. Unset means that
        configured default, so every call has a deadline. It has to be a real
        length of time: zero, negative and NaN each describe a call that cannot
        succeed, and passing them through would have produced a timeout whose
        behaviour depends on which layer looked at it first.
        """
        if timeout is None:
            return self._timeout_seconds
        try:
            seconds = float(timeout)
        except (TypeError, ValueError) as error:
            raise ValueError(f"`timeout` must be a number, got {timeout!r}.") from error
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError(
                f"`timeout` must be a finite number of seconds greater than zero, "
                f"got {timeout!r}."
            )
        return seconds

    @staticmethod
    def _raise_if_spent(remaining: float, deadline: float) -> None:
        if remaining <= 0:
            raise TimeoutError(
                f"The online store call ran out of its {deadline} seconds before "
                "the request was sent."
            )

    def _check_hopsworks_connection(self) -> None:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Checking Hopsworks connection.")
        assert (
            client._get_instance() is not None and client._get_instance()._connected
        ), """Hopsworks Client is not connected. Please connect to Hopsworks cluster
            via hopsworks.login before initialising the Online Store REST Client.
            """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Hopsworks connection is active.")

    def _set_auth(self, optional_config: dict[str, Any] | None = None) -> None:
        """Set authentication object for the Online Store REST Client.

        RonDB Rest Server uses Hopsworks Api Key to authenticate requests via the X-API-KEY header by default.
        The api key determines the permissions of the user making the request for access to a given Feature Store.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Setting authentication for Online Store REST Client.")
        if client._is_external():
            assert hasattr(client._get_instance()._auth, "_token"), (
                "External client must use API Key authentication. Contact your system administrator."
            )
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "External Online Store REST Client : Setting authentication using Hopsworks Client API Key."
                )
            self._auth = client.auth.OnlineStoreKeyAuth(
                client._get_instance()._auth._token
            )
        elif isinstance(optional_config, dict) and optional_config.get(
            self.API_KEY, False
        ):
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Setting authentication using provided API Key from optional configuration."
                )
            self._auth = client.auth.OnlineStoreKeyAuth(optional_config[self.API_KEY])
        elif hasattr(self, "_auth") and self._auth is not None:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Authentication for Online Store REST Client is already set. Using existing authentication api key."
                )
        else:
            raise FeatureStoreException(
                "RonDB Rest Server uses Hopsworks Api Key to authenticate request."
                f"Provide a configuration with the {self.API_KEY} key."
            )

    def _is_connected(self):
        """If Online Store Rest Client is initialised, ping RonDB Rest Server to ensure connection is active."""
        if self._session is None:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    "Checking Online Store REST Client is connected. Session is not initialised."
                )
            raise FeatureStoreException("Online Store REST Client is not initialised.")
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Checking Online Store REST Client is connected. Pinging RonDB Rest Server."
            )
        if not self._send_request("GET", ["ping"]):
            warn("Ping failed, RonDB Rest Server is not reachable.", stacklevel=2)
            return False
        return True

    @property
    def session(self) -> requests.Session:
        """Requests session object used to send requests to the Online Store REST API."""
        return self._session

    @property
    def base_url(self) -> furl:
        """Base URL for the Online Store REST API.

        This the url of the RonDB REST Server and should not be confused with the Opensearch Vector DB which also serves as an Online Store for features belonging to Feature Group containing embeddings.
        """
        return self._base_url

    @property
    def current_config(self) -> dict[str, Any]:
        """Current configuration of the Online Store REST Client."""
        return self._current_config

    @property
    def auth(self) -> client.auth.OnlineStoreKeyAuth:
        """Authentication object used to authenticate requests to the Online Store REST API.

        Extends the requests.auth.AuthBase class.
        """
        return self._auth
