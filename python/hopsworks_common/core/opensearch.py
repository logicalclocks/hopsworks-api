#
#   Copyright 2023 Logical Clocks AB
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

import contextlib
import logging
import re
import threading
from functools import wraps

import opensearchpy
import urllib3
from hopsworks_common.client.exceptions import (
    FeatureStoreException,
    VectorDatabaseException,
)
from hopsworks_common.core.opensearch_api import OpenSearchApi
from opensearchpy import OpenSearch
from opensearchpy.exceptions import (
    AuthenticationException,
    ConnectionError,
    ConnectionTimeout,
    RequestError,
)
from retrying import retry


def _is_timeout(exception):
    return isinstance(
        exception, (urllib3.exceptions.ReadTimeoutError, ConnectionTimeout)
    )


def _handle_opensearch_exception(func):
    @wraps(func)
    def error_handler_wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except (ConnectionError, AuthenticationException):
            # OpenSearchConnectionError occurs when connection is closed.
            # OpenSearchAuthenticationException occurs when jwt is expired
            # args[0] is 'self' - the ProjectOpenSearchClient instance
            client_wrapper = args[0] if args else None
            if client_wrapper and isinstance(client_wrapper, ProjectOpenSearchClient):
                feature_store_id = getattr(client_wrapper, "_feature_store_id", None)
                # Get singleton instance and refresh the connection
                singleton = OpenSearchClientSingleton.get_instance()
                if singleton:
                    singleton._refresh_opensearch_connection(feature_store_id)
                    # Update the wrapper's client reference
                    client_wrapper._opensearch_client = singleton._get_or_create_client(
                        feature_store_id
                    )
            return func(*args, **kw)
        except RequestError as e:
            caused_by = e.info.get("error") and e.info["error"].get("caused_by")
            if caused_by and caused_by["type"] == "illegal_argument_exception":
                client_wrapper = args[0] if args else None
                if client_wrapper and isinstance(
                    client_wrapper, ProjectOpenSearchClient
                ):
                    raise client_wrapper._create_vector_database_exception(
                        caused_by["reason"]
                    ) from e
                raise VectorDatabaseException(
                    VectorDatabaseException.OTHERS,
                    f"Error in Opensearch request: {caused_by['reason']}",
                    e.info,
                ) from e
            raise VectorDatabaseException(
                VectorDatabaseException.OTHERS,
                f"Error in Opensearch request: {e}",
                e.info,
            ) from e
        except Exception as e:
            if _is_timeout(e):
                raise FeatureStoreException(
                    ProjectOpenSearchClient.TIMEOUT_ERROR_MSG
                ) from e
            raise e

    return error_handler_wrapper


class OpensearchRequestOption:
    DEFAULT_OPTION_MAP = {
        "timeout": "30s",
    }

    DEFAULT_OPTION_MAP_V2_3 = {
        # starting from opensearch client v2.3, timeout should be in int/float
        # https://github.com/opensearch-project/opensearch-py/pull/394
        "timeout": 30,
    }

    @classmethod
    def get_version(cls):
        return opensearchpy.__version__[0:2]

    @classmethod
    def get_options(cls, options: dict):
        """Construct a map of options for the request to the vector database.

        Args:
            options (dict): The options used for the request to the vector database.
                The keys are attribute values of the OpensearchRequestOption class.

        Returns:
            dict: A dictionary containing the constructed options map, where keys represent
            attribute values of the OpensearchRequestOption class, and values are obtained
            either from the provided options or default values if not available.
        """
        default_option = (
            cls.DEFAULT_OPTION_MAP
            if cls.get_version() < (2, 3)
            else cls.DEFAULT_OPTION_MAP_V2_3
        )
        if options:
            # make lower case to avoid issues with cases
            options = {k.lower(): v for k, v in options.items()}
            new_options = {}
            for option, value in default_option.items():
                if option in options:
                    if (
                        option == "timeout"
                        and cls.get_version() < (2, 3)
                        and isinstance(options[option], int)
                    ):
                        new_options[option] = f"{options[option]}s"
                    else:
                        new_options[option] = options[option]
                else:
                    new_options[option] = value
            return new_options
        return default_option


class ProjectOpenSearchClient:
    """Wrapper for OpenSearch client associated with a specific project.

    Thread-safe and can be used concurrently.
    """

    TIMEOUT_ERROR_MSG = """
    Cannot fetch results from Opensearch due to timeout. It is because the server is busy right now or longer time is needed to reload a large index. Try and increase the timeout limit by providing the parameter `options={"timeout": 60}` in the method `find_neighbor` or `count`.
    """

    def __init__(self, opensearch_client: OpenSearch, feature_store_id: int = None):
        self._opensearch_client = opensearch_client
        self._feature_store_id = feature_store_id

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_is_timeout,
    )
    @_handle_opensearch_exception
    def search(self, index=None, body=None, options=None):
        return self._opensearch_client.search(
            body=body, index=index, params=OpensearchRequestOption.get_options(options)
        )

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_is_timeout,
    )
    @_handle_opensearch_exception
    def count(self, index, body=None, options=None):
        result = self._opensearch_client.count(
            index=index, body=body, params=OpensearchRequestOption.get_options(options)
        )
        return result["count"]

    def close(self):
        """Close the underlying OpenSearch client."""
        if self._opensearch_client:
            self._opensearch_client.close()

    def _create_vector_database_exception(self, message):
        """Create appropriate VectorDatabaseException based on error message."""
        if "[knn] requires k" in message:
            pattern = r"\[knn\] requires k <= (\d+)"
            match = re.search(pattern, message)
            if match:
                k = match.group(1)
                reason = VectorDatabaseException.REQUESTED_K_TOO_LARGE
                message = (
                    f"Illegal argument in vector database request: "
                    f"Requested k is too large, it needs to be less than {k}."
                )
                info = {VectorDatabaseException.REQUESTED_K_TOO_LARGE_INFO_K: int(k)}
            else:
                reason = VectorDatabaseException.REQUESTED_K_TOO_LARGE
                message = "Illegal argument in vector database request: Requested k is too large."
                info = {}
        elif "Result window is too large" in message:
            pattern = r"or equal to: \[(\d+)\]"
            match = re.search(pattern, message)
            if match:
                n = match.group(1)
                reason = VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE
                message = (
                    f"Illegal argument in vector database request: "
                    f"Requested n is too large, it needs to be less than {n}."
                )
                info = {
                    VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE_INFO_N: int(
                        n
                    )
                }
            else:
                reason = VectorDatabaseException.REQUESTED_NUM_RESULT_TOO_LARGE
                message = (
                    "Illegal argument in vector database request: "
                    "Requested n is too large."
                )
                info = {}
        else:
            reason = VectorDatabaseException.OTHERS
            message = message
            info = {}
        return VectorDatabaseException(reason, message, info)

    def get_opensearch_client(self):
        return self._opensearch_client

    @property
    def feature_store_id(self):
        return self._feature_store_id


class OpenSearchClientSingleton:
    """Thread-safe singleton manager for OpenSearch clients.

    Caches clients per feature_store_id and returns
    ProjectOpenSearchClient wrappers.
    """

    _instance = None
    _clients_cache = {}  # Cache OpenSearch clients by feature_store_id
    _cache_lock = threading.RLock()  # Reentrant lock for thread-safe cache access
    _federated_connector_cache = {}  # Cache federated connector check results

    TIMEOUT_ERROR_MSG = ProjectOpenSearchClient.TIMEOUT_ERROR_MSG
    FEDERATED_CONNECTOR_NAME = "opensearch_connector"
    DEFAULT_CACHE_KEY = "default"

    def __new__(cls, feature_store_id: int = None):
        if not cls._instance:
            with cls._cache_lock if hasattr(cls, "_cache_lock") else threading.RLock():
                if not cls._instance:  # Double-check locking
                    cls._instance = super().__new__(cls)
                    cls._instance._cache_lock = threading.RLock()
                    cls._instance._clients_cache = {}
                    cls._instance._federated_connector_cache = {}

                    # query log is at INFO level
                    # 2023-11-24 15:10:49,470 INFO: POST https://localhost:9200/index/_search [status:200 request:0.041s]
                    logging.getLogger("opensearchpy").setLevel(logging.WARNING)
                    logging.getLogger("opensearch").setLevel(logging.WARNING)

        # Return a ProjectOpenSearchClient wrapper for the requested feature_store_id
        return cls._instance._get_client_wrapper(feature_store_id)

    def _get_client_wrapper(self, feature_store_id: int = None):
        """Get or create a ProjectOpenSearchClient wrapper for the given feature_store_id."""
        opensearch_client = self._get_or_create_client(feature_store_id)
        return ProjectOpenSearchClient(opensearch_client, feature_store_id)

    def _get_federated_opensearch_config(self, feature_store_id: int) -> dict | None:
        """Try to fetch the federated_opensearch storage connector and return its config.

        Returns None if connector doesn't exist or isn't an OpenSearch connector.
        """
        cache_key = f"fs_{feature_store_id}"

        # Check cache first
        with self._cache_lock:
            if cache_key in self._federated_connector_cache:
                return self._federated_connector_cache[cache_key]

        try:
            # Import here to avoid circular imports
            from hsfs.core import storage_connector_api

            connector_api = storage_connector_api.StorageConnectorApi()
            connector = connector_api.get(
                feature_store_id, self.FEDERATED_CONNECTOR_NAME
            )

            if connector is None:
                # Connector doesn't exist
                with self._cache_lock:
                    self._federated_connector_cache[cache_key] = None
                return None

            # Check if it's an OpenSearch connector
            from hsfs.storage_connector import OpenSearchConnector

            if not isinstance(connector, OpenSearchConnector):
                logging.debug(
                    f"Storage connector '{self.FEDERATED_CONNECTOR_NAME}' exists but is not an OpenSearch connector. "
                    f"Using default OpenSearch configuration."
                )
                with self._cache_lock:
                    self._federated_connector_cache[cache_key] = None
                return None

            config = connector.connector_options()

            # Cache the config
            with self._cache_lock:
                self._federated_connector_cache[cache_key] = config

            logging.info(
                f"Using federated OpenSearch connector '{self.FEDERATED_CONNECTOR_NAME}' "
                f"with host: {connector.host}"
            )
            return config

        except Exception as e:
            # Log the error but fall back to default config
            logging.debug(
                f"Could not fetch federated OpenSearch connector '{self.FEDERATED_CONNECTOR_NAME}': {e}. "
                f"Using default OpenSearch configuration."
            )
            with self._cache_lock:
                self._federated_connector_cache[cache_key] = None
            return None

    def _get_or_create_client(self, feature_store_id: int = None):
        """Get cached client or create new one. Thread-safe.

        Cache key:
          - feature_store_id, if provided
          - DEFAULT_CACHE_KEY, otherwise

        If no federated connector config is found for a given feature store,
        the client for that feature store reuses the DEFAULT cache client.
        """
        if feature_store_id is not None:
            cache_key = feature_store_id
        else:
            cache_key = self.DEFAULT_CACHE_KEY

        with self._cache_lock:
            if cache_key not in self._clients_cache:
                # Try to get federated connector config first
                opensearch_config = None
                if feature_store_id is not None:
                    try:
                        opensearch_config = self._get_federated_opensearch_config(
                            feature_store_id
                        )
                    except Exception as e:
                        logging.debug(
                            f"Could not get federated connector config for feature store {feature_store_id}: {e}"
                        )

                if opensearch_config is not None:
                    # Dedicated client for this feature store
                    self._clients_cache[cache_key] = OpenSearch(**opensearch_config)
                else:
                    # Fall back to DEFAULT client and reuse it
                    self._clients_cache[cache_key] = self._clients_cache.get(
                        self.DEFAULT_CACHE_KEY,
                        OpenSearch(
                            **OpenSearchApi().get_default_py_config(feature_store_id)
                        ),
                    )

        return self._clients_cache[cache_key]

    def _refresh_opensearch_connection(self, feature_store_id: int = None):
        """Refresh the OpenSearch connection for a specific cache key. Thread-safe."""
        cache_key = feature_store_id if feature_store_id is not None else "default"

        with self._cache_lock:
            # Close and remove the cached client
            if cache_key in self._clients_cache:
                with contextlib.suppress(Exception):
                    self._clients_cache[cache_key].close()
                del self._clients_cache[cache_key]

            # Clear federated connector cache for this feature store to force re-check
            if feature_store_id is not None:
                fs_cache_key = f"fs_{feature_store_id}"
                if fs_cache_key in self._federated_connector_cache:
                    del self._federated_connector_cache[fs_cache_key]

        # Recreate the client using _get_or_create_client which will check for federated connector
        self._get_or_create_client(feature_store_id)

    @classmethod
    def close_all(cls):
        """Close all cached OpenSearch clients. Thread-safe."""
        if cls._instance and hasattr(cls._instance, "_clients_cache"):
            with cls._instance._cache_lock:
                for client in list(cls._instance._clients_cache.values()):
                    with contextlib.suppress(Exception):
                        client.close()
                cls._instance._clients_cache.clear()
                # Also clear federated connector cache
                if hasattr(cls._instance, "_federated_connector_cache"):
                    cls._instance._federated_connector_cache.clear()

    @classmethod
    def get_instance(cls) -> OpenSearchClientSingleton:
        """Get the singleton instance."""
        return cls._instance
