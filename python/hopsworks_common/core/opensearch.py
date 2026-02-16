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

from hopsworks_apigen import also_available_as
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
from hopsworks_common.core.opensearch_api import OPENSEARCH_CONFIG, OpenSearchApi
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
                client_wrapper.refresh_opensearch_connection()
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
            client_wrapper = args[0] if args else None
            # Invalidate the cache for federated connector clients if the error is not a timeout,
            # so that it wont't stuck at a error state.
            if (
                client_wrapper
                and isinstance(client_wrapper, ProjectOpenSearchClient)
                and not client_wrapper.is_cluster_client
            ):
                OpenSearchClientSingleton.invalidate_cache(
                    client_wrapper.feature_store_id, close_opensearch_client=True
                )
            raise e

    return error_handler_wrapper


@also_available_as("hopsworks.core.opensearch.OpensearchRequestOption", "hsfs.core.opensearch.OpensearchRequestOption")
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

    def __init__(
        self,
        opensearch_client: OpenSearch,
        feature_store_id: int = None,
        is_cluster_client: bool = True,
    ):
        self._opensearch_client = opensearch_client
        self._feature_store_id = feature_store_id
        self._is_cluster_client = is_cluster_client
        self._client_lock = threading.RLock()

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_is_timeout,
    )
    @_handle_opensearch_exception
    def search(self, index=None, body=None, options=None):
        return self.get_opensearch_client().search(
            body=body, index=index, params=OpensearchRequestOption.get_options(options)
        )

    @retry(
        wait_exponential_multiplier=1000,
        stop_max_attempt_number=5,
        retry_on_exception=_is_timeout,
    )
    @_handle_opensearch_exception
    def count(self, index, body=None, options=None):
        result = self.get_opensearch_client().count(
            index=index, body=body, params=OpensearchRequestOption.get_options(options)
        )
        return result["count"]

    def refresh_opensearch_connection(self):
        """Refresh the OpenSearch connection for the client."""
        if self.is_cluster_client:
            OpenSearchClientSingleton.get_instance().close_cluster_client()
        else:
            self.get_opensearch_client().close()
        self._opensearch_client = None
        # Recreate the client
        self.get_opensearch_client()

    def close(self):
        """Close the underlying OpenSearch client."""
        # Close the client if it is a federated connector client otherwise the cluster client is closed when python client is closed
        OpenSearchClientSingleton.invalidate_cache(
            self._feature_store_id, close_opensearch_client=self.is_cluster_client
        )

        # For default client, close the cluster client
        if not self._feature_store_id:
            OpenSearchClientSingleton.get_instance().close_cluster_client()
        self._opensearch_client = None

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
        """Get the underlying OpenSearch client."""
        if self.is_cluster_client:
            return (
                OpenSearchClientSingleton.get_instance().get_or_create_cluster_client()
            )
        if (
            self._opensearch_client is None
        ):  # when refreshing the connection, opensearch_client is None
            with self._client_lock:
                if self._opensearch_client is None:
                    _, self._opensearch_client = (
                        OpenSearchClientSingleton.get_instance()._get_or_create_opensearch_client(
                            self._feature_store_id
                        )
                    )
        return self._opensearch_client

    @property
    def feature_store_id(self):
        return self._feature_store_id

    @property
    def is_cluster_client(self):
        return self._is_cluster_client


@also_available_as("hopsworks.core.opensearch.OpenSearchClientSingleton", "hsfs.core.opensearch.OpenSearchClientSingleton")
class OpenSearchClientSingleton:
    """Thread-safe singleton for OpenSearch client reuse.

    Caches:
      - ProjectOpenSearchClient wrappers keyed by feature_store_id (or "default" if no feature store id is provided)
      - Federated connector configs per feature store
      - A single default/cluster OpenSearch client

    Client selection:
      - If a feature store has a federated OpenSearch connector, a dedicated OpenSearch
        client is created with its config and cached under that feature_store_id.
      - If no connector exists (or feature_store_id is None), the shared cluster
        OpenSearch client is used
    """

    _instance = None
    _default_opensearch_client = None
    _wrapper_cache = {}  # Cache ProjectOpenSearchClient wrappers by feature_store_id
    _cache_lock = threading.RLock()  # Reentrant lock for thread-safe cache access
    _federated_connector_cache = {}  # Cache federated connector check results
    _opensearch_api = None

    TIMEOUT_ERROR_MSG = ProjectOpenSearchClient.TIMEOUT_ERROR_MSG
    FEDERATED_CONNECTOR_NAME = "opensearch_connector"
    DEFAULT_CLUSTER_CACHE_KEY = "default"

    def __new__(cls, feature_store_id: int = None):
        if not cls._instance:
            with cls._cache_lock if hasattr(cls, "_cache_lock") else threading.RLock():
                if not cls._instance:  # Double-check locking
                    cls._instance = super().__new__(cls)
                    cls._instance._cache_lock = threading.RLock()
                    cls._instance._default_opensearch_client = None
                    cls._instance._federated_connector_cache = {}
                    cls._instance._opensearch_api = OpenSearchApi()

        # Return a ProjectOpenSearchClient wrapper for the requested feature_store_id
        return cls._instance._get_client_wrapper(feature_store_id)

    def _get_client_wrapper(self, feature_store_id: int = None):
        """Get or create a ProjectOpenSearchClient wrapper for the given feature_store_id."""
        cache_key = (
            f"fs_{feature_store_id}"
            if feature_store_id is not None
            else self.DEFAULT_CLUSTER_CACHE_KEY
        )
        if cache_key in self._wrapper_cache:
            return self._wrapper_cache[cache_key]
        with self._cache_lock:
            if cache_key not in self._wrapper_cache:
                is_cluster_client, opensearch_client = (
                    self._get_or_create_opensearch_client(feature_store_id)
                )
                wrapper = ProjectOpenSearchClient(
                    opensearch_client,
                    feature_store_id,
                    is_cluster_client=is_cluster_client,
                )
                self._wrapper_cache[cache_key] = wrapper
        return self._wrapper_cache[cache_key]

    def _get_federated_opensearch_config(self, feature_store_id: int) -> dict | None:
        """Try to fetch the federated_opensearch storage connector and return its config.

        Returns None if connector doesn't exist or isn't an OpenSearch connector.
        """
        cache_key = f"fs_{feature_store_id}" if feature_store_id else None

        if cache_key and cache_key in self._federated_connector_cache:
            return self._federated_connector_cache[cache_key]

        # Import here to avoid circular imports
        from hsfs.core import storage_connector_api

        connector_api = storage_connector_api.StorageConnectorApi()
        connector = connector_api.get(feature_store_id, self.FEDERATED_CONNECTOR_NAME)

        if connector is None:
            # Connector doesn't exist, do not cache anything
            return self._opensearch_api.get_default_py_config()

        # Check if it's an OpenSearch connector
        from hsfs.storage_connector import OpenSearchConnector

        if not isinstance(connector, OpenSearchConnector):
            logging.debug(
                f"Storage connector '{self.FEDERATED_CONNECTOR_NAME}' exists but is not an OpenSearch connector. "
                f"Using default OpenSearch configuration."
            )
            return None

        config = connector.connector_options()

        # Cache the config
        if cache_key:
            self._federated_connector_cache[cache_key] = config

        logging.debug(
            f"Using federated OpenSearch connector '{self.FEDERATED_CONNECTOR_NAME}' "
            f"with host: {connector.host}"
        )
        return config

    def _get_or_create_opensearch_client(self, feature_store_id: int = None):
        """Get cached client or create new one.

        If no federated connector config is found for a given feature store or no feature store id is provided,
        the client for that feature store reuses the default cluster client.
        """
        # query log is at INFO level
        # 2023-11-24 15:10:49,470 INFO: POST https://localhost:9200/index/_search [status:200 request:0.041s]
        logging.getLogger("opensearchpy").setLevel(logging.WARNING)
        logging.getLogger("opensearch").setLevel(logging.WARNING)

        if feature_store_id is None:
            return True, self.get_or_create_cluster_client()

        # Try to get federated connector config first
        opensearch_config = self._get_federated_opensearch_config(feature_store_id)

        if opensearch_config is None:
            return True, self.get_or_create_cluster_client()

        opensearch_config[OPENSEARCH_CONFIG.HEADERS] = {
            "Authorization": self._opensearch_api._get_authorization_token(
                feature_store_id
            )
        }

        # Dedicated client for this feature store
        return False, OpenSearch(**opensearch_config)

    def get_or_create_cluster_client(self):
        """Get or create a cluster client."""
        if self._default_opensearch_client is not None:
            return self._default_opensearch_client

        self._default_opensearch_client = OpenSearch(
            **OpenSearchApi().get_default_py_config()
        )
        return self._default_opensearch_client

    def close_cluster_client(self):
        """Close the cluster client."""
        with self._cache_lock:
            if self._default_opensearch_client is not None:
                self._default_opensearch_client.close()
                self._default_opensearch_client = None

    @classmethod
    def close_all(cls):
        """Close all cached OpenSearch clients. Thread-safe."""
        if cls._instance and hasattr(cls._instance, "_wrapper_cache"):
            with cls._instance._cache_lock:
                for client in list(cls._instance._wrapper_cache.values()):
                    with contextlib.suppress(Exception):
                        client.close()
                cls._instance._wrapper_cache.clear()
                # Also clear federated connector cache
                if hasattr(cls._instance, "_federated_connector_cache"):
                    cls._instance._federated_connector_cache.clear()
        with contextlib.suppress(Exception):
            cls._instance._default_opensearch_client.close()
            cls._instance._default_opensearch_client = None
        cls._instance = None

    @classmethod
    def get_instance(cls) -> OpenSearchClientSingleton:
        """Get the singleton instance."""
        return cls._instance

    @classmethod
    def invalidate_cache(
        cls, feature_store_id: int = None, close_opensearch_client: bool = False
    ):
        """Invalidate cached ProjectOpenSearchClient and connector config.

        Args:
            feature_store_id: The feature store ID to invalidate cache for.
            close_opensearch_client: Whether to close the cached OpenSearch client when invalidating.
        """
        if cls._instance is None:
            return

        if feature_store_id is not None:
            # Invalidate specific feature store cache
            fs_cache_key = f"fs_{feature_store_id}"
            closed_client = None
            with cls._instance._cache_lock:
                # remove the client from the cache
                if fs_cache_key in cls._instance._wrapper_cache:
                    if close_opensearch_client:
                        with contextlib.suppress(Exception):
                            closed_client = cls._instance._wrapper_cache[
                                fs_cache_key
                            ].get_opensearch_client()
                            closed_client.close()
                    del cls._instance._wrapper_cache[fs_cache_key]

                # Remove connector config cache
                if fs_cache_key in cls._instance._federated_connector_cache:
                    del cls._instance._federated_connector_cache[fs_cache_key]

                logging.debug(
                    f"Invalidated OpenSearch cache for feature store {feature_store_id}"
                )

                # If the closed client is the shared default client, clear it so a new one is created next time.
                if close_opensearch_client and closed_client is not None:
                    with contextlib.suppress(Exception):
                        if cls._instance._default_opensearch_client is closed_client:
                            cls._instance._default_opensearch_client = None
