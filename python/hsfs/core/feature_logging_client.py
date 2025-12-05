#
#   Copyright 2025 Hopsworks AB
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
from typing import Any

import httpx
from furl import furl


_logger = logging.getLogger(__name__)

_client = None


def init_client(
    config: dict[str, Any] | None = None,
):
    global _client
    if not _client:
        _client = FeatureLoggingClientSingleton(config=config)
    else:
        _logger.warning("Feature Logging Client is already initialized.")


def get_instance() -> FeatureLoggingClientSingleton:
    global _client
    if _client is None:
        _logger.warning(
            "Feature Logging Client is not initialized. Initializing with default configuration."
        )
        _client = FeatureLoggingClientSingleton()
    _logger.debug("Accessing global Feature Logging Client instance.")
    return _client


class FeatureLoggingClientSingleton:
    _DEFAULT_TIMEOUT_SECONDS = 2
    _DEFAULT_POOL_SIZE = 5
    _DEFAULT_SCHEME = "http"
    _DEFAULT_HOST = "localhost"
    _DEFAULT_PORT = 9099

    def __init__(
        self,
        config: dict[str, Any] | None = None,
    ):
        _logger.debug("Initializing Feature Logging Client.")
        self._current_config = self._get_default_client_config()
        if config:
            _logger.debug(f"Config: {config!r}")
            self._current_config.update(
                {k: v for k, v in config.items() if v is not None}
            )
        self._setup_rest_client()

    def _setup_rest_client(self):
        _logger.debug("Setting up HTTPX client.")
        base_url = self._build_base_url()
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=self._current_config["timeout"],
            limits=httpx.Limits(max_connections=self._current_config["pool_size"]),
        )

    def _build_base_url(self) -> str:
        url = furl()
        url.scheme = self._current_config.get("scheme", "http")
        url.host = self._current_config["host"]
        url.port = self._current_config.get("port", None)
        return url.url

    def _get_default_client_config(self) -> dict[str, Any]:
        return {
            "scheme": self._DEFAULT_SCHEME,
            "host": self._DEFAULT_HOST,
            "port": self._DEFAULT_PORT,
            "timeout": self._DEFAULT_TIMEOUT_SECONDS,
            "pool_size": self._DEFAULT_POOL_SIZE,
        }

    async def post(
        self, json_data: str, endpoint: str = "", headers=None
    ) -> httpx.Response:
        url = f"{self._client.base_url}/{endpoint.lstrip('/')}"
        _logger.debug(f"Performing POST request to {url}")
        response = await self._client.post(url, headers=headers, content=json_data)
        response.raise_for_status()
        return response

    async def close(self):
        global _client
        _logger.debug("Closing HTTPX client.")
        await self._client.aclose()
        _client = None
