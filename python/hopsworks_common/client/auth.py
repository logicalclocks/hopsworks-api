#
#   Copyright 2020 Logical Clocks AB
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

import os

import requests
from hopsworks_common.client import exceptions


class BearerAuth(requests.auth.AuthBase):
    """Class to encapsulate a Bearer token."""

    def __init__(self, token: str) -> None:
        self._token = token.strip()

    def __call__(self, r: requests.Request) -> requests.Request:
        r.headers["Authorization"] = "Bearer " + self._token
        return r


class ApiKeyAuth(requests.auth.AuthBase):
    """Class to encapsulate an API key."""

    def __init__(self, token: str) -> None:
        self._token = token.strip()

    def __call__(self, r: requests.Request) -> requests.Request:
        r.headers["Authorization"] = "ApiKey " + self._token
        return r


class OnlineStoreKeyAuth(requests.auth.AuthBase):
    """Class to encapsulate an API key."""

    def __init__(self, token):
        self._token = token.strip()

    def __call__(self, r):
        r.headers["X-API-KEY"] = self._token
        return r


def get_api_key(api_key_value, api_key_file):
    if api_key_value is not None:
        return api_key_value
    elif api_key_file is not None:
        file = None
        if os.path.exists(api_key_file):
            try:
                file = open(api_key_file, mode="r")
                return file.read()
            finally:
                file.close()
        else:
            raise IOError(
                "Could not find api key file on path: {}".format(api_key_file)
            )
    else:
        raise exceptions.ExternalClientError(
            "Either api_key_file or api_key_value must be set when connecting to"
            " hopsworks from an external environment."
        )
