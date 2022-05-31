#
#   Copyright 2022 Logical Clocks AB
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

import warnings
import logging
import os
import sys
import hsml  # noqa: F401
import hsfs  # noqa: F401

from hopsworks.client.exceptions import RestAPIError

from hopsworks import client

from hopsworks.connection import Connection

connection = Connection.connection

_saas_connection = Connection.connection

def hw_formatwarning(message, category, filename, lineno, line=None):
    return "{}: {}\n".format(category.__name__, message)


warnings.formatwarning = hw_formatwarning

__all__ = ["connection"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)


def login():
    """Connect to managed Hopsworks.

    # Returns
        `Project`: The Project object
    # Raises
        `RestAPIError`: If unable to connect to Hopsworks
    """

    global _saas_connection

    # If already logged in, should reset connection and follow login procedure as Connection may no longer be valid
    if type(_saas_connection) is Connection:
        _saas_connection = Connection.connection

    # TODO: Should not be static
    host="app.hopsworks.ai"
    port=443

    if client.base.Client.REST_ENDPOINT not in os.environ:
        api_key_path = os.getcwd() + "/.hw_api_key"
        if os.path.exists(api_key_path):
            try:
                saas_connection = _saas_connection(host=host, port=port, api_key_file=api_key_path)
                saas_project = saas_connection.get_projects()[0]
                _saas_connection = saas_connection
                return saas_project
            except RestAPIError:
                # API Key may be invalid, have the user supply it again
                os.remove(api_key_path)

        print("Copy your Api Key: https://managed.hopsworks.ai")
        api_key_value = input("\nPaste it here: ")
        api_key_file = open(api_key_path, "w")
        api_key_file.write(api_key_value)
        api_key_file.close()

        saas_connection = _saas_connection(host=host, port=port, api_key_file=api_key_path)
        saas_project = saas_connection.get_projects()[0]
        _saas_connection = saas_connection
        return saas_project
    else:
        raise Exception('Only supported from external environments')

def logout():
    global _saas_connection
    _saas_connection.close()
    _saas_connection = Connection.connection
