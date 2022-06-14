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

from hopsworks.client.exceptions import RestAPIError

from hopsworks.connection import Connection

# Needs to run before import of hsml and hsfs
warnings.filterwarnings(action="ignore", category=UserWarning, module=r".*psycopg2")

import hsml  # noqa: F401, E402
import hsfs  # noqa: F401, E402

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


def login(project: str = None, api_key_value: str = None, api_key_file: str = None):
    """Connect to managed Hopsworks.

    ```python

    import hopsworks

    hopsworks.login()

    ```
    # Arguments
        project: Name of the project to access.
        api_key_value: Value of the Api Key
        api_key_file: Path to file wih Api Key
    # Returns
        `Project`: The Project object
    # Raises
        `RestAPIError`: If unable to connect to Hopsworks
    """

    global _saas_connection

    if "HOPSWORKS_API_KEY" in os.environ:
        api_key_value = os.environ["HOPSWORKS_API_KEY"]

    if "HOPSWORKS_PROJECT" in os.environ:
        project = os.environ["HOPSWORKS_PROJECT"]

    host = "c.app.hopsworks.ai"
    if "HOPSWORKS_HOST" in os.environ:
        host = os.environ["HOPSWORKS_HOST"]

    port = 443
    if "HOPSWORKS_PORT" in os.environ:
        port = os.environ["HOPSWORKS_PORT"]

    # If already logged in, should reset connection and follow login procedure as Connection may no longer be valid
    logout()

    api_key_path = os.getcwd() + "/.hw_api_key"
    api_key_val = None
    # If user supplied the api key directly
    if api_key_value is not None:
        api_key_val = api_key_value
    # If user supplied the api key in a file
    elif api_key_file is not None:
        file = None
        if os.path.exists(api_key_file):
            try:
                file = open(api_key_file, mode="r")
                api_key_val = file.read()
            finally:
                file.close()
        else:
            raise IOError(
                "Could not find api key file on path: {}".format(api_key_file)
            )
    elif os.path.exists(api_key_path):
        try:
            _saas_connection = _saas_connection(
                host=host, port=port, api_key_file=api_key_path
            )
            project_obj = _prompt_project(_saas_connection, project)
            print("\nLogged in to project, explore it here " + project_obj.get_url())
            return project_obj
        except RestAPIError:
            logout()
            # API Key may be invalid, have the user supply it again
            os.remove(api_key_path)

    if api_key_val is None:
        print(
            "Copy your Api Key (first register/login): https://c.app.hopsworks.ai/account/api/generated"
        )
        api_key_val = input("\nPaste it here: ")
        # If api key was provided as input, save the API key locally on disk to avoid users having to enter it again in the same environment
        api_key_file = open(api_key_path, "w")
        api_key_file.write(api_key_val)
        api_key_file.close()

    try:
        _saas_connection = _saas_connection(
            host=host, port=port, api_key_value=api_key_val
        )
        project_obj = _prompt_project(_saas_connection, project)
    except RestAPIError as e:
        logout()
        raise e

    print("\nLogged in to project, explore it here " + project_obj.get_url())
    return project_obj


def _prompt_project(valid_connection, project):
    saas_projects = valid_connection.get_projects()
    if project is None:
        if len(saas_projects) == 0:
            raise Exception("Could not find any project")
        elif len(saas_projects) == 1:
            return saas_projects[0]
        else:
            while True:
                print("\nMultiple projects found. \n")
                for index in range(len(saas_projects)):
                    print("\t (" + str(index + 1) + ") " + saas_projects[index].name)
                while True:
                    project_index = input("\nEnter project to access: ")
                    # Handle invalid input type
                    try:
                        project_index = int(project_index)
                        # Handle negative indexing
                        if project_index <= 0:
                            print("Invalid input, must be greater than or equal to 1")
                            continue
                        # Handle index out of range
                        try:
                            return saas_projects[project_index - 1]
                        except IndexError:
                            print(
                                "Invalid input, should be an integer from the list of projects."
                            )
                    except ValueError:
                        print(
                            "Invalid input, should be an integer from the list of projects."
                        )
    else:
        for proj in saas_projects:
            if proj.name == project:
                return proj
        raise Exception("Could not find project {}".format(project))


def logout():
    global _saas_connection
    if type(_saas_connection) is Connection:
        _saas_connection.close()
    _saas_connection = Connection.connection
