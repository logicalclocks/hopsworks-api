#
#   Copyright 2021 Logical Clocks AB
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

import hopsworks_common.connection
from hopsworks_common import usage
from hopsworks_common.connection import (
    HOPSWORKS_PORT_DEFAULT,
    HOSTNAME_VERIFICATION_DEFAULT,
)
from hopsworks_common.decorators import connected, not_connected


__all__ = [
    "Connection",
    "HOPSWORKS_PORT_DEFAULT",
    "HOSTNAME_VERIFICATION_DEFAULT",
]


class Connection(hopsworks_common.connection.Connection):
    """A Hopsworks Model Management connection object.

    The connection is project specific, so you can access the project's own Model Registry and Model Serving.

    This class provides convenience classmethods accessible from the `hsml`-module:

    !!! example "Connection factory"
        For convenience, `hsml` provides a factory method, accessible from the top level
        module, so you don't have to import the `Connection` class manually:

        ```python
        import hsml
        conn = hsml.connection()
        ```

    !!! hint "Save API Key as File"
        To get started quickly, you can simply create a file with the previously
         created Hopsworks API Key and place it on the environment from which you
         wish to connect to Hopsworks.

        You can then connect by simply passing the path to the key file when
        instantiating a connection:

        ```python hl_lines="6"
            import hsml
            conn = hsml.connection(
                'my_instance',                      # DNS of your Hopsworks instance
                443,                                # Port to reach your Hopsworks instance, defaults to 443
                'my_project',                       # Name of your Hopsworks project
                api_key_file='modelregistry.key',   # The file containing the API key generated above
                hostname_verification=True)         # Disable for self-signed certificates
            )
            mr = conn.get_model_registry()          # Get the project's default model registry
            ms = conn.get_model_serving()           # Uses the previous model registry
        ```

    Clients in external clusters need to connect to the Hopsworks Model Registry and Model Serving using an
    API key. The API key is generated inside the Hopsworks platform, and requires at
    least the "project", "modelregistry", "dataset.create", "dataset.view", "dataset.delete", "serving" and "kafka" scopes
    to be able to access a model registry and its model serving.
    For more information, see the [integration guides](../../integrations/overview.md).

    # Arguments
        host: The hostname of the Hopsworks instance, defaults to `None`.
        port: The port on which the Hopsworks instance can be reached,
            defaults to `443`.
        project: The name of the project to connect to. When running on Hopsworks, this
            defaults to the project from where the client is run from.
            Defaults to `None`.
        hostname_verification: Whether or not to verify Hopsworks certificate, defaults
            to `True`.
        trust_store_path: Path on the file system containing the Hopsworks certificates,
            defaults to `None`.
        api_key_file: Path to a file containing the API Key.
        api_key_value: API Key as string, if provided, however, this should be used with care,
        especially if the used notebook or job script is accessible by multiple parties. Defaults to `None`.

    # Returns
        `Connection`. Connection handle to perform operations on a Hopsworks project.
    """

    @not_connected
    def connect(self):
        """Instantiate the connection.

        Creating a `Connection` object implicitly calls this method for you to
        instantiate the connection. However, it is possible to close the connection
        gracefully with the `close()` method, in order to clean up materialized
        certificates. This might be desired when working on external environments.
        Subsequently you can call `connect()` again to reopen the connection.

        !!! example
            ```python
            import hsml
            conn = hsml.connection()
            conn.close()
            conn.connect()
            ```
        """
        super().connect()

        from hsml.core import model_api, model_registry_api, model_serving_api

        self._model_api = model_api.ModelApi()
        self._model_registry_api = model_registry_api.ModelRegistryApi()
        self._model_serving_api = model_serving_api.ModelServingApi()
        self._model_serving_api.load_default_configuration()  # istio client, default resources,...

    @usage.method_logger
    @connected
    def get_model_registry(self, project: str = None):
        """Get a reference to a model registry to perform operations on, defaulting to the project's default model registry.
        Shared model registries can be retrieved by passing the `project` argument.

        # Arguments
            project: The name of the project that owns the shared model registry,
            the model registry must be shared with the project the connection was established for, defaults to `None`.
        # Returns
            `ModelRegistry`. A model registry handle object to perform operations on.
        """
        return self._model_registry_api.get(project)

    @usage.method_logger
    @connected
    def get_model_serving(self):
        """Get a reference to model serving to perform operations on. Model serving operates on top of a model registry, defaulting to the project's default model registry.

        !!! example
            ```python

            import hopsworks

            project = hopsworks.login()

            ms = project.get_model_serving()
            ```

        # Returns
            `ModelServing`. A model serving handle object to perform operations on.
        """
        return self._model_serving_api.get()

    def close(self):
        """Close a connection gracefully.

        This will clean up any materialized certificates on the local file system of
        external environments.

        Usage is recommended but optional.
        """
        from hsml import client

        client.stop()
        self._model_api = None
        self._connected = False
        print("Connection closed.")
