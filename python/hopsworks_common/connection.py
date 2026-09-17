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

import importlib
import importlib.util
import logging
import os
import re
import sys
import warnings
import weakref
from typing import TYPE_CHECKING

from hopsworks_apigen import also_available_as
from hopsworks_common import client, constants, usage, util, version
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import (
    hosts_api,
    project_api,
    secret_api,
    services_api,
    variable_api,
)
from hopsworks_common.decorators import _connected, _not_connected
from requests.exceptions import ConnectionError
from typing_extensions import Self


if TYPE_CHECKING:
    from hopsworks_common.project import Project
    from hsfs import feature_store
    from hsml.model_registry import ModelRegistry
    from hsml.model_serving import ModelServing


HOPSWORKS_PORT_DEFAULT = 443
HOSTNAME_VERIFICATION_DEFAULT = os.environ.get(
    "HOPSWORKS_HOSTNAME_VERIFICATION", "False"
).lower() in ("true", "1", "y", "yes")
# alias for backwards compatibility:
HOPSWORKS_HOSTNAME_VERIFICATION_DEFAULT = HOSTNAME_VERIFICATION_DEFAULT
# Backwards compatibility alias
CERT_FOLDER_DEFAULT = constants.CLIENT.CERT_FOLDER_DEFAULT
PROJECT_ID = "HOPSWORKS_PROJECT_ID"
PROJECT_NAME = "HOPSWORKS_PROJECT_NAME"


_hsfs_engine_type = None


_logger = logging.getLogger(__name__)


@also_available_as("hopsworks.connection.Connection")
class Connection:
    """A hopsworks connection object.

    This class provides convenience classmethods accessible from the `hopsworks`-module:

    Example: Connection factory
        For convenience, `hopsworks` provides a factory method, accessible from the top level module, so you don't have to import the `Connection` class manually:

        ```python
        import hopsworks
        conn = hopsworks.connection()
        ```

    Hint: Save API Key as File
        To get started quickly, you can simply create a file with the previously created Hopsworks API Key and place it on the environment from which you wish to connect to Hopsworks.

        You can then connect by simply passing the path to the key file when instantiating a connection:

        ```python hl_lines="6"
        import hopsworks
        conn = hopsworks.connection(
            'my_instance',                 # DNS of your Hopsworks instance
            443,                           # Port to reach your Hopsworks instance, defaults to 443
            api_key_file='hopsworks.key',  # The file containing the API key generated above
            hostname_verification=True)    # Disable for self-signed certificates
        )
        project = hopsworks.login(project="my_project")
        ```

    Clients in external clusters need to connect to the Hopsworks using an API key.
    The API key is generated inside the Hopsworks platform, and requires at least the "project" scope to be able to access a project.

    Parameters:
        host:
            The hostname of the Hopsworks instance in the form of `[UUID].cloud.hopsworks.ai`.
            Do **not** use the url including `https://` when connecting programatically.
        port: The port on which the Hopsworks instance can be reached.
        project:
            The name of the project to connect to.
            When running on Hopsworks, this defaults to the project from where the client is run from.
        engine:
            Specifies the engine to use.
            Possible options are `spark`, `python`, `training`, `spark-no-metastore`, or `spark-delta`.
            The default value, `None`, automatically selects the engine based on the environment:

            - `spark`: Used if Spark is available and the connection is not to Hopsworks SaaS, such as in Hopsworks or Databricks environments.
            - `python`: Used in local Python environments or AWS SageMaker when Spark is not available or the connection is done to Hopsworks SaaS.
            - `training`: Used when only feature store metadata is needed, such as for obtaining training dataset locations and label information during Hopsworks training experiments.
            - `spark-no-metastore`: Functions like "spark" but does not rely on the Hive metastore.
            - `spark-delta`: Minimizes dependencies further by avoiding both Hive metastore and HopsFS.

        hostname_verification: Whether or not to verify Hopsworks' certificate.
        trust_store_path: Path on the file system containing the Hopsworks certificates.
        cert_folder:
            The directory to store retrieved HopsFS certificates.
            Only required when running without a Spark environment.
        api_key_file: Path to a file containing the API Key.
        api_key_value:
            API Key as string.
            If provided, `api_key_file` is ignored; however, this should be used with care, especially if the used notebook or job script is accessible by multiple parties.

    Returns:
        Connection handle to perform operations on a Hopsworks project.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str | None = None,
        engine: str | None = None,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str | None = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str | None = None,
        api_key_value: str | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._project = project
        self._engine = engine
        self._hostname_verification = hostname_verification
        self._trust_store_path = trust_store_path
        self._cert_folder = cert_folder
        self._api_key_file = api_key_file
        self._api_key_value = api_key_value
        self._connected = False
        self._backend_version = None
        self._feature_store_api_cache = None
        self._model_registry_api_cache = None
        self._model_serving_api_cache = None
        self._serving_defaults_loaded = False
        self._serving_defaults_loading = False

        self._connect()

    # The hsfs/hsml API objects are built on first use. Constructing them imports
    # hsfs and hsml (pandas, pyarrow, sqlalchemy, boto: ~0.7 s), which a caller
    # that only needs the REST client, such as the hops CLI, never pays for.
    @property
    def _feature_store_api(self):
        if self._feature_store_api_cache is None:
            from hsfs.core import feature_store_api

            self._feature_store_api_cache = feature_store_api.FeatureStoreApi()
        return self._feature_store_api_cache

    @_feature_store_api.setter
    def _feature_store_api(self, value) -> None:
        self._feature_store_api_cache = value

    @property
    def _model_registry_api(self):
        if self._model_registry_api_cache is None:
            from hsml.core import model_registry_api

            self._model_registry_api_cache = model_registry_api.ModelRegistryApi()
        return self._model_registry_api_cache

    @_model_registry_api.setter
    def _model_registry_api(self, value) -> None:
        self._model_registry_api_cache = value

    @property
    def _model_serving_api(self):
        if self._model_serving_api_cache is None:
            from hsml.core import model_serving_api

            self._model_serving_api_cache = model_serving_api.ModelServingApi()
        return self._model_serving_api_cache

    @_model_serving_api.setter
    def _model_serving_api(self, value) -> None:
        self._model_serving_api_cache = value

    @usage._method_logger
    @_connected
    def _get_feature_store(
        self,
        name: str | None = None,
    ) -> feature_store.FeatureStore:
        """Get a reference to a feature store to perform operations on.

        Defaulting to the project name of default feature store.
        To get a shared feature stores, the project name of the feature store is required.

        Parameters:
            name: The name of the feature store.

        Returns:
            A feature store handle object to perform operations on.
        """
        if not name:
            name = client._get_instance()._project_name
        return self._feature_store_api._get(util._append_feature_store_suffix(name))

    @usage._method_logger
    @_connected
    def _get_model_registry(self, project: str | None = None) -> ModelRegistry:
        """Get a reference to a model registry to perform operations on, defaulting to the project's default model registry.

        Shared model registries can be retrieved by passing the `project` argument.

        Parameters:
            project: The name of the project that owns the shared model registry, the model registry must be shared with the project the connection was established for.

        Returns:
            A model registry handle object to perform operations on.
        """
        return self._model_registry_api._get(project)

    @usage._method_logger
    @_connected
    def _get_model_serving(self) -> ModelServing:
        """Get a reference to model serving to perform operations on. Model serving operates on top of a model registry, defaulting to the project's default model registry.

        Example:
            ```python
            import hopsworks

            project = hopsworks.login()

            ms = project.get_model_serving()
            ```

        Returns:
            A model serving handle object to perform operations on.
        """
        self._load_serving_defaults()
        return self._model_serving_api._get()

    @usage._method_logger
    @_connected
    def _get_secrets_api(self) -> secret_api.SecretsApi:
        """Get the secrets api.

        Returns:
            The Secrets Api handle.
        """
        return self._secret_api

    @usage._method_logger
    @_connected
    def _create_project(
        self,
        name: str,
        description: str | None = None,
        feature_store_topic: str | None = None,
        namespace: str | None = None,
    ) -> Project:
        """Create a new project.

        Example:
            ```python
            import hopsworks

            connection = hopsworks.connection()

            hopsworks.create_project("my_hopsworks_project", description="An example Hopsworks project")
            ```

        Parameters:
            name: The name of the project.
            description: Description of the project, as it is shown in the UI.
            feature_store_topic: Feature store topic name.
            namespace: Kubernetes namespace to use for the project. If ``None``
                the backend derives one from the project name.

        Returns:
            A project handle object to perform operations on.
        """
        return self._project_api._create_project(
            name, description, feature_store_topic, namespace
        )

    @usage._method_logger
    @_connected
    def _get_project(self, name: str | None = None) -> Project:
        """Get an existing project.

        Parameters:
            name: The name of the project.

        Returns:
            A project handle object to perform operations on.
        """
        _client = client._get_instance()
        if not name and not _client._project_name:
            raise ValueError(
                "No project name provided. Please provide a project name or"
                " set a project when login or creating the connection."
            )
        if not _client._project_name:
            self._provide_project(name)
        elif not name:
            name = client._get_instance()._project_name

        return self._project_api._get_project(name)

    @usage._method_logger
    @_connected
    def _get_projects(self) -> list[Project]:
        """Get all projects.

        Returns:
            List of Project objects.
        """
        return self._project_api._get_projects()

    @usage._method_logger
    @_connected
    def _project_exists(self, name: str) -> bool:
        """Check if a project exists.

        Parameters:
            name: The name of the project.

        Returns:
            Whether the project exists.
        """
        return self._project_api._exists(name)

    @_connected
    def _check_compatibility(self):
        """Check the compatibility between the client and backend.

        Assumes versioning (major.minor.patch).
        A client is considered compatible if the major and minor version matches.
        """
        versionPattern = r"\d+\.\d+"
        regexMatcher = re.compile(versionPattern)

        client_version = version.__version__
        if self._backend_version is None:
            self.backend_version = self._variable_api._get_version("hopsworks")

        major_minor_client = regexMatcher.search(client_version).group(0)
        major_minor_backend = regexMatcher.search(self._backend_version).group(0)

        if major_minor_backend != major_minor_client:
            print("\n", file=sys.stderr)
            warnings.warn(
                f"The installed hopsworks client version {client_version} may not be compatible with the connected Hopsworks backend version {self._backend_version}. \nTo ensure compatibility please install the latest bug fix release matching the minor version of your backend ({major_minor_backend}) by running 'pip install hopsworks=={major_minor_backend}.*'",
                stacklevel=1,
            )
            sys.stderr.flush()

    @_not_connected
    def _connect(self) -> None:
        """Instantiate the connection.

        Creating a `Connection` object implicitly calls this method for you to instantiate the connection.
        However, it is possible to close the connection gracefully with the `close()` method, in order to clean up materialized certificates.
        This might be desired when working on external environments such as AWS SageMaker.
        Subsequently you can call `connect()` again to reopen the connection.

        Example:
            ```python
            import hopsworks
            conn = hopsworks.connection()
            conn.close()
            conn.connect()
            ```
        """
        client._stop()
        self._connected = True
        finalizer = weakref.finalize(self, self._close)
        try:
            external = client.base.Client.REST_ENDPOINT not in os.environ
            saas = self._host == constants.HOSTS.SAAS_HOST
            # determine engine, needed to init client
            if (
                self._engine is None
                and importlib.util.find_spec("pyspark")
                and (not external or not saas)
            ):
                self._engine = "spark"
            elif self._engine is None:
                self._engine = "python"
            elif self._engine.lower() == "spark":
                self._engine = "spark"
            elif self._engine.lower() == "python":
                self._engine = "python"
            elif self._engine.lower() == "training":
                self._engine = "training"
            elif self._engine.lower() == "spark-no-metastore":
                self._engine = "spark-no-metastore"
            elif self._engine.lower() == "spark-delta":
                self._engine = "spark-delta"
            else:
                raise ConnectionError(
                    "Engine you are trying to initialize is unknown. "
                    "Supported engines are `'spark'`, `'python'`, `'training'`, `'spark-no-metastore'`, and `'spark-delta'`."
                )

            # init client
            if external:
                client._init(
                    "external",
                    self._host,
                    self._port,
                    self._project,
                    self._engine,
                    self._hostname_verification,
                    self._trust_store_path,
                    self._cert_folder,
                    self._api_key_file,
                    self._api_key_value,
                )
            else:
                client._init(
                    "hopsworks",
                    hostname_verification=self._hostname_verification,
                )

            client._set_connection(self)
            global _hsfs_engine_type
            _hsfs_engine_type = self._engine
            self._project_api = project_api.ProjectApi()
            self._hosts_api = hosts_api.HostsApi()
            self._services_api = services_api.ServicesApi()
            self._secret_api = secret_api.SecretsApi()
            self._variable_api = variable_api.VariableApi()
            self.backend_version = self._variable_api._get_version("hopsworks")
            usage._init_usage(self._host, self._backend_version)

            self._provide_project()
        except (TypeError, ConnectionError):
            self._connected = False
            finalizer.detach()
            raise

        self._check_compatibility()

    @_connected
    def _provide_project(self, name=None):
        _client = client._get_instance()

        if name:
            self._project = name
            if _client._is_external():
                _client._provide_project(name)

        if _client._project_name:
            self._project = _client._project_name

        if not self._project:
            return

        if self._engine not in ("python", "training"):
            # Spark engines must come up while the project is being set (the
            # session needs the certificates before user code runs); the Python
            # engine initialises itself on first use instead of importing hsfs
            # here.
            from hsfs import engine

            engine._get_instance()

    def _load_serving_defaults(self) -> None:
        """Load the model-serving defaults and the istio client, once.

        Called on first use of anything serving-related (see
        ``client._load_serving_defaults``) rather than during login, so a caller
        that never deploys a model pays neither the requests nor the import.
        """
        if (
            self._serving_defaults_loaded
            or self._serving_defaults_loading
            or not self._connected
            or not self._project
        ):
            return
        # Loading reads the serving settings itself (via the client getters), so
        # the in-progress flag stops that recursion; ``loaded`` is only set once
        # the load succeeded, or was deliberately skipped, so a transient error
        # leaves the next call free to retry.
        self._serving_defaults_loading = True
        try:
            serving_available = self._variable_api._get_data_science_profile_enabled()
            if serving_available:
                try:
                    # istio client, default resources,...
                    self._model_serving_api._load_default_configuration()
                except RestAPIError as e:
                    if e.response.error_code == 403 and e.error_code == 320004:
                        print(
                            'The used API key does not include "SERVING" scope, the related functionality will be disabled.'
                        )
                        _logger.debug(f"The ignored exception: {e}")
                        serving_available = False
                    else:
                        raise e
            if not serving_available:
                # Serving is off for this session: give the getters definite
                # values (no KServe, no instance limits, no Knative domain)
                # instead of None.
                client._set_kserve_installed(False)
                client._set_serving_num_instances_limits([-1, -1])
                client._set_knative_domain("")
            self._serving_defaults_loaded = True
        finally:
            self._serving_defaults_loading = False

    def _close(self) -> None:
        """Close a connection gracefully.

        This will clean up any materialized certificates on the local file system of external environments such as AWS SageMaker.

        Usage is optional.

        Example:
            ```python
            import hopsworks
            conn = hopsworks.connection()
            conn.close()
            ```
        """
        if not self._connected:
            return  # the connection is already closed

        opensearch = sys.modules.get("hopsworks_common.core.opensearch")
        if opensearch is not None and opensearch.OpenSearchClientSingleton._instance:
            opensearch.OpenSearchClientSingleton._close_all()
        client._stop()
        # Nothing to stop unless hsfs was actually used on this connection.
        if "hsfs.engine" in sys.modules:
            from hsfs import engine

            engine._stop()
        # hsfs.engine._get_type() answers from this; after close it must fail
        # again rather than report the closed connection's engine.
        global _hsfs_engine_type
        _hsfs_engine_type = None
        self._feature_store_api = None
        self._model_registry_api = None
        self._model_serving_api = None
        self._serving_defaults_loaded = False
        self._serving_defaults_loading = False
        self._connected = False
        _logger.info("Connection closed.")

    @classmethod
    def _connection(
        cls,
        host: str | None = None,
        port: int = HOPSWORKS_PORT_DEFAULT,
        project: str | None = None,
        engine: str | None = None,
        hostname_verification: bool = HOSTNAME_VERIFICATION_DEFAULT,
        trust_store_path: str | None = None,
        cert_folder: str = CERT_FOLDER_DEFAULT,
        api_key_file: str | None = None,
        api_key_value: str | None = None,
    ) -> Connection:
        """Connection factory method, accessible through `hopsworks.connection()`.

        This class provides convenience classmethods accessible from the `hopsworks`-module:

        Example: Connection factory
            For convenience, `hopsworks` provides a factory method, accessible from the top level module, so you don't have to import the `Connection` class manually:

            ```python
            import hopsworks
            conn = hopsworks.connection()
            ```

        Hint: Save API Key as File
            To get started quickly, you can simply create a file with the previously created Hopsworks API Key and place it on the environment from which you wish to connect to Hopsworks.

            You can then connect by simply passing the path to the key file when instantiating a connection:

            ```python hl_lines="6"
            import hopsworks
            conn = hopsworks.connection(
                'my_instance',                 # DNS of your Hopsworks instance
                443,                           # Port to reach your Hopsworks instance, defaults to 443
                api_key_file='hopsworks.key',  # The file containing the API key generated above
                hostname_verification=True)    # Disable for self-signed certificates
            )
            project = hopsworks.login(project="my_project")
            ```

        Clients in external clusters need to connect to the Hopsworks using an API key.
        The API key is generated inside the Hopsworks platform, and requires at least the "project" scope to be able to access a project.

        Parameters:
            host:
                The hostname of the Hopsworks instance in the form of `[UUID].cloud.hopsworks.ai`.
                Do **not** use the url including `https://` when connecting programatically.
            port: The port on which the Hopsworks instance can be reached.
            project:
                The name of the project to connect to.
                When running on Hopsworks, this defaults to the project from where the client is run from.
            engine:
                Specifies the engine to use.
                Possible options are `spark`, `python`, `training`, `spark-no-metastore`, or `spark-delta`.
                The default value, `None`, automatically selects the engine based on the environment:

                * `spark`: Used if Spark is available and the connection is not to Hopsworks SaaS, such as in Hopsworks or Databricks environments.
                * `python`: Used in local Python environments or AWS SageMaker when Spark is not available or the connection is done to Hopsworks SaaS.
                * `training`: Used when only feature store metadata is needed, such as for obtaining training dataset locations and label information during Hopsworks training experiments.
                * `spark-no-metastore`: Functions like "spark" but does not rely on the Hive metastore.
                * `spark-delta`: Minimizes dependencies further by avoiding both Hive metastore and HopsFS.

            hostname_verification: Whether or not to verify Hopsworks' certificate.
            trust_store_path: Path on the file system containing the Hopsworks certificates.
            cert_folder:
                The directory to store retrieved HopsFS certificates.
                Only required when running without a Spark environment.
            api_key_file: Path to a file containing the API Key.
            api_key_value
                API Key as string.
                If provided, `api_key_file` is ignored; however, this should be used with care, especially if the used notebook or job script is accessible by multiple parties.

        Returns:
            Connection handle to perform operations on a Hopsworks project.
        """
        return cls(
            host,
            port,
            project,
            engine,
            hostname_verification,
            trust_store_path,
            cert_folder,
            api_key_file,
            api_key_value,
        )

    @property
    def host(self) -> str | None:
        return self._host

    @host.setter
    @_not_connected
    def host(self, host: str | None) -> None:
        self._host = host

    @property
    def port(self) -> int:
        return self._port

    @port.setter
    @_not_connected
    def port(self, port: int) -> None:
        self._port = port

    @property
    def project(self) -> str | None:
        return self._project

    @project.setter
    @_not_connected
    def project(self, project: str | None) -> None:
        self._project = project

    @property
    def hostname_verification(self):
        return self._hostname_verification

    @hostname_verification.setter
    @_not_connected
    def hostname_verification(self, hostname_verification):
        self._hostname_verification = hostname_verification

    @property
    def trust_store_path(self) -> str | None:
        return self._trust_store_path

    @trust_store_path.setter
    @_not_connected
    def trust_store_path(self, trust_store_path: str | None) -> None:
        self._trust_store_path = trust_store_path

    @property
    def cert_folder(self) -> str:
        return self._cert_folder

    @cert_folder.setter
    @_not_connected
    def cert_folder(self, cert_folder: str) -> None:
        self._cert_folder = cert_folder

    @property
    def api_key_file(self) -> str | None:
        return self._api_key_file

    @property
    def api_key_value(self) -> str | None:
        return self._api_key_value

    @property
    def backend_version(self) -> str | None:
        """The version of the backend currently connected to hopsworks."""
        return self._backend_version

    @backend_version.setter
    def backend_version(self, backend_version: str) -> None:
        """The version of the backend currently connected to hopsworks.

        Parameters:
            backend_version: The version string of the backend, as obtained from the backend itself.
        """
        self._backend_version = backend_version.split("-SNAPSHOT")[
            0
        ].strip()  # Strip off the -SNAPSHOT part of the version if it is present.
        return self._backend_version

    @api_key_file.setter
    @_not_connected
    def api_key_file(self, api_key_file: str | None) -> None:
        self._api_key_file = api_key_file

    @api_key_value.setter
    @_not_connected
    def api_key_value(self, api_key_value: str | None) -> str | None:
        self._api_key_value = api_key_value

    def __enter__(self) -> Self:
        self._connect()
        return self

    def __exit__(self, type, value, traceback):
        self._close()
