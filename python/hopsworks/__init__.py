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
from __future__ import annotations

import getpass
import importlib
import logging
import os
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Literal

# Lightweight imports happen eagerly. Heavy submodules (`hsfs`, `hsml`,
# `hopsworks.connection`, …) are loaded on first attribute access via the
# PEP 562 ``__getattr__`` below. This keeps ``import hopsworks`` cheap for
# entry points (the ``hops`` CLI, dependent libraries' import-time checks)
# that don't need the full feature-store / model-registry surface area.
from hopsworks.core import project_api, secret_api
from hopsworks.decorators import NoHopsworksConnectionError
from hopsworks_apigen import public
from hopsworks_common import client, constants, project, usage, version
from hopsworks_common.client.exceptions import (
    HopsworksSSLClientError,
    ProjectException,
    RestAPIError,
)
from hopsworks_common.constants import CLIENT
from hopsworks_common.core import env_var_api
from requests.exceptions import SSLError


# Needs to run before import of hsml and hsfs (consumed transitively by some
# clients that explicitly do ``import hopsworks; hopsworks.hsfs``).
warnings.filterwarnings(action="ignore", category=UserWarning, module=r".*psycopg2")


__version__ = version.__version__


_connected_project = None
_secrets_api = None
_env_vars_api = None
_project_api = None


def hw_formatwarning(message, category, filename, lineno, line=None):
    return f"{category.__name__}: {message}\n"


warnings.formatwarning = hw_formatwarning

__all__ = ["connection", "udf"]


# ─── Lazy attribute resolution ───────────────────────────────────────────────
# Map name → factory. Factories are callables so we can side-effect
# ``sys.modules`` for ``hopsworks.hsfs`` / ``hopsworks.hsml`` aliases on first
# touch, matching the previous behaviour of the eager imports.


def _load_hsfs():  # type: ignore[no-untyped-def]
    mod = importlib.import_module("hsfs")
    sys.modules.setdefault("hopsworks.hsfs", mod)
    return mod


def _load_hsml():  # type: ignore[no-untyped-def]
    mod = importlib.import_module("hsml")
    sys.modules.setdefault("hopsworks.hsml", mod)
    return mod


def _load_connection_class():  # type: ignore[no-untyped-def]
    from hopsworks.connection import Connection

    return Connection


def _load_build_spark():  # type: ignore[no-untyped-def]
    from hopsworks.spark import build_spark  # noqa: F401

    return build_spark


_LAZY = {
    "hsfs": _load_hsfs,
    "hsml": _load_hsml,
    "Connection": _load_connection_class,
    "build_spark": _load_build_spark,
    "connection": lambda: _load_connection_class().connection,
    # ``udf`` is the public entry point for hopsworks UDFs; pulling it through
    # ``hsfs`` keeps the lazy-load path consistent.
    "udf": lambda: _load_hsfs().hopsworks_udf.udf,
}


def __getattr__(name):  # type: ignore[no-untyped-def]
    """PEP 562 lazy attribute access.

    ``import hopsworks`` no longer triggers ``hsfs`` / ``hsml`` /
    ``great_expectations``; the first time anyone reads
    ``hopsworks.connection`` / ``hopsworks.udf`` / ``hopsworks.hsfs`` /
    ``hopsworks.hsml``, the relevant module is imported and cached on the
    package object so subsequent accesses are free.
    """
    factory = _LAZY.get(name)
    if factory is None:
        raise AttributeError(f"module 'hopsworks' has no attribute {name!r}")
    value = factory()
    # Cache so subsequent attribute lookups skip ``__getattr__`` entirely.
    globals()[name] = value
    return value


# PEP 562 ``__getattr__`` only fires on attribute access, not when the import
# machinery resolves a dotted module path. Without this finder,
# ``from hopsworks.hsfs.builtin_transformations import X`` raises
# ``ModuleNotFoundError: No module named 'hopsworks.hsfs'`` — the public
# ``hopsworks.hsfs[.*]`` / ``hopsworks.hsml[.*]`` aliases have been supported
# since #292 (Aug 2024) and downstream code (e.g. loadtest, customer notebooks)
# depends on them. A meta-path finder restores the alias contract while keeping
# the lazy goal: ``hsfs`` / ``hsml`` are only imported when something actually
# references them, not on ``import hopsworks``.
import importlib.util  # noqa: E402


_ALIAS_TO_REAL = {"hopsworks.hsfs": "hsfs", "hopsworks.hsml": "hsml"}


class _AliasLoader:
    """No-op loader that hands back an already-executed module."""

    def __init__(self, real_module):  # type: ignore[no-untyped-def]
        self._real = real_module

    def create_module(self, spec):  # type: ignore[no-untyped-def]
        return self._real

    def exec_module(self, module):  # type: ignore[no-untyped-def]
        pass  # real module was already executed by importlib.import_module


class _HsfsHsmlAliasFinder:
    """Route ``hopsworks.hsfs[.*]`` and ``hopsworks.hsml[.*]`` to the real packages."""

    def find_spec(self, fullname, path=None, target=None):  # type: ignore[no-untyped-def]
        for alias, real in _ALIAS_TO_REAL.items():
            if fullname == alias or fullname.startswith(alias + "."):
                real_name = real + fullname[len(alias) :]
                real_mod = importlib.import_module(real_name)
                return importlib.util.spec_from_loader(fullname, _AliasLoader(real_mod))
        return None


# Append (not prepend) so the standard ``PathFinder`` and pytest's assertion
# rewriter still run first for ordinary modules; our finder only fires for the
# two alias namespaces, which no real on-disk subpackage shadows.
if not any(isinstance(f, _HsfsHsmlAliasFinder) for f in sys.meta_path):
    sys.meta_path.append(_HsfsHsmlAliasFinder())


def _make_connection(*args, **kwargs):  # type: ignore[no-untyped-def]
    """Factory: create a new Connection via the lazy-loaded Connection class."""
    return _load_connection_class().connection(*args, **kwargs)


# Holds the active Connection instance after login(); points to _make_connection
# when logged out so the login() flow can always call _hw_connection(...) to
# create a fresh connection regardless of prior auth state.
_hw_connection = _make_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    stream=sys.stdout,
)


@public(order=1)
def login(
    host: str | None = None,
    port: int = 443,
    project: str | None = None,
    api_key_value: str | None = None,
    api_key_file: str | None = None,
    hostname_verification: bool = False,
    trust_store_path: str | None = None,
    cert_folder: str | None = None,
    engine: Literal["spark", "python", "training", "spark-no-metastore", "spark-delta"]
    | None = None,
) -> project.Project:
    """Connect to [Hopsworks SaaS](https://run.hopsworks.ai/) by calling the `hopsworks.login()` function with no arguments.

    Example: Connect to Hopsworks SaaS
        ```python
        import hopsworks

        project = hopsworks.login()
        ```

    Alternatively, connect to your own Hopsworks installation by specifying the host, port and API key.

    Example: Connect to your Hopsworks cluster
        ```python
        import hopsworks

        project = hopsworks.login(
            host="my.hopsworks.server",
            port=8181,
            api_key_value="DKN8DndwaAjdf98FFNSxwdVKx",
        )
        ```

    In addition to setting function arguments directly, `hopsworks.login()` also reads the environment variables:
    `HOPSWORKS_HOST`, `HOPSWORKS_PORT`, `HOPSWORKS_PROJECT`, `HOPSWORKS_API_KEY`, `HOPSWORKS_HOSTNAME_VERIFICATION`, `HOPSWORKS_TRUST_STORE_PATH`, `HOPSWORKS_CERT_FOLDER` and `HOPSWORKS_ENGINE`.

    The function arguments do however take precedence over the environment variables in case both are set.

    Parameters:
        host: The hostname of the Hopsworks instance.
        port: The port on which the Hopsworks instance can be reached.
        project:
            Name of the project to access.
            If used inside a Hopsworks environment it always gets the current project.
            If not provided you will be prompted to enter it.
        api_key_value: Value of the API Key.
        api_key_file: Path to file wih API Key.
        hostname_verification: Whether to verify Hopsworks' certificate.
        trust_store_path: Path on the file system containing the Hopsworks certificates.
        cert_folder:
            The directory to store downloaded certificates.
            Defaults to the system temp directory.
        engine:
            Specifies the engine to use.
            The default value is `None`, which automatically selects the engine based on the environment:

            * `spark`: Used if Spark is available, such as in Hopsworks or Databricks environments.
            * `python`: Used in local Python environments or AWS SageMaker when Spark is not available.
            * `training`: Used when only feature store metadata is needed, such as for obtaining training dataset locations and label information during Hopsworks training experiments.
            * `spark-no-metastore`: Functions like `spark` but does not rely on the Hive metastore.
            * `spark-delta`: Minimizes dependencies further by avoiding both Hive metastore and HopsFS.

    Returns:
        The Project object to perform operations on.

    Raises:
        hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request.
        hopsworks.client.exceptions.HopsworksSSLClientError: If SSLError is raised from underlying requests library.
    """
    global _connected_project

    # If already logged in, should reset connection and follow login procedure as Connection may no longer be valid
    logout()

    global _hw_connection

    # If project argument not defined, get HOPSWORKS_ENGINE environment variable
    if engine is None and "HOPSWORKS_ENGINE" in os.environ:
        engine = os.environ["HOPSWORKS_ENGINE"]

    # If inside hopsworks, just return the current project for now
    if "REST_ENDPOINT" in os.environ:
        _hw_connection = _hw_connection(
            hostname_verification=hostname_verification, engine=engine
        )
        _connected_project = _hw_connection.get_project()
        _initialize_module_apis()
        print("\nLogged in to project, explore it here " + _connected_project.get_url())
        return _connected_project

    # This is run for an external client

    # Function arguments takes precedence over environment variable.
    # Here we check if environment variable exists and function argument is not set, we use the environment variable.

    # If api_key_value/api_key_file not defined, get HOPSWORKS_API_KEY environment variable
    api_key = None
    if (
        api_key_value is None
        and api_key_file is None
        and "HOPSWORKS_API_KEY" in os.environ
    ):
        api_key = os.environ["HOPSWORKS_API_KEY"]

    # If project argument not defined, get HOPSWORKS_PROJECT environment variable
    if project is None and "HOPSWORKS_PROJECT" in os.environ:
        project = os.environ["HOPSWORKS_PROJECT"]

    # If host argument not defined, get HOPSWORKS_HOST environment variable
    if host is None and "HOPSWORKS_HOST" in os.environ:
        host = os.environ["HOPSWORKS_HOST"]
    elif host is None:  # Always do a fallback to Hopsworks SaaS if not defined
        host = constants.HOSTS.SAAS_HOST

    is_saas = host == constants.HOSTS.SAAS_HOST

    # If port same as default, get HOPSWORKS_HOST environment variable
    if port == 443 and "HOPSWORKS_PORT" in os.environ:
        port = os.environ["HOPSWORKS_PORT"]

    hostname_verification = os.getenv(
        "HOPSWORKS_HOSTNAME_VERIFICATION", f"{hostname_verification}"
    ).lower() in ("true", "1", "y", "yes")

    trust_store_path = os.getenv("HOPSWORKS_TRUST_STORE_PATH", trust_store_path)

    # If cert_folder not provided, check environment variable, then fall back to default
    if cert_folder is None:
        cert_folder = os.getenv("HOPSWORKS_CERT_FOLDER", CLIENT.CERT_FOLDER_DEFAULT)

    # This .hw_api_key is created when a user logs into Hopsworks SaaS the first time.
    # It is then used only for future login calls to SaaS. For other Hopsworks installations it's ignored.
    api_key_path = _get_cached_api_key_path()

    # Conditions for getting the api_key
    # If user supplied the api key directly
    if api_key_value is not None:
        api_key = api_key_value
    # If user supplied the api key in a file
    elif api_key_file is not None:
        if os.path.exists(api_key_file):
            api_key = Path(api_key_file).read_text()
        else:
            raise OSError(f"Could not find api key file on path: {api_key_file}")
    # If user connected to Hopsworks SaaS, and the cached .hw_api_key exists, then use it.
    elif os.path.exists(api_key_path) and is_saas:
        try:
            _hw_connection = _hw_connection(
                host=host,
                port=port,
                engine=engine,
                api_key_file=api_key_path,
                hostname_verification=hostname_verification,
                trust_store_path=trust_store_path,
                cert_folder=cert_folder,
            )
            _connected_project = _prompt_project(_hw_connection, project, is_saas)
            if _connected_project:
                _set_active_project(_connected_project)
            print(
                "\nLogged in to project, explore it here "
                + _connected_project.get_url()
            )
            _initialize_module_apis()
            return _connected_project
        except RestAPIError:
            logout()
            # API Key may be invalid, have the user supply it again
            os.remove(api_key_path)
        except SSLError as ssl_e:
            logout()
            _handle_ssl_errors(ssl_e)

    if api_key is None and is_saas:
        print("Copy your API Key (first register/login at https://run.hopsworks.ai)")
        api_key = getpass.getpass(prompt="\nPaste it here: ")

        # If api key was provided as input, save the API key locally on disk to avoid users having to enter it again in the same environment
        Path(os.path.dirname(api_key_path)).mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            path=api_key_path, flags=(os.O_WRONLY | os.O_CREAT | os.O_TRUNC), mode=0o600
        )
        with open(descriptor, "w") as fh:
            fh.write(api_key.strip())

    try:
        _hw_connection = _hw_connection(
            host=host,
            port=port,
            engine=engine,
            api_key_value=api_key,
            hostname_verification=hostname_verification,
            trust_store_path=trust_store_path,
            cert_folder=cert_folder,
        )
        _connected_project = _prompt_project(_hw_connection, project, is_saas)
        if _connected_project:
            _set_active_project(_connected_project)
    except RestAPIError as hw_e:
        logout()
        raise hw_e
    except SSLError as ssl_e:
        logout()
        _handle_ssl_errors(ssl_e)

    if _connected_project is None:
        print(
            "Could not find any project, use hopsworks.create_project('my_project') to create one"
        )
    else:
        print("\nLogged in to project, explore it here " + _connected_project.get_url())

    _initialize_module_apis()
    return _connected_project


def _handle_ssl_errors(ssl_e):
    raise HopsworksSSLClientError(
        "Hopsworks certificate verification can be turned off by specifying hopsworks.login(hostname_verification=False) "
        "or setting the environment variable HOPSWORKS_HOSTNAME_VERIFICATION='False'"
    ) from ssl_e


def _get_cached_api_key_path():
    """This function is used to get an appropriate path to store the user supplied API Key for Hopsworks SaaS.

    First it will search for .hw_api_key in the current working directory, if it exists it will use it (this is default in 3.0 client)
    Otherwise, falls back to storing the API key in HOME
    If not sufficient permissions are set in HOME to create the API key (writable and executable), it uses the temp directory to store it.
    """
    api_key_name = ".hw_api_key"
    api_key_folder = f".{getpass.getuser()}_hopsworks_app"

    # Path for current working directory api key
    cwd_api_key_path = os.path.join(os.getcwd(), api_key_name)

    # Path for home api key
    home_dir_path = os.path.expanduser("~")
    home_api_key_path = os.path.join(home_dir_path, api_key_folder, api_key_name)

    # Path for tmp api key
    temp_dir_path = tempfile.gettempdir()
    temp_api_key_path = os.path.join(temp_dir_path, api_key_folder, api_key_name)

    if os.path.exists(
        cwd_api_key_path
    ):  # For backward compatibility, if .hw_api_key exists in current working directory get it from there
        api_key_path = cwd_api_key_path
    elif os.access(home_dir_path, os.W_OK) and os.access(
        home_dir_path, os.X_OK
    ):  # Otherwise use the home directory of the user
        api_key_path = home_api_key_path
    else:  # Finally try to store it in temp
        api_key_path = temp_api_key_path

    return api_key_path


def _prompt_project(valid_connection, project, is_saas):
    if project is None:
        saas_projects = valid_connection._project_api._get_projects()
        if len(saas_projects) == 0:
            if is_saas:
                raise ProjectException("Could not find any project")
            return None
        if len(saas_projects) == 1:
            return saas_projects[0]
        while True:
            print("\nMultiple projects found. \n")
            for index in range(len(saas_projects)):
                print("\t (" + str(index + 1) + ") " + saas_projects[index].name)
            while True:
                project_index = input(
                    "\nEnter number corresponding to the project to use: "
                )
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
        try:
            return valid_connection._project_api._get_project(project)
        except RestAPIError as x:
            raise ProjectException(f"Could not find project {project}") from x


def logout():
    """Cleans up and closes the connection for hopsworks."""
    global _hw_connection
    global _project_api
    global _secrets_api
    global _env_vars_api

    if _is_connection_active():
        _hw_connection.close()

    client.stop()
    _project_api = None
    _secrets_api = None
    _env_vars_api = None
    _hw_connection = _make_connection


def _is_connection_active():
    global _hw_connection
    return isinstance(_hw_connection, _load_connection_class())


@public
def get_current_project() -> project.Project:
    """Get a reference to the current logged in project.

    Example: Example for getting the project reference
        ```python
        import hopsworks

        hopsworks.login()

        project = hopsworks.get_current_project()
        ```

    Returns:
        The Project object to perform operations on.
    """
    global _connected_project
    if _connected_project is None:
        raise ProjectException("No project is set for this session")
    return _connected_project


def _initialize_module_apis():
    global _project_api
    global _secrets_api
    global _env_vars_api
    _project_api = project_api.ProjectApi()
    _secrets_api = secret_api.SecretsApi()
    _env_vars_api = env_var_api.EnvVarsApi()


@public
def create_project(
    name: str,
    description: str | None = None,
    feature_store_topic: str | None = None,
    namespace: str | None = None,
) -> project.Project | None:
    """Create a new project.

    Example: Example for creating a new project
        ```python
        import hopsworks

        hopsworks.login(...)

        hopsworks.create_project("my_project", description="An example Hopsworks project")
        ```

    Parameters:
        name: The name of the project.
        description: Description of the project.
        feature_store_topic: Feature store topic name.
        namespace: Kubernetes namespace to use for the project. If ``None`` the
            backend derives one from the project name.

    Returns:
        The Project object to perform operations on.
    """
    global _hw_connection
    global _connected_project

    if not _is_connection_active():
        raise NoHopsworksConnectionError

    new_project = _hw_connection._project_api._create_project(
        name, description, feature_store_topic, namespace
    )
    if _connected_project is None:
        _connected_project = new_project
        if _connected_project:
            _set_active_project(_connected_project)
        print(
            f"Setting {_connected_project.name} as the current project, a reference can be retrieved by calling hopsworks.get_current_project()"
        )
        return _connected_project
    print(
        f"You are already using the project {_connected_project.name}, to access the new project use hopsworks.login(..., project='{new_project.name}')"
    )
    return None


@public
def get_secrets_api() -> secret_api.SecretsApi:
    """Get the secrets api.

    Returns:
        The Secrets API handle.
    """
    global _secrets_api
    if not _is_connection_active():
        raise NoHopsworksConnectionError
    return _secrets_api


@public
def get_env_vars_api() -> env_var_api.EnvVarsApi:
    """Get the environment variables api.

    Returns:
        The environment variables API handle.
    """
    global _env_vars_api
    if not _is_connection_active():
        raise NoHopsworksConnectionError
    return _env_vars_api


def _set_active_project(project):
    _client = client.get_instance()
    if _client._is_external():
        _client.provide_project(project.name)


def disable_usage_logging():
    usage.disable()


def get_sdk_info():
    return usage.get_env()
