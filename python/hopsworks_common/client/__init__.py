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
import sys
from typing import Literal

from hopsworks_apigen import also_available_as
from hopsworks_common.client import external, hopsworks
from hopsworks_common.constants import HOSTS


_client: hopsworks.Client | external.Client | None = None


@also_available_as("hopsworks.client._init")
def _init(
    client_type: Literal["hopsworks", "external"],
    host: str | None = None,
    port: int | None = None,
    project: str | None = None,
    engine: str | None = None,
    hostname_verification: bool | None = None,
    trust_store_path: str | None = None,
    cert_folder: str | None = None,
    api_key_file: str | None = None,
    api_key_value: str | None = None,
) -> None:
    global _client
    if not _client:
        if client_type == "hopsworks":
            _client = hopsworks.Client(hostname_verification)
        elif client_type == "external":
            _client = external.Client(
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
    elif _client._is_external() and not _client._project_name:
        _client._provide_project(project)


@also_available_as("hopsworks.client._get_instance")
def _get_instance() -> hopsworks.Client | external.Client:
    global _client
    if not _client:
        raise Exception("Couldn't find client. Try reconnecting to Hopsworks.")
    return _client


@also_available_as("hopsworks.client._stop")
def _stop() -> None:
    global _client
    if _client:
        _client._close()
    _client = None
    # Only touch the istio client if that subpackage was ever imported; importing
    # it here just to close nothing would pull in grpc and pandas.
    istio = sys.modules.get("hopsworks_common.client.istio")
    if istio is not None and istio._client:
        istio._client._close()
    if istio is not None:
        istio._client = None
    # The serving defaults belong to the connection that loaded them; the next
    # connection reloads them on first use instead of reading the old cluster's.
    global _kserve_installed, _serving_num_instances_limits, _knative_domain
    _kserve_installed = None
    _serving_num_instances_limits = None
    _knative_domain = None


@also_available_as("hopsworks.client._is_saas_connection")
def _is_saas_connection() -> bool:
    return _get_instance()._host == HOSTS.SAAS_HOST


_kserve_installed = None


@also_available_as("hopsworks.client._set_kserve_installed")
def _set_kserve_installed(kserve_installed):
    global _kserve_installed
    _kserve_installed = kserve_installed


@also_available_as("hopsworks.client._is_kserve_installed")
def _is_kserve_installed() -> bool:
    global _kserve_installed
    if _kserve_installed is None:
        _load_serving_defaults()
    return _kserve_installed


_serving_num_instances_limits = None


@also_available_as("hopsworks.client._set_serving_num_instances_limits")
def _set_serving_num_instances_limits(num_instances_range):
    global _serving_num_instances_limits
    _serving_num_instances_limits = num_instances_range


@also_available_as("hopsworks.client._get_serving_num_instances_limits")
def _get_serving_num_instances_limits():
    global _serving_num_instances_limits
    if _serving_num_instances_limits is None:
        _load_serving_defaults()
    return _serving_num_instances_limits


@also_available_as("hopsworks.client._is_scale_to_zero_required")
def _is_scale_to_zero_required():
    # scale-to-zero is required for KServe deployments if the Hopsworks variable `kube_serving_min_num_instances`
    # is set to 0. Other possible values are -1 (unlimited num instances) or >1 num instances.
    return _get_serving_num_instances_limits()[0] == 0


_knative_domain = None


@also_available_as("hopsworks.client._get_knative_domain")
def _get_knative_domain():
    global _knative_domain
    if _knative_domain is None:
        _load_serving_defaults()
    return _knative_domain


def _load_serving_defaults() -> None:
    """Fetch the serving defaults from the connected cluster, once, on first use.

    The defaults are the KServe flag, the instance limits, the Knative domain and the istio client.
    They used to be loaded during login.
    That cost six requests and the istio import for every caller, including ones that never touch model serving.
    """
    if _connection is not None:
        _connection._load_serving_defaults()


def __getattr__(name: str):
    # ``client.istio`` stays reachable as before, but the subpackage (grpc,
    # pandas, numpy: ~0.4 s) is imported the first time it is actually used.
    if name == "istio":
        return importlib.import_module("hopsworks_common.client.istio")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


@also_available_as("hopsworks.client._set_knative_domain")
def _set_knative_domain(knative_domain):
    global _knative_domain
    _knative_domain = knative_domain


_connection = None


@also_available_as("hopsworks.client._get_connection")
def _get_connection():
    return _connection


@also_available_as("hopsworks.client._set_connection")
def _set_connection(connection):
    global _connection
    _connection = connection


@also_available_as("hopsworks.client._is_external")
def _is_external():
    global _client
    if _client is None:
        raise ConnectionError("Hopsworks Client not initialized.")
    return _client._is_external()
