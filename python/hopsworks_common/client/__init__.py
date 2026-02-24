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

from typing import Literal

from hopsworks_apigen import also_available_as
from hopsworks_common.client import external, hopsworks, istio
from hopsworks_common.constants import HOSTS


_client: hopsworks.Client | external.Client | None = None


@also_available_as("hopsworks.client.init")
def init(
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
        _client.provide_project(project)


@also_available_as("hopsworks.client.get_instance")
def get_instance() -> hopsworks.Client | external.Client:
    global _client
    if not _client:
        raise Exception("Couldn't find client. Try reconnecting to Hopsworks.")
    return _client


@also_available_as("hopsworks.client.stop")
def stop() -> None:
    global _client
    if _client:
        _client._close()
    _client = None
    if istio._client:
        istio._client._close()
    istio._client = None


@also_available_as("hopsworks.client.is_saas_connection")
def is_saas_connection() -> bool:
    return get_instance()._host == HOSTS.APP_HOST


_kserve_installed = None


@also_available_as("hopsworks.client.set_kserve_installed")
def set_kserve_installed(kserve_installed):
    global _kserve_installed
    _kserve_installed = kserve_installed


@also_available_as("hopsworks.client.is_kserve_installed")
def is_kserve_installed() -> bool:
    global _kserve_installed
    return _kserve_installed


_serving_num_instances_limits = None


@also_available_as("hopsworks.client.set_serving_num_instances_limits")
def set_serving_num_instances_limits(num_instances_range):
    global _serving_num_instances_limits
    _serving_num_instances_limits = num_instances_range


@also_available_as("hopsworks.client.get_serving_num_instances_limits")
def get_serving_num_instances_limits():
    global _serving_num_instances_limits
    return _serving_num_instances_limits


@also_available_as("hopsworks.client.is_scale_to_zero_required")
def is_scale_to_zero_required():
    # scale-to-zero is required for KServe deployments if the Hopsworks variable `kube_serving_min_num_instances`
    # is set to 0. Other possible values are -1 (unlimited num instances) or >1 num instances.
    return get_serving_num_instances_limits()[0] == 0


_knative_domain = None


@also_available_as("hopsworks.client.get_knative_domain")
def get_knative_domain():
    global _knative_domain
    return _knative_domain


@also_available_as("hopsworks.client.set_knative_domain")
def set_knative_domain(knative_domain):
    global _knative_domain
    _knative_domain = knative_domain


_connection = None


@also_available_as("hopsworks.client.get_connection")
def get_connection():
    return _connection


@also_available_as("hopsworks.client.set_connection")
def set_connection(connection):
    global _connection
    _connection = connection


@also_available_as("hopsworks.client._is_external")
def _is_external():
    global _client
    if _client is None:
        raise ConnectionError("Hopsworks Client not initialized.")
    return _client._is_external()
