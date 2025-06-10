#
#   Copyright 2024 Hopsworks AB
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

from hopsworks_common.client import (
    _is_external,
    auth,
    base,
    exceptions,
    external,
    get_instance,
    get_knative_domain,
    get_serving_num_instances_limits,
    hopsworks,
    init,
    is_kserve_installed,
    is_saas_connection,
    is_scale_to_zero_required,
    istio,
    online_store_rest_client,
    set_knative_domain,
    set_kserve_installed,
    set_serving_num_instances_limits,
    stop,
)


__all__ = [
    "auth",
    "base",
    "exceptions",
    "external",
    "get_instance",
    "get_knative_domain",
    "get_serving_num_instances_limits",
    "hopsworks",
    "init",
    "is_kserve_installed",
    "is_saas_connection",
    "is_scale_to_zero_required",
    "istio",
    "online_store_rest_client",
    "set_knative_domain",
    "set_kserve_installed",
    "set_serving_num_instances_limits",
    "stop",
    "_is_external",
]
