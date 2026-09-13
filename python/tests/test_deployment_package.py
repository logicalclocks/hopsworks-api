#
#   Copyright 2026 Hopsworks AB
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

import importlib
import sys

import pytest
from hsml import deployment as deployment_pkg


# Released module paths that moved into hsml.deployment and keep a deprecated shim.
MOVED_MODULES = {
    "hsml.deployable_component": "hsml.deployment.deployable_component",
    "hsml.deployable_component_logs": "hsml.deployment.deployable_component_logs",
    "hsml.deployment_tracing_config": "hsml.deployment.tracing_config",
    "hsml.inference_batcher": "hsml.deployment.inference_batcher",
    "hsml.inference_endpoint": "hsml.deployment.inference_endpoint",
    "hsml.inference_logger": "hsml.deployment.inference_logger",
    "hsml.model_serving": "hsml.deployment.model_serving",
    "hsml.predictor": "hsml.deployment.predictor",
    "hsml.predictor_state": "hsml.deployment.predictor_state",
    "hsml.predictor_state_condition": "hsml.deployment.predictor_state_condition",
    "hsml.resources": "hsml.deployment.resources",
    "hsml.scaling_config": "hsml.deployment.scaling_config",
    "hsml.transformer": "hsml.deployment.transformer",
}


class TestDeploymentPackage:
    def test_package_exports_every_deployment_entity(self):
        for name in deployment_pkg.__all__:
            entity = getattr(deployment_pkg, name)
            assert entity.__module__.startswith("hsml.deployment."), name

    def test_deployment_handle_is_the_package_level_name(self):
        from hsml.deployment.deployment import Deployment

        assert deployment_pkg.Deployment is Deployment

    @pytest.mark.parametrize("old_path", sorted(MOVED_MODULES))
    def test_old_import_path_warns_and_resolves_to_the_moved_module(self, old_path):
        new_path = MOVED_MODULES[old_path]
        sys.modules.pop(old_path, None)
        with pytest.warns(DeprecationWarning, match=new_path):
            shim = importlib.import_module(old_path)
        target = importlib.import_module(new_path)
        exported = [n for n in dir(target) if not n.startswith("_")]
        assert exported, new_path
        for name in exported:
            assert getattr(shim, name) is getattr(target, name), f"{old_path}.{name}"
