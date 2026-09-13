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
"""Model deployments: the deployment handle, its predictor and transformer, and their configuration objects.

`hsml.deployment.Deployment` is the deployment handle returned by `Model.deploy()`, `FeatureView.deploy()` and `ModelServing.get_deployment()`.
The submodules hold the components and value objects a deployment is built from; the names below are re-exported for convenience.
"""

from hsml.deployment.deployable_component import DeployableComponent
from hsml.deployment.deployable_component_logs import DeployableComponentLogs
from hsml.deployment.deployment import Deployment
from hsml.deployment.inference_batcher import InferenceBatcher
from hsml.deployment.inference_endpoint import InferenceEndpoint, InferenceEndpointPort
from hsml.deployment.inference_logger import InferenceLogger
from hsml.deployment.logging_config import DeploymentLoggingConfig
from hsml.deployment.predictor import Predictor
from hsml.deployment.predictor_state import PredictorState
from hsml.deployment.predictor_state_condition import PredictorStateCondition
from hsml.deployment.resources import (
    PredictorResources,
    Resources,
    TransformerResources,
)
from hsml.deployment.scaling_config import (
    PredictorScalingConfig,
    TransformerScalingConfig,
)
from hsml.deployment.schema import DeploymentSchema
from hsml.deployment.tracing_config import DeploymentTracingConfig
from hsml.deployment.transformer import Transformer


__all__ = [
    "DeployableComponent",
    "DeployableComponentLogs",
    "Deployment",
    "DeploymentLoggingConfig",
    "DeploymentSchema",
    "DeploymentTracingConfig",
    "InferenceBatcher",
    "InferenceEndpoint",
    "InferenceEndpointPort",
    "InferenceLogger",
    "Predictor",
    "PredictorResources",
    "PredictorScalingConfig",
    "PredictorState",
    "PredictorStateCondition",
    "Resources",
    "Transformer",
    "TransformerResources",
    "TransformerScalingConfig",
]
