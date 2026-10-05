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

from __future__ import annotations

import json
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import util
from hsml.deployment_logging_config import DeploymentLoggingConfig
from hsml.deployment_tracing_config import DeploymentTracingConfig
from hsml.resources import PredictorResources, TransformerResources
from hsml.scaling_config import PredictorScalingConfig, TransformerScalingConfig


def _environment_name(environment: dict | str | None) -> str | None:
    if isinstance(environment, dict):
        return environment.get("name")
    return environment


def _environment_dict(name: str | None) -> dict | None:
    return {"name": name} if name is not None else None


def _inner(component: Any, key: str) -> dict | None:
    return component.to_dict()[humps.camelize(key)] if component else None


@public
class DeploymentVersion:
    """One retained configuration version of a deployment.

    A deployment keeps every version it ever had.
    Exactly one of them is active, and rolling back reactivates an earlier one without copying it.
    Only the predictor, transformer and model artifact settings belong to a version.
    The API protocol, request batching, inference logging, scheduling configuration and Knative mode live on the deployment itself and are the same for every version.
    """

    def __init__(
        self,
        version: int,
        active: bool = False,
        created: str | None = None,
        created_by: str | None = None,
        updated: str | None = None,
        updated_by: str | None = None,
        model_name: str | None = None,
        model_version: int | None = None,
        model_path: str | None = None,
        model_framework: str | None = None,
        model_server: str | None = None,
        predictor: str | None = None,
        config_file: str | None = None,
        predictor_resources: PredictorResources | dict | None = None,
        predictor_scaling_config: PredictorScalingConfig | dict | None = None,
        predictor_env_vars: list[str] | None = None,
        predictor_environment_id: int | None = None,
        predictor_environment: dict | str | None = None,
        transformer: str | None = None,
        transformer_resources: TransformerResources | dict | None = None,
        transformer_scaling_config: TransformerScalingConfig | dict | None = None,
        transformer_env_vars: list[str] | None = None,
        transformer_environment_id: int | None = None,
        transformer_environment: dict | str | None = None,
        git_url: str | None = None,
        git_branch: str | None = None,
        git_provider: str | None = None,
        git_auto_redeploy: bool | None = None,
        git_current_commit: str | None = None,
        vllm_variant: str | None = None,
        vllm_image_tag: str | None = None,
        tracing: DeploymentTracingConfig | dict | None = None,
        feature_logging: DeploymentLoggingConfig | dict | None = None,
        **kwargs: Any,
    ) -> None:
        self._version = version
        self._active = active
        self._created = created
        self._created_by = created_by
        self._updated = updated
        self._updated_by = updated_by
        self._model_name = model_name
        self._model_version = model_version
        self._model_path = model_path
        self._model_framework = model_framework
        self._model_server = model_server
        self._predictor = predictor
        self._config_file = config_file
        self._predictor_resources = util._get_obj_from_json(
            predictor_resources, PredictorResources
        )
        self._predictor_scaling_config = util._get_obj_from_json(
            predictor_scaling_config, PredictorScalingConfig
        )
        self._predictor_env_vars = predictor_env_vars
        self._predictor_environment_id = predictor_environment_id
        self._predictor_environment = _environment_name(predictor_environment)
        self._transformer = transformer
        self._transformer_resources = util._get_obj_from_json(
            transformer_resources, TransformerResources
        )
        self._transformer_scaling_config = util._get_obj_from_json(
            transformer_scaling_config, TransformerScalingConfig
        )
        self._transformer_env_vars = transformer_env_vars
        self._transformer_environment_id = transformer_environment_id
        self._transformer_environment = _environment_name(transformer_environment)
        self._git_url = git_url
        self._git_branch = git_branch
        self._git_provider = git_provider
        self._git_auto_redeploy = git_auto_redeploy
        self._git_current_commit = git_current_commit
        self._vllm_variant = vllm_variant
        self._vllm_image_tag = vllm_image_tag
        self._tracing = util._get_obj_from_json(tracing, DeploymentTracingConfig)
        self._feature_logging = util._get_obj_from_json(
            feature_logging, DeploymentLoggingConfig
        )

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> DeploymentVersion | list[DeploymentVersion]:
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            return [cls(**item) for item in json_decamelized.get("items", [])]
        return cls(**json_decamelized)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self._version,
            "active": self._active,
            "created": self._created,
            "createdBy": self._created_by,
            "updated": self._updated,
            "updatedBy": self._updated_by,
            "modelName": self._model_name,
            "modelVersion": self._model_version,
            "modelPath": self._model_path,
            "modelFramework": self._model_framework,
            "modelServer": self._model_server,
            "predictor": self._predictor,
            "configFile": self._config_file,
            "predictorResources": _inner(
                self._predictor_resources, PredictorResources.RESOURCES_CONFIG_KEY
            ),
            "predictorScalingConfig": _inner(
                self._predictor_scaling_config,
                PredictorScalingConfig.SCALING_CONFIG_KEY,
            ),
            "predictorEnvVars": self._predictor_env_vars,
            "predictorEnvironmentId": self._predictor_environment_id,
            "predictorEnvironment": _environment_dict(self._predictor_environment),
            "transformer": self._transformer,
            "transformerResources": _inner(
                self._transformer_resources, TransformerResources.RESOURCES_CONFIG_KEY
            ),
            "transformerScalingConfig": _inner(
                self._transformer_scaling_config,
                TransformerScalingConfig.SCALING_CONFIG_KEY,
            ),
            "transformerEnvVars": self._transformer_env_vars,
            "transformerEnvironmentId": self._transformer_environment_id,
            "transformerEnvironment": _environment_dict(self._transformer_environment),
            "gitUrl": self._git_url,
            "gitBranch": self._git_branch,
            "gitProvider": self._git_provider,
            "gitAutoRedeploy": self._git_auto_redeploy,
            "gitCurrentCommit": self._git_current_commit,
            "vllmVariant": self._vllm_variant,
            "vllmImageTag": self._vllm_image_tag,
            "tracing": self._tracing.to_dict() if self._tracing else None,
            "featureLogging": (
                self._feature_logging.to_dict() if self._feature_logging else None
            ),
        }

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    @public
    @property
    def version(self) -> int:
        """Number of this version, unique within the deployment and never reused."""
        return self._version

    @public
    @property
    def active(self) -> bool:
        """Whether this is the version the deployment currently runs."""
        return self._active

    @public
    @property
    def created(self) -> str | None:
        """When this version was created."""
        return self._created

    @public
    @property
    def created_by(self) -> str | None:
        """Who created this version."""
        return self._created_by

    @public
    @property
    def updated(self) -> str | None:
        """When this version was last edited in place, or None if it never was."""
        return self._updated

    @public
    @property
    def updated_by(self) -> str | None:
        """Who last edited this version in place."""
        return self._updated_by

    @public
    @property
    def model_name(self) -> str | None:
        """Name of the model this version serves, or None for a model-less deployment."""
        return self._model_name

    @public
    @property
    def model_version(self) -> int | None:
        """Version of the model this version serves."""
        return self._model_version

    @public
    @property
    def model_path(self) -> str | None:
        """Path of the model in the model registry."""
        return self._model_path

    @public
    @property
    def model_framework(self) -> str | None:
        """Framework of the model this version serves."""
        return self._model_framework

    @public
    @property
    def model_server(self) -> str | None:
        """Model server the predictor runs on."""
        return self._model_server

    @public
    @property
    def predictor(self) -> str | None:
        """Predictor script of this version.

        This is a base name inside the version directory, which stores it with a `predictor-` prefix, or an absolute HopsFS path for a script read from outside the version.
        """
        return self._predictor

    @public
    @property
    def config_file(self) -> str | None:
        """Model server configuration file of this version.

        This is a base name inside the version directory, which stores it with a `configfile-` prefix, or an absolute HopsFS path for a file read from outside the version.
        """
        return self._config_file

    @public
    @property
    def predictor_resources(self) -> PredictorResources | None:
        """Resources allocated to the predictor."""
        return self._predictor_resources

    @public
    @property
    def predictor_scaling_config(self) -> PredictorScalingConfig | None:
        """Scaling configuration of the predictor."""
        return self._predictor_scaling_config

    @public
    @property
    def predictor_env_vars(self) -> list[str] | None:
        """User environment variables of the predictor, as `KEY=VALUE` strings."""
        return self._predictor_env_vars

    @public
    @property
    def predictor_environment_id(self) -> int | None:
        """Id of the Python environment the predictor runs in."""
        return self._predictor_environment_id

    @public
    @property
    def predictor_environment(self) -> str | None:
        """Name of the Python environment the predictor runs in."""
        return self._predictor_environment

    @public
    @property
    def transformer(self) -> str | None:
        """Transformer script of this version, or None without a transformer.

        This is a base name inside the version directory, which stores it with a `transformer-` prefix, or an absolute HopsFS path for a script read from outside the version.
        """
        return self._transformer

    @public
    @property
    def transformer_resources(self) -> TransformerResources | None:
        """Resources allocated to the transformer."""
        return self._transformer_resources

    @public
    @property
    def transformer_scaling_config(self) -> TransformerScalingConfig | None:
        """Scaling configuration of the transformer."""
        return self._transformer_scaling_config

    @public
    @property
    def transformer_env_vars(self) -> list[str] | None:
        """User environment variables of the transformer, as `KEY=VALUE` strings."""
        return self._transformer_env_vars

    @public
    @property
    def transformer_environment_id(self) -> int | None:
        """Id of the Python environment the transformer runs in."""
        return self._transformer_environment_id

    @public
    @property
    def transformer_environment(self) -> str | None:
        """Name of the Python environment the transformer runs in."""
        return self._transformer_environment

    @public
    @property
    def git_url(self) -> str | None:
        """Git repository an agent deployment reads its scripts from."""
        return self._git_url

    @public
    @property
    def git_branch(self) -> str | None:
        """Branch of the git repository the deployment tracks."""
        return self._git_branch

    @public
    @property
    def git_provider(self) -> str | None:
        """Git provider of the repository."""
        return self._git_provider

    @public
    @property
    def git_auto_redeploy(self) -> bool | None:
        """Whether the deployment redeploys itself when the tracked branch moves."""
        return self._git_auto_redeploy

    @public
    @property
    def git_current_commit(self) -> str | None:
        """Commit the deployment was last rolled to by git-sync."""
        return self._git_current_commit

    @public
    @property
    def vllm_variant(self) -> str | None:
        """VLLM variant of an LLM deployment."""
        return self._vllm_variant

    @public
    @property
    def vllm_image_tag(self) -> str | None:
        """VLLM image tag of an LLM deployment."""
        return self._vllm_image_tag

    @public
    @property
    def tracing(self) -> DeploymentTracingConfig | None:
        """Tracing configuration of an agent deployment."""
        return self._tracing

    @public
    @property
    def feature_logging(self) -> DeploymentLoggingConfig | None:
        """Feature logging overrides of the predictor."""
        return self._feature_logging

    def __repr__(self):
        return (
            f"DeploymentVersion(version: {self._version!r}, active: {self._active!r})"
        )
