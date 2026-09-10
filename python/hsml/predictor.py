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
from __future__ import annotations

import ast
import json
import os
import re
from typing import Any

import humps
from hopsworks_apigen import public
from hopsworks_common import client, tag, util
from hopsworks_common.constants import (
    INFERENCE_ENDPOINTS,
    MODEL,
    MODEL_SERVING,
    PREDICTOR,
    SCALING_CONFIG,
    Default,
)
from hsml import deployment
from hsml.deployable_component import DeployableComponent
from hsml.deployment_schema import (
    OUTPUT_FEATURE_VECTORS,
    DeploymentSchema,
    _check_schema_refinement,
    _check_training_dataset_version,
    _infer_deployment_schema,
    _infer_model_deployment_schema,
)
from hsml.deployment_tracing_config import DeploymentTracingConfig
from hsml.inference_batcher import InferenceBatcher
from hsml.inference_logger import InferenceLogger
from hsml.predictor_state import PredictorState
from hsml.resources import PredictorResources
from hsml.scaling_config import (
    PredictorScalingConfig,
)
from hsml.transformer import Transformer


@public
class Predictor(DeployableComponent):
    """Metadata object representing a predictor in Model Serving."""

    @staticmethod
    def _get_raw_num_instances(resources):
        if resources is None:
            return None
        return (
            resources._num_instances
            if hasattr(resources, "_num_instances")
            else resources.num_instances
        )

    def __init__(
        self,
        name: str,
        model_server: str,
        model_name: str = None,
        model_path: str = None,
        model_version: int = None,
        model_framework: str = None,  # MODEL.FRAMEWORK
        serving_tool: str | None = None,
        script_file: str | None = None,
        config_file: str | None = None,
        resources: PredictorResources | dict | Default | None = None,  # base
        inference_logger: InferenceLogger | dict | Default | None = None,  # base
        inference_batcher: InferenceBatcher | dict | Default | None = None,  # base
        transformer: Transformer | dict | Default | None = None,
        id: int | None = None,
        version: int | None = None,
        description: str | None = None,
        created_at: str | None = None,
        creator: str | None = None,
        api_protocol: str | None = INFERENCE_ENDPOINTS.API_PROTOCOL_REST,
        environment: str | None = None,
        project_namespace: str = None,
        scaling_configuration: PredictorScalingConfig | dict | Default | None = None,
        env_vars: dict[str, str] | None = None,
        vllm_variant: str | None = None,
        vllm_image_tag: str | None = None,
        tracing: DeploymentTracingConfig | dict | Default | None = None,
        git_url: str | None = None,
        git_provider: str | None = None,
        git_branch: str | None = None,
        git_auto_redeploy: bool | None = None,
        git_current_commit: str | None = None,
        git_resolved_branch: str | None = None,
        missing_mandatory_tags: list[dict[str, Any]] | None = None,
        tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None = None,
        schema: DeploymentSchema | dict | None = None,
        default_predictor: bool | None = None,
        **kwargs,
    ):
        self._missing_mandatory_tags = missing_mandatory_tags or []
        self._tags = tag.Tag._normalize(tags)
        self._schema = util._get_obj_from_json(schema, DeploymentSchema)
        self._schema_loaded = False
        self._default_predictor = bool(default_predictor)
        serving_tool = (
            self._validate_serving_tool(serving_tool)
            or self._get_default_serving_tool()
        )
        resources = self._validate_resources(
            util._get_obj_from_json(resources, PredictorResources), serving_tool
        ) or self._get_default_resources(serving_tool)

        self._scaling_configuration = util._get_obj_from_json(
            scaling_configuration, PredictorScalingConfig
        ) or PredictorScalingConfig.get_default_scaling_configuration(
            serving_tool=serving_tool,
            min_instances=self._get_raw_num_instances(resources),
        )

        super().__init__(
            script_file,
            resources,
            inference_batcher,
            scaling_configuration=self._scaling_configuration,
        )

        self._name = name
        self._model_name = model_name
        self._model_path = model_path
        self._model_version = model_version
        self._model_framework = model_framework
        self._serving_tool = serving_tool
        self._model_server = model_server
        self._config_file = config_file
        self._id = id
        self._version = version
        self._description = description
        self._created_at = created_at
        self._creator = creator

        self._inference_logger = util._get_obj_from_json(
            inference_logger, InferenceLogger
        )
        self._transformer = util._get_obj_from_json(transformer, Transformer)
        self._validate_script_file(
            self._model_framework, self._script_file, self._default_predictor
        )
        self._api_protocol = api_protocol
        self._environment = environment
        self._project_namespace = project_namespace
        self._project_name = None
        self._env_vars = env_vars
        self._tracing = util._get_obj_from_json(tracing, DeploymentTracingConfig)
        self._git_url = git_url
        self._git_provider = git_provider
        self._git_branch = git_branch
        self._git_auto_redeploy = bool(git_auto_redeploy)
        self._git_current_commit = git_current_commit
        self._git_resolved_branch = git_resolved_branch
        self._vllm_variant = vllm_variant
        self._vllm_image_tag = vllm_image_tag

    @public
    def deploy(self) -> deployment.Deployment:
        """Create a deployment for this predictor and persists it in the Model Serving.

        Returns:
            The deployment metadata object of a new or existing deployment.

        Examples:
            ```python

            import hopsworks

            project = hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            # get Hopsworks Model Serving handle
            ms = project.get_model_serving()

            my_predictor = ms.create_predictor(my_model)
            my_deployment = my_predictor.deploy()

            print(my_deployment.get_state())
            ```
        """
        _deployment = deployment.Deployment(
            predictor=self, name=self._name, description=self._description
        )
        _deployment.save()

        return _deployment

    @public
    def describe(self):
        """Print a JSON description of the predictor."""
        util._pretty_print(self)

    def _set_state(self, state: PredictorState):
        """Set the state of the predictor."""
        self._state = state

    @classmethod
    def _validate_serving_tool(cls, serving_tool):
        if serving_tool is not None:
            if client._is_saas_connection():
                # only kserve supported in saasy hopsworks
                if serving_tool != PREDICTOR.SERVING_TOOL_KSERVE:
                    raise ValueError(
                        "KServe deployments are the only supported in Serverless Hopsworks"
                    )
                return serving_tool
            # if not saas, check valid serving_tool
            serving_tools = list(util._get_members(PREDICTOR, prefix="SERVING_TOOL"))
            if serving_tool not in serving_tools:
                raise ValueError(
                    "Serving tool '{}' is not valid. Possible values are '{}'".format(
                        serving_tool, ", ".join(serving_tools)
                    )
                )
        return serving_tool

    @classmethod
    def _validate_script_file(
        cls, model_framework, script_file, default_predictor=False
    ):
        if (
            script_file is None
            and model_framework == MODEL.FRAMEWORK_PYTHON
            and not default_predictor
        ):
            raise ValueError(
                "Predictor scripts are required in deployments for custom Python models."
            )

    @classmethod
    def _infer_model_server(cls, model_framework):
        if model_framework == MODEL.FRAMEWORK_TENSORFLOW:
            return PREDICTOR.MODEL_SERVER_TF_SERVING
        if model_framework == MODEL.FRAMEWORK_LLM:
            return PREDICTOR.MODEL_SERVER_VLLM
        return PREDICTOR.MODEL_SERVER_PYTHON

    @classmethod
    def _get_default_serving_tool(cls):
        # set kserve as default if it is available
        return (
            PREDICTOR.SERVING_TOOL_KSERVE
            if client._is_kserve_installed()
            else PREDICTOR.SERVING_TOOL_DEFAULT
        )

    @classmethod
    def _validate_resources(cls, resources, serving_tool):
        if (
            resources is not None
            and serving_tool == PREDICTOR.SERVING_TOOL_KSERVE
            and cls._get_raw_num_instances(resources) != 0
            and client._is_scale_to_zero_required()
        ):
            # ensure scale-to-zero for kserve deployments when required
            raise ValueError(
                "Scale-to-zero is required for KServe deployments in this cluster. Please, set the number of instances to 0."
            )
        return resources

    @classmethod
    def _get_default_resources(cls, serving_tool):
        num_instances = (
            0  # enable scale-to-zero by default if required
            if serving_tool == PREDICTOR.SERVING_TOOL_KSERVE
            and client._is_scale_to_zero_required()
            else SCALING_CONFIG.MIN_NUM_INSTANCES
        )
        return PredictorResources(num_instances)

    @public
    @classmethod
    def for_model(cls, model, **kwargs):
        kwargs["model_name"] = model.name
        kwargs["model_path"] = model.model_path
        kwargs["model_version"] = model.version

        _reject_reserved_env_vars(kwargs.get("env_vars"))
        default_predictor = kwargs.pop("default_predictor", None)
        schema = kwargs.pop("schema", None)
        passed_features = kwargs.pop("passed_features", None)
        if schema is not None and passed_features:
            raise ValueError(
                "Pass either schema or passed_features, not both: passed features are "
                "already part of a schema."
            )
        use_default, feature_view = _resolve_default_predictor(
            model, default_predictor, kwargs, passed_features
        )
        kwargs["default_predictor"] = use_default
        kwargs["schema"] = _resolve_model_schema(
            model, feature_view, use_default, schema, passed_features
        )
        if not use_default and passed_features:
            raise ValueError(
                "passed_features only applies to the default predictor. Pass "
                "default_predictor=True, or describe the request with schema=."
            )

        # get predictor for specific model, includes model type-related validations
        return util._get_predictor_for_model(model=model, **kwargs)

    @public
    @classmethod
    def for_feature_view(
        cls,
        feature_view: Any,
        name: str | None = None,
        training_dataset_version: int | None = None,
        passed_features: list[str] | None = None,
        schema: DeploymentSchema | dict | None = None,
        script_file: str | None = None,
        **kwargs: Any,
    ) -> Predictor:
        """Build the predictor of a feature view deployment: the default predictor with no model.

        Resolves the training dataset version from the last accessed training
        dataset when not given, checks that statistics-dependent
        transformations have one, infers the schema, and records the feature
        view identity in the predictor env vars. Further keyword arguments,
        such as `resources`, `environment`, or `env_vars`, pass through to
        `Predictor`.

        Parameters:
            feature_view: The feature view to serve.
            name: Deployment name; defaults to the view name and version without special characters.
            training_dataset_version: Training dataset whose statistics the transformations use.
            passed_features: Features of the view whose values clients send with each request.
            schema: A refinement of the inferred deployment schema, keeping its fields.
            script_file: A script subclassing `DefaultPredict` that ends with the `run_kserve_wrapper()` hand-over.

        Returns:
            The predictor, with the schema pending until the deployment is saved.
        """
        from hsml.python.feature_view_endpoint import FeatureViewEndpoint

        _reject_reserved_env_vars(kwargs.get("env_vars"))
        if name is None:
            name = re.sub(
                r"[^a-zA-Z0-9]", "", f"{feature_view.name}{feature_view.version}"
            )
        if training_dataset_version is None:
            training_dataset_version = feature_view.get_last_accessed_training_dataset()
        _check_training_dataset_version(
            feature_view,
            training_dataset_version,
            f"feature view '{feature_view.name}' v{feature_view.version}",
            "Pass training_dataset_version=<n> to deploy(), or read or create a "
            "training dataset from this feature view in this session first.",
        )
        given = util._get_obj_from_json(schema, DeploymentSchema)
        if given is not None:
            if passed_features:
                raise ValueError(
                    "Pass either schema or passed_features, not both: passed features "
                    "are already part of a schema."
                )
            # a manual schema carries its passed features; infer with the same ones
            passed_features = [f.name for f in given.passed_features] or None
        inferred = _infer_deployment_schema(
            feature_view,
            passed_features=passed_features,
            training_dataset_version=training_dataset_version,
            output_kind=OUTPUT_FEATURE_VECTORS,
        )
        if given is not None:
            _check_schema_refinement(inferred, given)
        if script_file is not None:
            _check_feature_view_script(script_file)

        env_vars = dict(kwargs.pop("env_vars", None) or {})
        env_vars[MODEL_SERVING.SCRIPT_KIND_ENV_VAR] = (
            MODEL_SERVING.SCRIPT_KIND_PREDICTOR
        )
        env_vars[MODEL_SERVING.FEATURE_VIEW_NAME_ENV_VAR] = feature_view.name
        env_vars[MODEL_SERVING.FEATURE_VIEW_VERSION_ENV_VAR] = str(feature_view.version)
        if training_dataset_version is not None:
            env_vars[MODEL_SERVING.TRAINING_DATASET_VERSION_ENV_VAR] = str(
                training_dataset_version
            )
        return FeatureViewEndpoint(
            name=name,
            script_file=script_file,
            schema=given or inferred,
            default_predictor=True,
            env_vars=env_vars,
            **kwargs,
        )

    @public
    @classmethod
    def for_server(cls, name: str, script_file: str, **kwargs):
        # get predictor for a HTTP server without model
        return util._get_predictor_for_server(
            name=name, script_file=script_file, **kwargs
        )

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if isinstance(json_decamelized, list):
            if len(json_decamelized) == 0:
                return []
            return [cls.from_json(predictor) for predictor in json_decamelized]
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls.from_json(predictor) for predictor in json_decamelized["items"]]
        return cls.from_json(json_decamelized)

    @classmethod
    def from_json(cls, json_decamelized):
        predictor = Predictor(**cls.extract_fields_from_json(json_decamelized))
        predictor._set_state(PredictorState.from_response_json(json_decamelized))
        return predictor

    @classmethod
    def extract_fields_from_json(cls, json_decamelized):
        kwargs = {}
        kwargs["name"] = json_decamelized.pop("name")
        kwargs["description"] = util._extract_field_from_json(
            json_decamelized, "description"
        )
        kwargs["version"] = json_decamelized.pop("version")
        with_model = "model_version" in json_decamelized
        kwargs["model_name"] = util._extract_field_from_json(
            json_decamelized,
            "model_name",
            default=(kwargs["name"] if with_model else None),
        )
        kwargs["model_version"] = util._extract_field_from_json(
            json_decamelized, "model_version"
        )
        kwargs["model_path"] = util._extract_field_from_json(
            json_decamelized, "model_path"
        )
        kwargs["model_framework"] = (
            json_decamelized.pop("model_framework")
            if "model_framework" in json_decamelized
            else (
                MODEL.FRAMEWORK_SKLEARN if with_model else None
            )  # backward compatibility
        )
        kwargs["model_server"] = json_decamelized.pop("model_server")
        kwargs["serving_tool"] = json_decamelized.pop("serving_tool")
        kwargs["script_file"] = util._extract_field_from_json(
            json_decamelized, "predictor"
        )
        kwargs["config_file"] = util._extract_field_from_json(
            json_decamelized, "config_file"
        )
        kwargs["resources"] = PredictorResources.from_json(json_decamelized)
        kwargs["inference_logger"] = InferenceLogger.from_json(json_decamelized)
        kwargs["inference_batcher"] = InferenceBatcher.from_json(json_decamelized)
        kwargs["transformer"] = Transformer.from_json(json_decamelized)
        kwargs["tracing"] = util._extract_field_from_json(
            json_decamelized,
            ["tracing", "tracing_config"],
            as_instance_of=DeploymentTracingConfig,
        )
        kwargs["git_url"] = util._extract_field_from_json(json_decamelized, "git_url")
        kwargs["git_provider"] = util._extract_field_from_json(
            json_decamelized, "git_provider"
        )
        kwargs["git_branch"] = util._extract_field_from_json(
            json_decamelized, "git_branch"
        )
        kwargs["git_auto_redeploy"] = util._extract_field_from_json(
            json_decamelized, "git_auto_redeploy"
        )
        kwargs["git_current_commit"] = util._extract_field_from_json(
            json_decamelized, "git_current_commit"
        )
        kwargs["git_resolved_branch"] = util._extract_field_from_json(
            json_decamelized, "git_resolved_branch"
        )
        kwargs["id"] = json_decamelized.pop("id")
        kwargs["created_at"] = json_decamelized.pop("created")
        kwargs["creator"] = json_decamelized.pop("creator")
        kwargs["api_protocol"] = json_decamelized.pop("api_protocol")
        if "environment_dto" in json_decamelized:
            environment = json_decamelized.pop("environment_dto")
            kwargs["environment"] = environment["name"]
        if "predictor_env_vars" in json_decamelized:
            env_vars = json_decamelized.pop("predictor_env_vars")
            kwargs["env_vars"] = (
                dict(e.split("=", 1) for e in env_vars) if env_vars else None
            )
        kwargs["project_namespace"] = json_decamelized.pop("project_namespace")
        kwargs["scaling_configuration"] = PredictorScalingConfig.from_json(
            json_decamelized
        )
        kwargs["vllm_variant"] = util._extract_field_from_json(
            json_decamelized, "vllm_variant"
        )
        kwargs["vllm_image_tag"] = util._extract_field_from_json(
            json_decamelized, "vllm_image_tag"
        )
        kwargs["missing_mandatory_tags"] = util._extract_field_from_json(
            json_decamelized, "missing_mandatory_tags"
        )
        return kwargs

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**self.extract_fields_from_json(json_decamelized))
        self._set_state(PredictorState.from_response_json(json_decamelized))
        return self

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        predictor_dict = {
            "id": self._id,
            "name": self._name,
            "description": self._description,
            "version": self._version,
            "created": self._created_at,
            "creator": self._creator,
            "modelServer": self._model_server,
            "servingTool": self._serving_tool,
            "predictor": self._script_file,
            "configFile": self._config_file,
            "apiProtocol": self._api_protocol,
            "projectNamespace": self._project_namespace,
        }
        if self._model_server == PREDICTOR.MODEL_SERVER_VLLM:
            predictor_dict = {
                **predictor_dict,
                "vllmVariant": self._vllm_variant,
                "vllmImageTag": self._vllm_image_tag,
            }
        if self.model_name is not None:
            predictor_dict = {**predictor_dict, "modelName": self._model_name}
        if self.model_path is not None:
            predictor_dict = {**predictor_dict, "modelPath": self._model_path}
        if self.model_version is not None:
            predictor_dict = {**predictor_dict, "modelVersion": self._model_version}
        if self.model_framework is not None:
            predictor_dict = {**predictor_dict, "modelFramework": self._model_framework}
        if self._env_vars:
            predictor_dict = {
                **predictor_dict,
                "predictorEnvVars": [f"{k}={v}" for k, v in self._env_vars.items()],
            }
        if self.environment is not None:
            predictor_dict = {
                **predictor_dict,
                "environmentDTO": {"name": self._environment},
            }
        if self._resources is not None:
            predictor_dict = {**predictor_dict, **self._resources.to_dict()}
        if self._inference_logger is not None:
            predictor_dict = {**predictor_dict, **self._inference_logger.to_dict()}
        if self._inference_batcher is not None:
            predictor_dict = {**predictor_dict, **self._inference_batcher.to_dict()}
        if self._transformer is not None:
            predictor_dict = {**predictor_dict, **self._transformer.to_dict()}
        if self._tracing is not None:
            predictor_dict = {**predictor_dict, "tracing": self._tracing.to_dict()}
        if self._git_url is not None:
            predictor_dict = {**predictor_dict, "gitUrl": self._git_url}
        if self._git_provider is not None:
            predictor_dict = {**predictor_dict, "gitProvider": self._git_provider}
        if self._git_branch is not None:
            predictor_dict = {**predictor_dict, "gitBranch": self._git_branch}
        if self._git_url is not None:
            # Only meaningful alongside a git source, and the backend rejects it
            # without one, so it rides with git_url rather than being sent alone.
            predictor_dict = {
                **predictor_dict,
                "gitAutoRedeploy": self._git_auto_redeploy,
            }
        if self._scaling_configuration is not None:
            predictor_dict = {**predictor_dict, **self._scaling_configuration.to_dict()}
        tags_dict = tag.Tag._tags_to_dict(self._tags)
        if tags_dict:
            predictor_dict = {**predictor_dict, "tags": tags_dict}
        return predictor_dict

    @public
    @property
    def id(self):
        """Id of the predictor."""
        return self._id

    @public
    @property
    def name(self):
        """Name of the predictor."""
        return self._name

    @name.setter
    def name(self, name: str):
        self._name = name

    @public
    @property
    def version(self):
        """Version of the predictor."""
        return self._version

    @public
    @property
    def description(self):
        """Description of the predictor."""
        return self._description

    @description.setter
    def description(self, description: str):
        self._description = description

    @public
    @property
    def model_name(self):
        """Name of the model deployed by the predictor."""
        return self._model_name

    @model_name.setter
    def model_name(self, model_name: str):
        self._model_name = model_name

    @public
    @property
    def model_path(self):
        """Model path deployed by the predictor."""
        return self._model_path

    @model_path.setter
    def model_path(self, model_path: str):
        self._model_path = model_path

    @public
    @property
    def model_version(self):
        """Model version deployed by the predictor."""
        return self._model_version

    @model_version.setter
    def model_version(self, model_version: int):
        self._model_version = model_version

    @public
    @property
    def model_framework(self):
        """Model framework of the model to be deployed by the predictor."""
        return self._model_framework

    @model_framework.setter
    def model_framework(self, model_framework: str):
        self._model_framework = model_framework
        self._model_server = self._infer_model_server(model_framework)

    @public
    @property
    def artifact_version(self):
        """Artifact version deployed by the predictor.

        Warning: Deprecated
            Artifact versions are deprecated in favor of deployment versions.
        """
        return self._version

    @artifact_version.setter
    def artifact_version(self, artifact_version: int | str):
        pass  # do nothing, kept for backward compatibility

    @public
    @property
    def artifact_files_path(self):
        """Path of the artifact files deployed by the predictor."""
        # "/Projects/{project_name}/Deployments/{name}/{version}"
        return "{}/{}/{}/{}/{}".format(
            "/Projects",
            self._project_name,
            MODEL_SERVING.DEPLOYMENTS_DATASET,
            str(self._name),
            str(self._version),
        )

    @public
    @property
    def artifact_path(self):
        """Path of the model artifact deployed by the predictor. Resolves to /Projects/{project_name}/Models/{name}/{version}/Artifacts/{artifact_version}/{name}_{version}_{artifact_version}.zip."""
        # TODO: Deprecated
        artifact_name = f"{self._model_name}_{str(self._model_version)}_{str(self._artifact_version)}.zip"
        return f"{self._model_path}/{str(self._model_version)}/Artifacts/{str(self._artifact_version)}/{artifact_name}"

    @public
    @property
    def model_server(self):
        """Model server used by the predictor."""
        return self._model_server

    @public
    @property
    def serving_tool(self):
        """Serving tool used to run the model server."""
        return self._serving_tool

    @serving_tool.setter
    def serving_tool(self, serving_tool: str):
        self._serving_tool = serving_tool

    @public
    @property
    def script_file(self):
        """Script file used to load and run the model."""
        return self._script_file

    @script_file.setter
    def script_file(self, script_file: str):
        self._script_file = script_file

    @public
    @property
    def config_file(self):
        """Model server configuration file passed to the model deployment.

        It can be accessed via `CONFIG_FILE_PATH` environment variable from a predictor or transformer script.
        For LLM deployments without a predictor script, this file is used to configure the vLLM engine.
        """
        return self._config_file

    @config_file.setter
    def config_file(self, config_file: str):
        self._config_file = config_file

    @public
    @property
    def inference_logger(self):
        """Configuration of the inference logger attached to this predictor."""
        return self._inference_logger

    @inference_logger.setter
    def inference_logger(self, inference_logger: InferenceLogger):
        self._inference_logger = inference_logger

    @public
    @property
    def transformer(self):
        """Transformer configuration attached to the predictor."""
        return self._transformer

    @transformer.setter
    def transformer(self, transformer: Transformer):
        self._transformer = transformer

    @public
    @property
    def tracing(self):
        """Tracing configuration attached to the predictor."""
        return self._tracing

    @tracing.setter
    def tracing(self, tracing: DeploymentTracingConfig | dict | Default | None):
        self._tracing = util._get_obj_from_json(tracing, DeploymentTracingConfig)

    @public
    @property
    def git_url(self):
        """Configured Git repository URL for this deployment."""
        return self._git_url

    @git_url.setter
    def git_url(self, git_url: str | None):
        self._git_url = git_url

    @public
    @property
    def git_provider(self):
        """Configured Git provider for this deployment."""
        return self._git_provider

    @git_provider.setter
    def git_provider(self, git_provider: str | None):
        self._git_provider = git_provider

    @public
    @property
    def git_branch(self):
        """Configured Git branch for this deployment."""
        return self._git_branch

    @git_branch.setter
    def git_branch(self, git_branch: str | None):
        self._git_branch = git_branch

    @public
    @property
    def git_auto_redeploy(self) -> bool:
        """Whether the deployment is rolled to the branch HEAD when a new commit is pushed."""
        return self._git_auto_redeploy

    @git_auto_redeploy.setter
    def git_auto_redeploy(self, git_auto_redeploy: bool | None):
        self._git_auto_redeploy = bool(git_auto_redeploy)

    @public
    @property
    def git_current_commit(self) -> str | None:
        """Commit this deployment is currently running.

        Read-only, server-managed: recorded when the deployment clones the
        repository and when auto-redeploy rolls it to a new commit.
        """
        return self._git_current_commit

    @public
    @property
    def git_resolved_branch(self) -> str | None:
        """Branch the running clone resolved to.

        Read-only. Only set when no branch was configured, in which case this
        is the repository's default branch that the deployment checked out.
        """
        return self._git_resolved_branch

    @public
    @property
    def created_at(self):
        """Created at date of the predictor."""
        return self._created_at

    @public
    @property
    def creator(self):
        """Creator of the predictor."""
        return self._creator

    @public
    @property
    def requested_instances(self):
        """Total number of requested instances in the predictor."""
        num_instances = self._get_raw_num_instances(self._resources)
        if self._transformer is not None:
            num_instances += self._get_raw_num_instances(self._transformer.resources)
        return num_instances

    @public
    @property
    def schema(self) -> DeploymentSchema | None:
        """Deployment schema: the one set in this session, else the one the deployment's revision names, else `None`.

        Reading a persisted schema downloads
        `Deployments/<name>/resources/schema/<id>.json` once per object.
        Setting a schema marks it pending; `save()` publishes it.
        """
        if self._schema is not None or self._schema_loaded:
            return self._schema
        self._schema_loaded = True
        schema_id = self.schema_id
        if schema_id is None:
            return None
        from hsml.engine import serving_engine

        self._schema = serving_engine.ServingEngine()._read_schema(self, schema_id)
        return self._schema

    @schema.setter
    def schema(self, schema: DeploymentSchema | dict | None):
        self._schema = util._get_obj_from_json(schema, DeploymentSchema)
        self._schema_loaded = False

    @public
    @property
    def schema_id(self) -> str | None:
        """Id of the schema this deployment's revision serves, from its env vars."""
        if self._schema is not None:
            return self._schema.schema_id
        return (self._env_vars or {}).get(MODEL_SERVING.DEPLOYMENT_SCHEMA_ID_ENV_VAR)

    @public
    @property
    def default_predictor(self) -> bool:
        """Whether the library's default predictor serves this deployment."""
        return self._default_predictor

    @public
    @property
    def feature_view_name(self) -> str | None:
        """Feature view served by a feature view deployment, else `None`."""
        return (self._env_vars or {}).get(MODEL_SERVING.FEATURE_VIEW_NAME_ENV_VAR)

    @public
    @property
    def feature_view_version(self) -> int | None:
        """Feature view version served by a feature view deployment, else `None`."""
        version = (self._env_vars or {}).get(MODEL_SERVING.FEATURE_VIEW_VERSION_ENV_VAR)
        return int(version) if version is not None else None

    @public
    @property
    def has_feature_view(self) -> bool:
        """Whether this is a feature view deployment."""
        return self.feature_view_name is not None

    @public
    @property
    def api_protocol(self):
        """API protocol enabled in the predictor (e.g., HTTP or GRPC)."""
        return self._api_protocol

    @api_protocol.setter
    def api_protocol(self, api_protocol):
        self._api_protocol = api_protocol

    @public
    @property
    def env_vars(self):
        """Environment variables of the predictor."""
        return self._env_vars

    @env_vars.setter
    def env_vars(self, env_vars: dict[str, str] | None):
        self._env_vars = env_vars

    @public
    @property
    def environment(self):
        """Name of the inference environment."""
        return self._environment

    @environment.setter
    def environment(self, environment):
        self._environment = environment

    @public
    @property
    def project_namespace(self):
        """Kubernetes project namespace."""
        return self._project_namespace

    @project_namespace.setter
    def project_namespace(self, project_namespace):
        self._project_namespace = project_namespace

    @public
    @property
    def project_name(self):
        """Name of the project the deployment belongs to."""
        return self._project_name

    @project_name.setter
    def project_name(self, project_name: str):
        self._project_name = project_name

    @public
    @property
    def vllm_variant(self):
        """VLLM image variant for this predictor (VLLM or VLLM_OMNI)."""
        return self._vllm_variant

    @vllm_variant.setter
    def vllm_variant(self, vllm_variant: str):
        self._vllm_variant = vllm_variant

    @public
    @property
    def vllm_image_tag(self):
        """VLLM image tag override; None means use the cluster default."""
        return self._vllm_image_tag

    @vllm_image_tag.setter
    def vllm_image_tag(self, vllm_image_tag: str):
        self._vllm_image_tag = vllm_image_tag

    @public
    def get_endpoint_url(self) -> str | None:
        """Get the base endpoint URL for this predictor.

        Returns the base URL that can be used with external HTTP clients.
        This is the path-based routing base endpoint without any protocol-specific
        suffixes like `:predict` or `/v1`.

        If Istio client is not available, returns `None` (Hopsworks REST API
        doesn't support base-only endpoints).

        Returns:
            Base endpoint URL, or `None` if unavailable.

        Examples:
            ```python
            url = predictor.get_endpoint_url()
            # url = "https://host:port/v1/project/name"
            ```
        """
        from hsml.core import serving_api

        serving = serving_api.ServingApi()

        istio_client = client.istio._get_instance()
        if istio_client is not None:
            path_parts = serving._get_istio_inference_path(self, base_only=True)
            return f"{istio_client._base_url}/{'/'.join(str(p) for p in path_parts)}"

        # Hopsworks REST API doesn't support base-only endpoints
        return None

    @public
    def get_openai_url(self) -> str | None:
        """Get the OpenAI-compatible API URL for vLLM deployments.

        Returns the URL for OpenAI-compatible API endpoints (e.g., /v1/chat/completions).
        This method only returns a URL for LLM (vLLM) deployments.

        Returns:
            OpenAI-compatible URL (base URL + "/v1"), or `None` if not a LLM deployment.

        Examples:
            ```python
            url = predictor.get_openai_compatible_url()
            # url = "https://host:port/v1/project/name/v1"
            # Then use: url + "/chat/completions"
            ```
        """
        if self._model_server != PREDICTOR.MODEL_SERVER_VLLM:
            return None

        base_url = self.get_endpoint_url()
        if base_url is None:
            return None

        return f"{base_url}/v1"

    @public
    def get_inference_url(self) -> str | None:
        """Get the KServe inference URL for standard model deployments and feature view deployments.

        Returns the full URL with `:predict` suffix for KServe inference protocol.
        This method only returns a URL for standard model deployments (non-vLLM,
        with a model attached) and for feature view deployments.

        If Istio client is not available, falls back to Hopsworks REST API path
        for model deployments. A feature view deployment is only reachable
        through Istio, because the Hopsworks REST proxy forwards requests for
        deployments without a model to the pod root, not to the KServe predict
        route; `None` is returned when Istio is unavailable.

        Returns:
            Inference URL with `:predict` suffix, or `None` if not a standard model deployment.

        Examples:
            ```python
            url = predictor.get_inference_url()
            # url = "https://host:port/v1/project/name/v1/models/name:predict"
            ```
        """
        from hsml.core import serving_api

        # Only for standard model deployments (has model, not vLLM)
        has_model = self._model_name is not None and self._model_version is not None
        if self._model_server == PREDICTOR.MODEL_SERVER_VLLM or not (
            has_model or self.has_feature_view
        ):
            return None

        serving = serving_api.ServingApi()

        # Try Istio client first
        istio_client = client.istio._get_instance()
        if istio_client is not None:
            path_parts = serving._get_istio_inference_path(self, base_only=False)
            return f"{istio_client._base_url}/{'/'.join(str(p) for p in path_parts)}"
        if not has_model:
            return None

        # Fallback to Hopsworks REST API path
        hopsworks_client = client._get_instance()
        path_parts = serving._get_hopsworks_inference_path(
            hopsworks_client._project_id, self
        )
        return f"{hopsworks_client._base_url}/hopsworks-api/api/{'/'.join(str(p) for p in path_parts)}"

    def __repr__(self):
        desc = (
            f", description: {self._description!r}"
            if self._description is not None
            else ""
        )
        git = ""
        if self._git_url is not None:
            git += f", git_url: {self._git_url!r}"
        if self._git_provider is not None:
            git += f", git_provider: {self._git_provider!r}"
        if self._git_branch is not None:
            git += f", git_branch: {self._git_branch!r}"
        if self._git_url is not None:
            git += f", git_auto_redeploy: {self._git_auto_redeploy!r}"
        return f"Predictor(name: {self._name!r}" + desc + git + ")"


# region Default predictor resolution


def _reject_reserved_env_vars(env_vars: dict[str, str] | None) -> None:
    if not env_vars:
        return
    reserved = sorted(set(env_vars) & set(MODEL_SERVING.RESERVED_ENV_VARS))
    if reserved:
        raise ValueError(
            f"Environment variables {reserved} are set by Hopsworks for this deployment "
            "and cannot be overridden."
        )


def _resolve_feature_view(model):
    feature_view = getattr(model, "_feature_view", None)
    # A model fetched from the registry carries the backend's feature view
    # JSON, not a usable object; only an in-session create_model keeps the real one.
    if feature_view is not None and hasattr(feature_view, "serving_keys"):
        return feature_view
    resolved = model.get_feature_view(init=False)
    if (
        resolved is None
        and isinstance(feature_view, dict)
        and feature_view.get("name")
        and feature_view.get("version") is not None
    ):
        # Right after save() the provenance link may not exist yet (it needs a
        # training dataset), but the response still names the view.
        import hopsworks

        if not hopsworks._connected_project:
            from hopsworks_common.client.exceptions import FeatureStoreException

            raise FeatureStoreException(
                f"Model '{model.name}' names feature view {feature_view['name']} "
                f"v{feature_view['version']}, but no project is connected to resolve "
                "it. Call hopsworks.login() first."
            )
        resolved = hopsworks._connected_project.get_feature_store().get_feature_view(
            feature_view["name"], int(feature_view["version"])
        )
    return resolved


def _resolve_default_predictor(model, default_predictor, kwargs, passed_features=None):
    """Decide whether the default predictor serves `model`, per the resolution table of the spec.

    Returns `(use_default, feature_view)`. Automatic mode only turns on for
    `PYTHON` models with a feature view, no script, no transformer, REST, and
    KServe; forced mode raises on the first unmet condition.
    """
    if default_predictor is False:
        return False, None
    # A model without a known framework is never picked automatically; users
    # can still force the default predictor for it.
    framework = getattr(model, "framework", None)
    script_file = kwargs.get("script_file")
    transformer = kwargs.get("transformer")
    api_protocol = kwargs.get("api_protocol") or INFERENCE_ENDPOINTS.API_PROTOCOL_REST
    serving_tool = kwargs.get("serving_tool") or Predictor._get_default_serving_tool()

    if default_predictor is None:
        eligible = (
            framework == MODEL.FRAMEWORK_PYTHON
            and script_file is None
            and transformer is None
            and api_protocol == INFERENCE_ENDPOINTS.API_PROTOCOL_REST
            and serving_tool == PREDICTOR.SERVING_TOOL_KSERVE
        )
        if not eligible:
            return False, None
        feature_view = _resolve_feature_view(model)
        return feature_view is not None, feature_view

    problems = []
    if framework not in (MODEL.FRAMEWORK_PYTHON, MODEL.FRAMEWORK_SKLEARN):
        problems.append(
            f"the model framework is {framework}, only PYTHON and SKLEARN are supported"
        )
    if transformer is not None:
        problems.append("a transformer is configured")
    if api_protocol != INFERENCE_ENDPOINTS.API_PROTOCOL_REST:
        problems.append(f"the API protocol is {api_protocol}, only REST is supported")
    if serving_tool != PREDICTOR.SERVING_TOOL_KSERVE:
        problems.append(f"the serving tool is {serving_tool}, only KSERVE is supported")
    feature_view = None
    if not problems:
        feature_view = _resolve_feature_view(model)
        if (
            feature_view is None
            and not passed_features
            and not _model_input_columns(model)
        ):
            problems.append(
                "the model has no feature view; register it with "
                "create_model(feature_view=...) or name its input columns with "
                "passed_features=[...]"
            )
    if problems:
        raise ValueError("The default predictor cannot be used: " + problems[0] + ".")
    return True, feature_view


def _model_input_columns(model) -> list[dict]:
    model_schema = model.model_schema
    if not model_schema:
        return []
    input_ = humps.decamelize(model_schema).get("input_schema") or {}
    return [c for c in (input_.get("columnar_schema") or []) if c.get("name")]


def _model_output_columns(model) -> list[dict] | None:
    model_schema = model.model_schema
    if not model_schema:
        return None
    output = humps.decamelize(model_schema).get("output_schema") or {}
    columns = output.get("columnar_schema")
    if not columns:
        return None
    return [
        {"name": c.get("name"), "type": c.get("type")} for c in columns if c.get("name")
    ]


def _resolve_model_schema(model, feature_view, use_default, schema, passed_features):
    given = util._get_obj_from_json(schema, DeploymentSchema)
    if not use_default:
        return given
    if given is not None and passed_features is None:
        # a manual schema carries its passed features; infer with the same ones
        passed_features = [f.name for f in given.passed_features] or None
    if feature_view is None:
        inferred = _infer_model_deployment_schema(
            model.name,
            passed_features,
            model_input_columns=_model_input_columns(model),
            output_columns=_model_output_columns(model),
        )
        if given is not None:
            _check_schema_refinement(inferred, given)
            return given
        return inferred
    training_dataset_version = model.training_dataset_version
    _check_training_dataset_version(
        feature_view,
        training_dataset_version,
        f"model '{model.name}' v{model.version}",
        "Pass training_dataset_version=<n> to create_model(), or read the training "
        "dataset from the feature view before saving the model.",
    )
    inferred = _infer_deployment_schema(
        feature_view,
        passed_features=passed_features,
        training_dataset_version=training_dataset_version,
        output_columns=_model_output_columns(model),
    )
    if given is not None:
        _check_schema_refinement(inferred, given)
        return given
    return inferred


def _check_feature_view_script(script_file: str) -> None:
    """Require the KServe wrapper hand-over in a feature view deployment's script.

    Backends that honour `SERVING_SCRIPT_KIND=predictor` start the wrapper
    themselves and never run the script's `__main__` block; older backends
    start `python <script>`, so the footer is what makes the script serve.
    """
    if not os.path.isfile(script_file):
        return
    with open(script_file, encoding="utf-8") as f:
        source = f.read()
    try:
        tree = ast.parse(source, filename=script_file)
    except SyntaxError as e:
        raise ValueError(f"{script_file} is not valid Python: {e}") from e
    if not _hands_over_at_import(tree):
        raise ValueError(
            f"{script_file} must end with the hand-over to the KServe wrapper, because a "
            "deployment without a model may be started as a plain script:\n\n"
            "    from hsml.default_predictor import run_kserve_wrapper\n\n"
            '    if __name__ == "__main__":\n'
            "        run_kserve_wrapper()\n"
        )


def _hands_over_at_import(tree: ast.Module) -> bool:
    """True when running the module calls `run_kserve_wrapper`: a top-level call, or one under `if __name__ == "__main__"`."""
    for stmt in tree.body:
        if isinstance(stmt, ast.If) and _tests_dunder_main(stmt.test):
            statements = stmt.body
        else:
            statements = [stmt]
        for statement in statements:
            if isinstance(statement, ast.Expr) and _calls_run_kserve_wrapper(
                statement.value
            ):
                return True
    return False


def _tests_dunder_main(test: ast.expr) -> bool:
    if not (
        isinstance(test, ast.Compare)
        and len(test.ops) == 1
        and isinstance(test.ops[0], ast.Eq)
    ):
        return False
    parts = [test.left, *test.comparators]
    names = {p.id for p in parts if isinstance(p, ast.Name)}
    values = {p.value for p in parts if isinstance(p, ast.Constant)}
    return names == {"__name__"} and values == {"__main__"}


def _calls_run_kserve_wrapper(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    return name == "run_kserve_wrapper"


# endregion
