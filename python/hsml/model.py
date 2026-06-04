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

import json
import logging
import os
import re
import shutil
import warnings
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import client, usage, util
from hopsworks_common.constants import INFERENCE_ENDPOINTS as IE
from hopsworks_common.constants import MODEL_REGISTRY
from hsml.core import explicit_provenance
from hsml.engine import model_engine
from hsml.model_schema import ModelSchema
from hsml.predictor import Predictor
from hsml.schema import Schema


if TYPE_CHECKING:
    from hsfs import feature_view
    from hsfs.core.feature_monitoring_config import FeatureMonitoringConfig
    from hsml import deployment, tag
    from hsml.inference_batcher import InferenceBatcher
    from hsml.inference_logger import InferenceLogger
    from hsml.resources import PredictorResources
    from hsml.scaling_config import PredictorScalingConfig
    from hsml.transformer import Transformer


_logger = logging.getLogger(__name__)


@public
class Model:
    NOT_FOUND_ERROR_CODE = 360000
    """Metadata object representing a model in the Model Registry."""

    def __init__(
        self,
        id,
        name,
        version=None,
        created=None,
        creator=None,
        environment=None,
        description=None,
        project_name=None,
        metrics=None,
        program=None,
        user_full_name=None,
        model_schema=None,
        input_example=None,
        framework=None,
        model_registry_id=None,
        # unused, but needed since they come in the backend response
        tags=None,
        href=None,
        feature_view=None,
        training_dataset_version=None,
        **kwargs,
    ):
        self._id = id
        self._name = name
        self._version = version

        if description is None:
            self._description = "A collection of models for " + name
        else:
            self._description = description

        self._created = created
        self._creator = creator
        self._environment = environment
        self._project_name = project_name
        self._training_metrics = metrics
        self._program = program
        self._user_full_name = user_full_name
        self._input_example = input_example
        self._framework = framework
        self._model_schema = model_schema

        # This is needed for update_from_response_json function to not overwrite name of the shared registry this model originates from
        if not hasattr(self, "_shared_registry_project_name"):
            self._shared_registry_project_name = None

        self._model_registry_id = model_registry_id

        self._model_engine = model_engine.ModelEngine()
        self._feature_view = feature_view
        self._training_dataset_version = training_dataset_version

    @public
    @usage._method_logger
    def save(
        self,
        model_path: str,
        await_registration: int = 480,
        keep_original_files: bool = False,
        upload_configuration: dict[str, Any] | None = None,
    ) -> Model:
        """Persist this model including model files and metadata to the model registry.

        Parameters:
            model_path: Local or remote (Hopsworks file system) path to the folder where the model files are located, or path to a specific model file.
            await_registration: Awaiting time for the model to be registered in Hopsworks.
            keep_original_files: If the model files are located in hopsfs, whether to move or copy those files into the Models dataset. Default is False (i.e., model files will be moved)
            upload_configuration: When saving a model from outside Hopsworks, the model is uploaded to the model registry using the REST APIs. Each model artifact is divided into
                chunks and each chunk uploaded independently. This parameter can be used to control the upload chunk size, the parallelism and the number of retries.
                `upload_configuration` can contain the following keys:
                * key `chunk_size`: size of each chunk in megabytes. Default 10.
                * key `simultaneous_uploads`: number of chunks to upload in parallel. Default 3.
                * key `max_chunk_retries`: number of times to retry the upload of a chunk in case of failure. Default 1.

        Returns:
            The model metadata object.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue
        """
        if self._training_dataset_version is None and self._feature_view is not None:
            if self._feature_view.get_last_accessed_training_dataset() is not None:
                self._training_dataset_version = (
                    self._feature_view.get_last_accessed_training_dataset()
                )
            else:
                warnings.warn(
                    "Provenance cached data - feature view provided, but training dataset version is missing",
                    util.ProvenanceWarning,
                    stacklevel=1,
                )
        if self._model_schema is None:
            if (
                self._feature_view is not None
                and self._training_dataset_version is not None
            ):
                all_features = self._feature_view.get_training_dataset_schema(
                    self._training_dataset_version
                )
                features, labels = [], []
                for feature in all_features:
                    (labels if feature.label else features).append(feature.to_dict())
                self._model_schema = ModelSchema(
                    input_schema=Schema(features) if features else None,
                    output_schema=Schema(labels) if labels else None,
                )
            else:
                warnings.warn(
                    "Model schema cannot not be inferred without both the feature view and the training dataset version.",
                    util.ProvenanceWarning,
                    stacklevel=1,
                )

        return self._model_engine._save(
            model_instance=self,
            model_path=model_path,
            await_registration=await_registration,
            keep_original_files=keep_original_files,
            upload_configuration=upload_configuration,
        )

    @public
    @usage._method_logger
    def download(self, local_path: str | None = None) -> str:
        """Download the model files.

        If local_path is not provided, the model is downloaded to a cache directory
        under `/tmp/hopsworks/models/{project_name}/{model_name}/{version}/{id}/` and
        reused across subsequent downloads, including across processes.
        Cached models persist beyond program exit.
        The cache path includes the backend model `id`, so a model version that is
        deleted and recreated is re-downloaded automatically without manual cache
        invalidation.
        If the temp location is unusable (disk full, read-only, or no permission),
        the download falls back to `~/.hopsworks/cache/models` and then the current
        working directory.
        Use [`Model.clear_cache`][hsml.model.Model.clear_cache] to reclaim disk space.

        Parameters:
            local_path: path where to download the model files in the local filesystem.
                If None, downloads to cache directory (recommended for idempotent reuse).

        Returns:
            Absolute path to local folder containing the model files.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue
        """
        return self._model_engine._download(model_instance=self, local_path=local_path)

    @public
    @usage._method_logger
    def delete(self):
        """Delete the model.

        Danger: Potentially dangerous operation
            This operation drops all metadata associated with **this version** of the
            model **and** deletes the model files.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue
        """
        self._model_engine._delete(model_instance=self)

    @public
    @staticmethod
    def clear_cache(
        project_name: str | None = None,
        model_name: str | None = None,
        version: int | None = None,
    ) -> int:
        """Clear cached downloaded models.

        Utility method to clear the model cache from every fallback location
        (temp dir, the Hopsworks home `.cache`, and the working directory).
        Use this to free disk space when cached models are no longer needed.

        The filters narrow from broad to specific:
        `model_name` requires `project_name`, and `version` requires both
        `project_name` and `model_name`.

        Parameters:
            project_name: If specified, only clear cache for this project.
                If None, clears all cached models.
            model_name: If specified (requires project_name), only clear cache
                for this specific model. If None, clears all models in project.
            version: If specified (requires project_name and model_name), only clear
                cache for this specific model version. If None, clears all versions.

        Returns:
            Number of model versions removed from cache.

        Raises:
            ValueError: If `model_name` is given without `project_name`, or
                `version` is given without both `project_name` and `model_name`.

        Example:
            ```python
            # Clear all cached models
            Model.clear_cache()

            # Clear all models for a specific project
            Model.clear_cache(project_name="my_project")

            # Clear all versions of a specific model
            Model.clear_cache(project_name="my_project", model_name="my_model")

            # Clear a specific model version
            Model.clear_cache(project_name="my_project", model_name="my_model", version=1)
            ```
        """
        if model_name is not None and project_name is None:
            raise ValueError(
                "model_name requires project_name; refusing to clear cache to "
                "avoid deleting every cached project's models."
            )
        if version is not None and (project_name is None or model_name is None):
            raise ValueError(
                "version requires both project_name and model_name; refusing to "
                "clear cache to avoid deleting unintended models."
            )

        return sum(
            Model._clear_cache_base(cache_base, project_name, model_name, version)
            for cache_base in model_engine._model_cache_base_dirs()
        )

    @staticmethod
    def _clear_cache_base(cache_base, project_name, model_name, version):
        """Clear cached models under a single cache base directory.

        See `clear_cache` for the meaning of the filter parameters.

        Returns:
            int: Number of model versions removed from this base directory.
        """
        removed_count = 0

        if not os.path.exists(cache_base):
            return 0

        if project_name is None:
            # Clear entire cache - count before removing
            for project_dir in os.listdir(cache_base):
                project_path = os.path.join(cache_base, project_dir)
                if os.path.isdir(project_path):
                    for model_dir in os.listdir(project_path):
                        model_path = os.path.join(project_path, model_dir)
                        if os.path.isdir(model_path):
                            removed_count += len(
                                [
                                    d
                                    for d in os.listdir(model_path)
                                    if os.path.isdir(os.path.join(model_path, d))
                                ]
                            )
            shutil.rmtree(cache_base)
            return removed_count

        project_path = os.path.join(cache_base, project_name)
        if not os.path.exists(project_path):
            return 0

        if model_name is None:
            # Clear all models in project
            for model_dir in os.listdir(project_path):
                model_path = os.path.join(project_path, model_dir)
                if os.path.isdir(model_path):
                    removed_count += len(
                        [
                            d
                            for d in os.listdir(model_path)
                            if os.path.isdir(os.path.join(model_path, d))
                        ]
                    )
            shutil.rmtree(project_path)
            return removed_count

        model_path = os.path.join(project_path, model_name)
        if not os.path.exists(model_path):
            return 0

        if version is None:
            # Clear all versions of the model
            removed_count = len(
                [
                    d
                    for d in os.listdir(model_path)
                    if os.path.isdir(os.path.join(model_path, d))
                ]
            )
            shutil.rmtree(model_path)
            return removed_count

        # Clear specific version
        version_path = os.path.join(model_path, str(version))
        if os.path.exists(version_path):
            shutil.rmtree(version_path)
            removed_count = 1

        return removed_count

    @public
    @usage._method_logger
    def deploy(
        self,
        name: str | None = None,
        description: str | None = None,
        artifact_version: (
            str | None
        ) = None,  # deprecated, kept for backward compatibility
        serving_tool: str | None = None,
        script_file: str | None = None,
        config_file: str | None = None,
        resources: PredictorResources | dict | None = None,
        inference_logger: InferenceLogger | dict | None = None,
        inference_batcher: InferenceBatcher | dict | None = None,
        scaling_configuration: PredictorScalingConfig | dict | None = None,
        transformer: Transformer | dict | None = None,
        api_protocol: str | None = IE.API_PROTOCOL_REST,
        environment: str | None = None,
        env_vars: dict | None = None,
        vllm_variant: str | None = None,
        vllm_image_tag: str | None = None,
    ) -> deployment.Deployment:
        """Deploy the model.

        Example:
            ```python

            import hopsworks

            project = hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            my_deployment = my_model.deploy()
            ```
        Parameters:
            name: Name of the deployment.
            description: Description of the deployment.
            artifact_version: **Deprecated**. Version number of the model artifact to deploy, `CREATE` to create a new model artifact
            or `MODEL-ONLY` to reuse the shared artifact containing only the model files.
            serving_tool: Serving tool used to deploy the model server.
            script_file: Path to a custom predictor script implementing the Predict class.
            config_file: Model server configuration file to be passed to the model deployment.
                It can be accessed via `CONFIG_FILE_PATH` environment variable from a predictor or transformer script.
                For LLM deployments without a predictor script, this file is used to configure the vLLM engine.
            resources: Resources to be allocated for the predictor.
            inference_logger: Inference logger configuration.
            inference_batcher: Inference batcher configuration.
            scaling_configuration: Scaling configuration for the predictor.
            transformer: Transformer to be deployed together with the predictor.
            api_protocol: API protocol to be enabled in the deployment (i.e., 'REST' or 'GRPC').
            environment: The inference environment to use.
            env_vars: Environment variables to set on the predictor.
            vllm_variant: vLLM image variant for vLLM deployments. One of `'VLLM'` or `'VLLM_OMNI'`. Ignored for non-vLLM model servers.
            vllm_image_tag: vLLM image tag override. `None` uses the cluster default; if set, it should match one of the tags made available by a cluster administrator. Ignored for non-vLLM model servers.

        Returns:
            The deployment metadata object of a new or existing deployment.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue
        """
        if name is None:
            name = self._get_default_serving_name()

        predictor = Predictor.for_model(
            self,
            name=name,
            description=description,
            serving_tool=serving_tool,
            script_file=script_file,
            config_file=config_file,
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            scaling_configuration=scaling_configuration,
            transformer=transformer,
            api_protocol=api_protocol,
            environment=environment,
            env_vars=env_vars,
            vllm_variant=vllm_variant,
            vllm_image_tag=vllm_image_tag,
        )

        return predictor.deploy()

    @public
    @usage._method_logger
    def add_tag(self, name: str, value: str | dict):
        """Attach a tag to a model.

        A tag consists of a <name,value> pair. Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        Parameters:
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to add the tag.
        """
        self._model_engine._set_tag(model_instance=self, name=name, value=value)

    @public
    @usage._method_logger
    def set_tag(self, name: str, value: str | dict):
        """Deprecated: Use add_tag instead.

        Parameters:
            name: name of the tag
            value: value of the tag
        """
        warnings.warn(
            "The set_tag method is deprecated. Please use add_tag instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._model_engine._set_tag(model_instance=self, name=name, value=value)

    @public
    @usage._method_logger
    def delete_tag(self, name: str):
        """Delete a tag attached to a model.

        Parameters:
            name: Name of the tag to be removed.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to delete the tag.
        """
        self._model_engine._delete_tag(model_instance=self, name=name)

    @public
    def get_tag(self, name: str) -> str | None:
        """Get the tags of a model.

        Parameters:
            name: Name of the tag to get.

        Returns:
            tag value or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to retrieve the tag.
        """
        return self._model_engine._get_tag(model_instance=self, name=name)

    @public
    def get_tags(self) -> dict[str, tag.Tag]:
        """Retrieves all tags attached to a model.

        Returns:
            Dictionary of tags.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case of a server error.
        """
        return self._model_engine._get_tags(model_instance=self)

    @public
    def get_url(self):
        """Get url to the model in Hopsworks."""
        path = (
            "/p/"
            + str(client._get_instance()._project_id)
            + "/models/"
            + str(self.name)
            + "/"
            + str(self.version)
        )
        return util._get_hostname_replaced_url(sub_path=path)

    @public
    def get_feature_view(
        self, init: bool = True, online: bool = False
    ) -> feature_view.FeatureView | None:
        """Get the parent feature view of this model, based on explicit provenance.

        Only accessible, usable feature view objects are returned. Otherwise an Exception is raised.
        For more details, call the base method - get_feature_view_provenance

        Parameters:
            init: By default this is set to True. If you require a more complex initialization of the feature view for online or batch scenarios, you should set `init` to False to retrieve a non initialized feature view and then call `init_batch_scoring()` or `init_serving()` with the required parameters.
            online: By default this is set to False and the initialization for batch scoring is considered the default scenario. If you set `online` to True, the online scenario is enabled and the `init_serving()` method is called. When inside a deployment, the only available scenario is the online one, thus the parameter is ignored and init_serving is always called (if `init` is set to True). If you want to override this behaviour, you should set `init` to False and proceed with a custom initialization.

        Returns:
            Feature View Object or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to retrieve the feature view.
        """
        fv_prov = self.get_feature_view_provenance()
        fv = explicit_provenance.Links.get_one_accessible_parent(fv_prov)
        if fv is None:
            return None
        if init:
            td_prov = self.get_training_dataset_provenance()
            td = explicit_provenance.Links.get_one_accessible_parent(td_prov)
            is_deployment = "DEPLOYMENT_NAME" in os.environ
            if online or is_deployment:
                _logger.info(
                    "Initializing for batch and online retrieval of feature vectors"
                    + (" - within a deployment" if is_deployment else "")
                )
                fv.init_serving(training_dataset_version=td.version)
            elif online is False:
                _logger.info("Initializing for batch retrieval of feature vectors")
                fv.init_batch_scoring(training_dataset_version=td.version)
        return fv

    @public
    def get_feature_view_provenance(self) -> explicit_provenance.Links:
        """Get the parent feature view of this model, based on explicit provenance.

        This feature view can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature views, only a minimal information is returned.

        Returns:
            Object containing the section of provenance graph requested or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to retrieve the feature view provenance.
        """
        return self._model_engine._get_feature_view_provenance(model_instance=self)

    @public
    def get_training_dataset_provenance(self) -> explicit_provenance.Links:
        """Get the parent training dataset of this model, based on explicit provenance.

        This training dataset can be accessible, deleted or inaccessible.
        For deleted and inaccessible training datasets, only a minimal information is returned.

        Returns:
            Object containing the section of provenance graph requested or `None` if it does not exist.

        Raises:
            hopsworks.client.exceptions.RestAPIError: in case the backend fails to retrieve the training dataset provenance.
        """
        return self._model_engine._get_training_dataset_provenance(model_instance=self)

    @public
    def get_monitoring_configs(self) -> list[FeatureMonitoringConfig]:
        """Get the feature monitoring configurations for this model version.

        Example:
            ```python

            import hopsworks

            project = hopsworks.login()

            mr = project.get_model_registry()
            my_model = mr.get_model("my_model", version=1)

            fm_configs = my_model.get_monitoring_configs()
            ```

        Returns:
            List of `FeatureMonitoringConfig` objects for this model version.

        Raises:
            hopsworks.client.exceptions.RestAPIError: In case the backend encounters an issue.
        """
        try:
            from hsfs.core.feature_monitoring_config_api import (
                FeatureMonitoringConfigApi,
            )
        except ModuleNotFoundError as err:
            from hopsworks_common.client.exceptions import FeatureStoreException

            raise FeatureStoreException(
                "Feature monitoring requires the hsfs library, which is not installed. "
                "Install hsfs before fetching model monitoring configurations."
            ) from err

        return FeatureMonitoringConfigApi.get_by_model(
            model_registry_id=self._model_registry_id,
            model_id=self._id,
        )

    @public
    def create_model_monitoring(
        self,
        name: str,
        description: str | None = None,
        start_date_time: int | str | None = None,
        end_date_time: int | str | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
    ) -> FeatureMonitoringConfig:
        """Create a model monitoring config for this model.

        Resolves this model's parent feature view via provenance and delegates to
        ``feature_view.create_model_monitoring`` with this model's ``name`` and
        ``version`` already filled in. The resulting config targets the FV's
        logging feature group, filters by this model + version, and defaults the
        reference training dataset to the version that was used to train the model.

        Experimental:
            Public API is subject to change, this feature is not suitable for production use-cases.

        Example:
            ```python3
            mr = project.get_model_registry()
            my_model = mr.get_model("my_model", version=1)

            my_model.create_model_monitoring(
                name="psi_drift",
            ).with_detection_window(
                time_offset="1d", window_length="1d",
            ).with_reference_training_dataset(  # defaults to model's TD version
            ).compare_on_distribution(
                feature_name="amount", metric="PSI", threshold=0.2,
            ).save()
            ```

        Parameters:
            name: Name of the feature monitoring configuration.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression scheduling the FM job (UTC, Quartz).

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If this model has no
                parent feature view recorded in its provenance, or if downstream
                FV validation fails (no logging enabled, no recorded TD version, ...).

        Returns:
            A ``FeatureMonitoringConfig`` builder. Call ``with_detection_window``,
            ``with_reference_*``, ``compare_on`` / ``compare_on_distribution``,
            and ``save()`` to register it.
        """
        fv = self.get_feature_view(init=False)
        if fv is None:
            from hopsworks_common.client.exceptions import FeatureStoreException

            raise FeatureStoreException(
                f"Cannot create model monitoring for model '{self.name}' "
                f"v{self.version}: no parent feature view recorded in its provenance."
            )
        return fv.create_model_monitoring(
            name=name,
            model_name=self.name,
            model_version=self.version,
            description=description,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    def _get_default_serving_name(self):
        return re.sub(r"[^a-zA-Z0-9]", "", self._name)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [util._set_model_class(model) for model in json_decamelized["items"]]
        return util._set_model_class(json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:  # backwards compatibility
            _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self):
        return {
            "id": self._name + "_" + str(self._version),
            "projectName": self._project_name,
            "name": self._name,
            "modelSchema": self._model_schema,
            "version": self._version,
            "description": self._description,
            "inputExample": self._input_example,
            "framework": self._framework,
            "metrics": self._training_metrics,
            "environment": self._environment,
            "program": self._program,
            "featureView": util._feature_view_to_json(self._feature_view),
            "trainingDatasetVersion": self._training_dataset_version,
        }

    @public
    @property
    def id(self):
        """Id of the model."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @public
    @property
    def name(self):
        """Name of the model."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @public
    @property
    def version(self):
        """Version of the model."""
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @public
    @property
    def description(self):
        """Description of the model."""
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    @public
    @property
    def created(self):
        """Creation date of the model."""
        return self._created

    @created.setter
    def created(self, created):
        self._created = created

    @public
    @property
    def creator(self):
        """Creator of the model."""
        return self._creator

    @creator.setter
    def creator(self, creator):
        self._creator = creator

    @public
    @property
    def environment(self):
        """Input example of the model."""
        if self._environment is not None:
            return self._model_engine._read_file(
                model_instance=self, resource="environment.yml"
            )
        return self._environment

    @environment.setter
    def environment(self, environment):
        self._environment = environment

    @public
    @property
    def training_metrics(self):
        """Training metrics of the model."""
        return self._training_metrics

    @training_metrics.setter
    def training_metrics(self, training_metrics):
        self._training_metrics = training_metrics

    @public
    @property
    def program(self):
        """Executable used to export the model."""
        if self._program is not None:
            return self._model_engine._read_file(
                model_instance=self, resource=self._program
            )
        return None

    @program.setter
    def program(self, program):
        self._program = program

    @public
    @property
    def user(self):
        """User of the model."""
        return self._user_full_name

    @user.setter
    def user(self, user_full_name):
        self._user_full_name = user_full_name

    @public
    @property
    def input_example(self):
        """input_example of the model."""
        return self._model_engine._read_json(
            model_instance=self, resource="input_example.json"
        )

    @input_example.setter
    def input_example(self, input_example):
        self._input_example = input_example

    @public
    @property
    def framework(self):
        """Framework of the model."""
        return self._framework

    @framework.setter
    def framework(self, framework):
        self._framework = framework

    @public
    @property
    def model_schema(self):
        """Model schema of the model."""
        return self._model_engine._read_json(
            model_instance=self, resource="model_schema.json"
        )

    @model_schema.setter
    def model_schema(self, model_schema):
        self._model_schema = model_schema

    @public
    @property
    def project_name(self):
        """project_name of the model."""
        return self._project_name

    @project_name.setter
    def project_name(self, project_name):
        self._project_name = project_name

    @public
    @property
    def model_registry_id(self):
        """model_registry_id of the model."""
        return self._model_registry_id

    @model_registry_id.setter
    def model_registry_id(self, model_registry_id):
        self._model_registry_id = model_registry_id

    @public
    @property
    def model_path(self):
        """Path of the model with version folder omitted.

        Resolves to `/Projects/{project_name}/Models/{name}`.
        """
        return f"/Projects/{self.project_name}/Models/{self.name}"

    @public
    @property
    def version_path(self):
        """Path of the model including version folder.

        Resolves to `/Projects/{project_name}/Models/{name}/{version}`.
        """
        return f"{self.model_path}/{str(self.version)}"

    @public
    @property
    def model_files_path(self):
        """Path of the model files including version and files folder.

        Resolves to `/Projects/{project_name}/Models/{name}/{version}/Files`.
        """
        return f"{self.version_path}/{MODEL_REGISTRY.MODEL_FILES_DIR_NAME}"

    @public
    @property
    def shared_registry_project_name(self):
        """shared_registry_project_name of the model."""
        return self._shared_registry_project_name

    @shared_registry_project_name.setter
    def shared_registry_project_name(self, shared_registry_project_name):
        self._shared_registry_project_name = shared_registry_project_name

    @public
    @property
    def training_dataset_version(self) -> int:
        return self._training_dataset_version

    def __repr__(self):
        return f"Model(name: {self._name!r}, version: {self._version!r})"
