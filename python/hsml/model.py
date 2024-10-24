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

import json
import logging
import os
import warnings
from typing import Any, Dict, Optional, Union

import humps
from hopsworks_common import usage
from hsml import client, constants, util
from hsml.constants import ARTIFACT_VERSION
from hsml.constants import INFERENCE_ENDPOINTS as IE
from hsml.core import explicit_provenance
from hsml.engine import model_engine
from hsml.inference_batcher import InferenceBatcher
from hsml.inference_logger import InferenceLogger
from hsml.predictor import Predictor
from hsml.resources import PredictorResources
from hsml.transformer import Transformer


_logger = logging.getLogger(__name__)


class Model:
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
        training_dataset=None,
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
        self._training_dataset = training_dataset

        # This is needed for update_from_response_json function to not overwrite name of the shared registry this model originates from
        if not hasattr(self, "_shared_registry_project_name"):
            self._shared_registry_project_name = None

        self._model_registry_id = model_registry_id

        self._model_engine = model_engine.ModelEngine()
        self._feature_view = feature_view
        self._training_dataset_version = training_dataset_version
        if training_dataset_version is None and feature_view is not None:
            if feature_view.get_last_accessed_training_dataset() is not None:
                self._training_dataset_version = (
                    feature_view.get_last_accessed_training_dataset()
                )
            else:
                warnings.warn(
                    "Provenance cached data - feature view provided, but training dataset version is missing",
                    util.ProvenanceWarning,
                    stacklevel=1,
                )

    @usage.method_logger
    def save(
        self,
        model_path,
        await_registration=480,
        keep_original_files=False,
        upload_configuration: Optional[Dict[str, Any]] = None,
    ):
        """Persist this model including model files and metadata to the model registry.

        # Arguments
            model_path: Local or remote (Hopsworks file system) path to the folder where the model files are located, or path to a specific model file.
            await_registration: Awaiting time for the model to be registered in Hopsworks.
            keep_original_files: If the model files are located in hopsfs, whether to move or copy those files into the Models dataset. Default is False (i.e., model files will be moved)
            upload_configuration: When saving a model from outside Hopsworks, the model is uploaded to the model registry using the REST APIs. Each model artifact is divided into
                chunks and each chunk uploaded independently. This parameter can be used to control the upload chunk size, the parallelism and the number of retries.
                `upload_configuration` can contain the following keys:
                * key `chunk_size`: size of each chunk in megabytes. Default 10.
                * key `simultaneous_uploads`: number of chunks to upload in parallel. Default 3.
                * key `max_chunk_retries`: number of times to retry the upload of a chunk in case of failure. Default 1.

        # Returns
            `Model`: The model metadata object.
        """
        return self._model_engine.save(
            model_instance=self,
            model_path=model_path,
            await_registration=await_registration,
            keep_original_files=keep_original_files,
            upload_configuration=upload_configuration,
        )

    @usage.method_logger
    def download(self, local_path=None):
        """Download the model files.

        # Arguments
            local_path: path where to download the model files in the local filesystem
        # Returns
            `str`: Absolute path to local folder containing the model files.
        """
        return self._model_engine.download(model_instance=self, local_path=local_path)

    @usage.method_logger
    def delete(self):
        """Delete the model

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            model **and** deletes the model files.

        # Raises
            `RestAPIError`.
        """
        self._model_engine.delete(model_instance=self)

    @usage.method_logger
    def deploy(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        artifact_version: Optional[str] = ARTIFACT_VERSION.CREATE,
        serving_tool: Optional[str] = None,
        script_file: Optional[str] = None,
        resources: Optional[Union[PredictorResources, dict]] = None,
        inference_logger: Optional[Union[InferenceLogger, dict]] = None,
        inference_batcher: Optional[Union[InferenceBatcher, dict]] = None,
        transformer: Optional[Union[Transformer, dict]] = None,
        api_protocol: Optional[str] = IE.API_PROTOCOL_REST,
        environment: Optional[str] = None,
    ):
        """Deploy the model.

        !!! example
            ```python

            import hopsworks

            project = hopsworks.login()

            # get Hopsworks Model Registry handle
            mr = project.get_model_registry()

            # retrieve the trained model you want to deploy
            my_model = mr.get_model("my_model", version=1)

            my_deployment = my_model.deploy()
            ```
        # Arguments
            name: Name of the deployment.
            description: Description of the deployment.
            artifact_version: Version number of the model artifact to deploy, `CREATE` to create a new model artifact
            or `MODEL-ONLY` to reuse the shared artifact containing only the model files.
            serving_tool: Serving tool used to deploy the model server.
            script_file: Path to a custom predictor script implementing the Predict class.
            resources: Resources to be allocated for the predictor.
            inference_logger: Inference logger configuration.
            inference_batcher: Inference batcher configuration.
            transformer: Transformer to be deployed together with the predictor.
            api_protocol: API protocol to be enabled in the deployment (i.e., 'REST' or 'GRPC'). Defaults to 'REST'.
            environment: The inference environment to use.

        # Returns
            `Deployment`: The deployment metadata object of a new or existing deployment.
        """

        if name is None:
            name = self._name

        predictor = Predictor.for_model(
            self,
            name=name,
            description=description,
            artifact_version=artifact_version,
            serving_tool=serving_tool,
            script_file=script_file,
            resources=resources,
            inference_logger=inference_logger,
            inference_batcher=inference_batcher,
            transformer=transformer,
            api_protocol=api_protocol,
            environment=environment,
        )

        return predictor.deploy()

    @usage.method_logger
    def set_tag(self, name: str, value: Union[str, dict]):
        """Attach a tag to a model.

        A tag consists of a <name,value> pair. Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.
        # Raises
            `RestAPIError` in case the backend fails to add the tag.
        """

        self._model_engine.set_tag(model_instance=self, name=name, value=value)

    @usage.method_logger
    def delete_tag(self, name: str):
        """Delete a tag attached to a model.

        # Arguments
            name: Name of the tag to be removed.
        # Raises
            `RestAPIError` in case the backend fails to delete the tag.
        """
        self._model_engine.delete_tag(model_instance=self, name=name)

    def get_tag(self, name: str):
        """Get the tags of a model.

        # Arguments
            name: Name of the tag to get.
        # Returns
            tag value
        # Raises
            `RestAPIError` in case the backend fails to retrieve the tag.
        """
        return self._model_engine.get_tag(model_instance=self, name=name)

    def get_tags(self):
        """Retrieves all tags attached to a model.

        # Returns
            `Dict[str, obj]` of tags.
        # Raises
            `RestAPIError` in case the backend fails to retrieve the tags.
        """
        return self._model_engine.get_tags(model_instance=self)

    def get_url(self):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/models/"
            + str(self.name)
            + "/"
            + str(self.version)
        )
        return util.get_hostname_replaced_url(sub_path=path)

    def get_feature_view(self, init: bool = True, online: Optional[bool] = None):
        """Get the parent feature view of this model, based on explicit provenance.
         Only accessible, usable feature view objects are returned. Otherwise an Exception is raised.
         For more details, call the base method - get_feature_view_provenance

        # Returns
            `FeatureView`: Feature View Object.
        # Raises
            `Exception` in case the backend fails to retrieve the tags.
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

    def get_feature_view_provenance(self):
        """Get the parent feature view of this model, based on explicit provenance.
        This feature view can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature views, only a minimal information is
        returned.

        # Returns
            `ProvenanceLinks`: Object containing the section of provenance graph requested.
        """
        return self._model_engine.get_feature_view_provenance(model_instance=self)

    def get_training_dataset_provenance(self):
        """Get the parent training dataset of this model, based on explicit provenance.
        This training dataset can be accessible, deleted or inaccessible.
        For deleted and inaccessible training datasets, only a minimal information is
        returned.

        # Returns
            `ProvenanceLinks`: Object containing the section of provenance graph requested.
        """
        return self._model_engine.get_training_dataset_provenance(model_instance=self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [util.set_model_class(model) for model in json_decamelized["items"]]
        else:
            return util.set_model_class(json_decamelized)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "type" in json_decamelized:  # backwards compatibility
            _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

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
            "trainingDataset": self._training_dataset,
            "environment": self._environment,
            "program": self._program,
            "featureView": util.feature_view_to_json(self._feature_view),
            "trainingDatasetVersion": self._training_dataset_version,
        }

    @property
    def id(self):
        """Id of the model."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        """Name of the model."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def version(self):
        """Version of the model."""
        return self._version

    @version.setter
    def version(self, version):
        self._version = version

    @property
    def description(self):
        """Description of the model."""
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    @property
    def created(self):
        """Creation date of the model."""
        return self._created

    @created.setter
    def created(self, created):
        self._created = created

    @property
    def creator(self):
        """Creator of the model."""
        return self._creator

    @creator.setter
    def creator(self, creator):
        self._creator = creator

    @property
    def environment(self):
        """Input example of the model."""
        if self._environment is not None:
            return self._model_engine.read_file(
                model_instance=self, resource="environment.yml"
            )
        return self._environment

    @environment.setter
    def environment(self, environment):
        self._environment = environment

    @property
    def training_metrics(self):
        """Training metrics of the model."""
        return self._training_metrics

    @training_metrics.setter
    def training_metrics(self, training_metrics):
        self._training_metrics = training_metrics

    @property
    def program(self):
        """Executable used to export the model."""
        if self._program is not None:
            return self._model_engine.read_file(
                model_instance=self, resource=self._program
            )

    @program.setter
    def program(self, program):
        self._program = program

    @property
    def user(self):
        """user of the model."""
        return self._user_full_name

    @user.setter
    def user(self, user_full_name):
        self._user_full_name = user_full_name

    @property
    def input_example(self):
        """input_example of the model."""
        return self._model_engine.read_json(
            model_instance=self, resource="input_example.json"
        )

    @input_example.setter
    def input_example(self, input_example):
        self._input_example = input_example

    @property
    def framework(self):
        """framework of the model."""
        return self._framework

    @framework.setter
    def framework(self, framework):
        self._framework = framework

    @property
    def model_schema(self):
        """model schema of the model."""
        return self._model_engine.read_json(
            model_instance=self, resource="model_schema.json"
        )

    @model_schema.setter
    def model_schema(self, model_schema):
        self._model_schema = model_schema

    @property
    def training_dataset(self):
        """training_dataset of the model."""
        return self._training_dataset

    @training_dataset.setter
    def training_dataset(self, training_dataset):
        self._training_dataset = training_dataset

    @property
    def project_name(self):
        """project_name of the model."""
        return self._project_name

    @project_name.setter
    def project_name(self, project_name):
        self._project_name = project_name

    @property
    def model_registry_id(self):
        """model_registry_id of the model."""
        return self._model_registry_id

    @model_registry_id.setter
    def model_registry_id(self, model_registry_id):
        self._model_registry_id = model_registry_id

    @property
    def model_path(self):
        """path of the model with version folder omitted. Resolves to /Projects/{project_name}/Models/{name}"""
        return "/Projects/{}/Models/{}".format(self.project_name, self.name)

    @property
    def version_path(self):
        """path of the model including version folder. Resolves to /Projects/{project_name}/Models/{name}/{version}"""
        return "{}/{}".format(self.model_path, str(self.version))

    @property
    def model_files_path(self):
        """path of the model files including version and files folder. Resolves to /Projects/{project_name}/Models/{name}/{version}/Files"""
        return "{}/{}".format(
            self.version_path,
            constants.MODEL_REGISTRY.MODEL_FILES_DIR_NAME,
        )

    @property
    def shared_registry_project_name(self):
        """shared_registry_project_name of the model."""
        return self._shared_registry_project_name

    @shared_registry_project_name.setter
    def shared_registry_project_name(self, shared_registry_project_name):
        self._shared_registry_project_name = shared_registry_project_name

    @property
    def training_dataset_version(self) -> int:
        return self._training_dataset_version

    def __repr__(self):
        return f"Model(name: {self._name!r}, version: {self._version!r})"
