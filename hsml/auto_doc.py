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

import pathlib
import shutil
import os
import keras_autodoc

JSON_METHODS = [
    "extract_fields_from_json",
    "from_json",
    "from_response_json",
    "json",
    "update_from_response_json",
]

PAGES = {
    # Model registry
    "connection_api.md": {
        "connection": ["hsml.connection.Connection"],
        "connection_properties": keras_autodoc.get_properties(
            "hsml.connection.Connection", exclude=["trust_store_path"]
        ),
        "connection_methods": keras_autodoc.get_methods("hsml.connection.Connection"),
    },
    "model-registry/model_registry_api.md": {
        "mr_get": ["hsml.connection.Connection.get_model_registry"],
        "mr_modules": keras_autodoc.get_properties(
            "hsml.model_registry.ModelRegistry",
            exclude=[
                "project_id",
                "project_name",
                "model_registry_id",
                "shared_registry_project_name",
            ],
        ),
        "mr_properties": keras_autodoc.get_properties(
            "hsml.model_registry.ModelRegistry",
            exclude=[
                "python",
                "sklearn",
                "tensorflow",
                "torch",
            ],
        ),
        "mr_methods": keras_autodoc.get_methods(
            "hsml.model_registry.ModelRegistry", exclude=["from_response_json"]
        ),
    },
    "model-registry/model_api.md": {
        "ml_create_tf": ["hsml.model_registry.ModelRegistry.tensorflow.create_model"],
        "ml_create_th": ["hsml.model_registry.ModelRegistry.torch.create_model"],
        "ml_create_sl": ["hsml.model_registry.ModelRegistry.sklearn.create_model"],
        "ml_create_py": ["hsml.model_registry.ModelRegistry.python.create_model"],
        "ml_get": ["hsml.model_registry.ModelRegistry.get_model"],
        "ml_properties": keras_autodoc.get_properties("hsml.model.Model"),
        "ml_methods": keras_autodoc.get_methods(
            "hsml.model.Model",
            exclude=[
                "from_response_json",
                "json",
                "to_dict",
                "update_from_response_json",
            ],
        ),
    },
    "model-registry/model_schema.md": {},
    "model-registry/model_schema_api.md": {
        "schema": ["hsml.schema.Schema"],
        "schema_dict": ["hsml.schema.Schema.to_dict"],
        "model_schema": ["hsml.model_schema.ModelSchema"],
        "model_schema_dict": ["hsml.model_schema.ModelSchema.to_dict"],
    },
    "model-registry/links.md": {
        "links_properties": keras_autodoc.get_properties(
            "hsml.core.explicit_provenance.Links"
        ),
        "artifact_properties": keras_autodoc.get_properties(
            "hsml.core.explicit_provenance.Artifact"
        ),
    },
    # Model Serving
    "model-serving/model_serving_api.md": {
        "ms_get": ["hsml.connection.Connection.get_model_serving"],
        "ms_properties": keras_autodoc.get_properties(
            "hsml.model_serving.ModelServing"
        ),
        "ms_methods": keras_autodoc.get_methods(
            "hsml.model_serving.ModelServing", exclude=["from_response_json"]
        ),
    },
    "model-serving/deployment_api.md": {
        "ms_get_model_serving": ["hsml.connection.Connection.get_model_serving"],
        "ms_get_deployments": [
            "hsml.model_serving.ModelServing.get_deployment",
            "hsml.model_serving.ModelServing.get_deployment_by_id",
            "hsml.model_serving.ModelServing.get_deployments",
        ],
        "ms_create_deployment": ["hsml.model_serving.ModelServing.create_deployment"],
        "m_deploy": ["hsml.model.Model.deploy"],
        "p_deploy": ["hsml.predictor.Predictor.deploy"],
        "dep_properties": keras_autodoc.get_properties("hsml.deployment.Deployment"),
        "dep_methods": keras_autodoc.get_methods(
            "hsml.deployment.Deployment", exclude=JSON_METHODS + ["from_predictor"]
        ),
    },
    "model-serving/predictor_api.md": {
        "ms_get_model_serving": ["hsml.connection.Connection.get_model_serving"],
        "ms_create_predictor": ["hsml.model_serving.ModelServing.create_predictor"],
        "pred_properties": keras_autodoc.get_properties("hsml.predictor.Predictor"),
        "pred_methods": keras_autodoc.get_methods(
            "hsml.predictor.Predictor",
            exclude=JSON_METHODS + ["for_model"],
        ),
    },
    "model-serving/transformer_api.md": {
        "ms_get_model_serving": ["hsml.connection.Connection.get_model_serving"],
        "ms_create_transformer": ["hsml.model_serving.ModelServing.create_transformer"],
        "trans_properties": keras_autodoc.get_properties(
            "hsml.transformer.Transformer"
        ),
        "trans_methods": keras_autodoc.get_methods(
            "hsml.transformer.Transformer", exclude=JSON_METHODS
        ),
    },
    "model-serving/inference_logger_api.md": {
        "il": ["hsml.inference_logger.InferenceLogger"],
        "il_properties": keras_autodoc.get_properties(
            "hsml.inference_logger.InferenceLogger"
        ),
        "il_methods": keras_autodoc.get_methods(
            "hsml.inference_logger.InferenceLogger", exclude=JSON_METHODS
        ),
    },
    "model-serving/inference_batcher_api.md": {
        "ib": ["hsml.inference_batcher.InferenceBatcher"],
        "ib_properties": keras_autodoc.get_properties(
            "hsml.inference_batcher.InferenceBatcher"
        ),
        "ib_methods": keras_autodoc.get_methods(
            "hsml.inference_batcher.InferenceBatcher", exclude=JSON_METHODS
        ),
    },
    "model-serving/resources_api.md": {
        "res": ["hsml.resources.Resources"],
        "res_properties": keras_autodoc.get_properties("hsml.resources.Resources"),
        "res_methods": keras_autodoc.get_methods(
            "hsml.resources.Resources", exclude=JSON_METHODS
        ),
    },
    "model-serving/predictor_state_api.md": {
        "ps_get": ["hsml.deployment.Deployment.get_state"],
        "ps_properties": keras_autodoc.get_properties(
            "hsml.predictor_state.PredictorState"
        ),
        "ps_methods": keras_autodoc.get_methods(
            "hsml.predictor_state.PredictorState", exclude=JSON_METHODS
        ),
    },
    "model-serving/predictor_state_condition_api.md": {
        "psc_get": ["hsml.predictor_state.PredictorState.condition"],
        "psc_properties": keras_autodoc.get_properties(
            "hsml.predictor_state_condition.PredictorStateCondition"
        ),
        "psc_methods": keras_autodoc.get_methods(
            "hsml.predictor_state_condition.PredictorStateCondition",
            exclude=JSON_METHODS,
        ),
    },
}

hsml_dir = pathlib.Path(__file__).resolve().parents[0]
if "GITHUB_SHA" in os.environ:
    commit_sha = os.environ["GITHUB_SHA"]
    project_url = f"https://github.com/logicalclocks/machine-learning-api/tree/{commit_sha}/python"
else:
    branch_name = os.environ.get("GITHUB_BASE_REF", "master")
    project_url = f"https://github.com/logicalclocks/machine-learning-api/blob/{branch_name}/python"


def generate(dest_dir):
    doc_generator = keras_autodoc.DocumentationGenerator(
        PAGES,
        project_url=project_url,
        template_dir="./docs/templates",
        titles_size="###",
        extra_aliases={},
        max_signature_line_length=100,
    )
    shutil.copyfile(hsml_dir / "CONTRIBUTING.md", dest_dir / "CONTRIBUTING.md")
    shutil.copyfile(hsml_dir / "README.md", dest_dir / "index.md")

    doc_generator.generate(dest_dir / "generated")


if __name__ == "__main__":
    generate(hsml_dir / "docs")
