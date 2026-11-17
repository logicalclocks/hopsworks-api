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

from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common import usage
from hsml.triton.model import Model


if TYPE_CHECKING:
    import numpy
    import pandas
    from hopsworks_common import tag
    from hsfs.feature_view import FeatureView
    from hsml.model_schema import ModelSchema


_mr = None


@public
@usage._method_logger
def create_model(
    name: str,
    version: int | None = None,
    metrics: dict | None = None,
    description: str | None = None,
    input_example: pandas.DataFrame
    | pandas.Series
    | numpy.ndarray
    | list
    | None = None,
    model_schema: ModelSchema | None = None,
    feature_view: FeatureView | None = None,
    training_dataset_version: int | None = None,
    tags: tag.Tag | dict[str, Any] | list[tag.Tag | dict[str, Any]] | None = None,
) -> Model:
    """Create a Triton Inference Server model metadata object.

    The directory passed to `save()` is one Triton model: its `config.pbtxt`, at least one numeric version folder such as `1/`, and any files the backend needs, for example a conda-pack archive referenced by `EXECUTION_ENV_PATH`.
    Leave `name` out of `config.pbtxt`, because Triton serves the model under the deployment name and fails to load a model whose configured name differs.
    Triton serves the version folders its `version_policy` selects, by default the highest one, independently of the model registry version.

    Note: Lazy
        This method is lazy and does not persist any metadata or upload model artifacts in the model registry on its own.
        To save the model object and the model artifacts, call the `save()` method with a local path to the directory containing the model artifacts.

    Parameters:
        name: Name of the model to create.
        version: Version of the model to create.
            When it is `None`, the model gets the version after the last one in the model registry.
        metrics: Model evaluation metrics, for example accuracy or MAE.
        description: Description of the model.
        input_example: An input example that represents a single input for the model.
        model_schema: A model schema for the model inputs and outputs.
        feature_view: The feature view the model was trained from, as returned by the feature store.
            Without it, the model has no provenance.
        training_dataset_version: The training dataset version the model was trained on.
            When it is `None` and `feature_view` is set, it is the training dataset of the feature view that this code or notebook read last.
        tags: Optionally the tags to attach to the model when it is saved, in the same shapes accepted by feature groups.
            A single [`Tag`][hopsworks.tag.Tag], a `{"name": "owner", "value": "team-a"}` dict, or a list of either, for example `[{"name": "owner", "value": "team-a"}]`.
            The tags ride the create request, so any mandatory model tags missing from them cause the backend to reject the save.

    Returns:
        The model metadata object.
    """
    model = Model(
        id=None,
        name=name,
        version=version,
        description=description,
        metrics=metrics,
        input_example=input_example,
        model_schema=model_schema,
        feature_view=feature_view,
        training_dataset_version=training_dataset_version,
        tags=tags,
    )
    model._shared_registry_project_name = _mr.shared_registry_project_name
    model._model_registry_id = _mr.model_registry_id

    return model
