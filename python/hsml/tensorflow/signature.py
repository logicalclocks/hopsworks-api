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

from typing import TYPE_CHECKING, Any

from hopsworks_apigen import public
from hopsworks_common import usage
from hsml.tensorflow.model import Model


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
    """Create a TensorFlow model metadata object.

    Note: Lazy
        This method is lazy and does not persist any metadata or uploads model artifacts in the
        model registry on its own. To save the model object and the model artifacts, call the `save()` method with a
        local file path to the directory containing the model artifacts.

    Parameters:
        name: Name of the model to create.
        version: Optionally version of the model to create, defaults to `None` and
            will create the model with incremented version from the last
            version in the model registry.
        metrics: Optionally a dictionary with model evaluation metrics (e.g., accuracy, MAE)
        description: Optionally a string describing the model, defaults to empty string
            `""`.
        input_example: Optionally an input example that represents a single input for the model, defaults to `None`.
        model_schema: Optionally a model schema for the model inputs and/or outputs.
        feature_view: Optionally a feature view object returned by querying the feature store. If the feature view is not provided, the model will not have access to provenance.
        training_dataset_version: Optionally a training dataset version. If training dataset version is not provided, but the feature view is provided, the training dataset version used will be the last accessed training dataset of the feature view, within the code/notebook that reads the feature view and training dataset and then creates the model.
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
