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

from typing import Optional, Union

import numpy
import pandas
from hopsworks_common import usage
from hsml.llm.model import Model
from hsml.model_schema import ModelSchema


_mr = None


@usage.method_logger
def create_model(
    name: str,
    version: Optional[int] = None,
    metrics: Optional[dict] = None,
    description: Optional[str] = None,
    input_example: Optional[
        Union[pandas.DataFrame, pandas.Series, numpy.ndarray, list]
    ] = None,
    model_schema: Optional[ModelSchema] = None,
    feature_view=None,
    training_dataset_version: Optional[int] = None,
):
    """Create an LLM model metadata object.

    !!! note "Lazy"
        This method is lazy and does not persist any metadata or uploads model artifacts in the
        model registry on its own. To save the model object and the model artifacts, call the `save()` method with a
        local file path to the directory containing the model artifacts.

    # Arguments
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

    # Returns
        `Model`. The model metadata object.
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
    )
    model._shared_registry_project_name = _mr.shared_registry_project_name
    model._model_registry_id = _mr.model_registry_id

    return model
