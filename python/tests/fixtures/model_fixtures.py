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

import numpy as np
import pandas as pd
import pytest
from hsml.llm.model import Model as LLMModel
from hsml.model import Model as BaseModel
from hsml.python.model import Model as PythonModel
from hsml.sklearn.model import Model as SklearnModel
from hsml.tensorflow.model import Model as TensorflowModel
from hsml.torch.model import Model as TorchModel


MODEL_BASE_ID = 0
MODEL_PYTHON_ID = 1
MODEL_SKLEARN_ID = 2
MODEL_TENSORFLOW_ID = 3
MODEL_TORCH_ID = 4
MODEL_LLM_ID = 5

MODEL_BASE_NAME = "basemodel"
MODEL_PYTHON_NAME = "pythonmodel"
MODEL_SKLEARN_NAME = "sklearnmodel"
MODEL_TENSORFLOW_NAME = "tensorflowmodel"
MODEL_TORCH_NAME = "torchmodel"
MODEL_LLM_NAME = "llmmodel"

# models


@pytest.fixture
def model_base():
    return BaseModel(MODEL_BASE_ID, MODEL_BASE_NAME)


@pytest.fixture
def model_python():
    return PythonModel(MODEL_PYTHON_ID, MODEL_PYTHON_NAME)


@pytest.fixture
def model_sklearn():
    return SklearnModel(MODEL_SKLEARN_ID, MODEL_SKLEARN_NAME)


@pytest.fixture
def model_tensorflow():
    return TensorflowModel(MODEL_TENSORFLOW_ID, MODEL_TENSORFLOW_NAME)


@pytest.fixture
def model_torch():
    return TorchModel(MODEL_TORCH_ID, MODEL_TORCH_NAME)

@pytest.fixture
def model_llm():
    return LLMModel(MODEL_LLM_ID, MODEL_LLM_NAME)


# input example


@pytest.fixture
def input_example_numpy():
    return np.array([1, 2, 3, 4])


@pytest.fixture
def input_example_dict():
    return {"instances": [[1, 2, 3, 4]]}


@pytest.fixture
def input_example_dataframe_pandas_dataframe():
    return pd.DataFrame({"a": [1], "b": [2], "c": [3], "d": [4]})


@pytest.fixture
def input_example_dataframe_pandas_dataframe_empty():
    return pd.DataFrame()


@pytest.fixture
def input_example_dataframe_pandas_series():
    return pd.Series([1, 2, 3, 4])


@pytest.fixture
def input_example_dataframe_pandas_series_empty():
    return pd.Series()


@pytest.fixture
def input_example_dataframe_list():
    return [1, 2, 3, 4]


# metrics


@pytest.fixture
def model_metrics():
    return {"accuracy": 0.4, "rmse": 0.6}


@pytest.fixture
def model_metrics_wrong_type():
    return [0.4, 0.6]


@pytest.fixture
def model_metrics_wrong_metric_type():
    return {1: 0.4, 2: 0.6}


@pytest.fixture
def model_metrics_wrong_metric_value():
    return {"accuracy": "non-number", "rmse": 0.4}
