#
#   Copyright 2025 Hopsworks AB
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

import math

import pandas as pd
from hsfs import engine as hopsworks_engine
from hsfs import transformation_function
from hsfs.core.transformation_function_engine import TransformationFunctionEngine
from hsfs.engine import python as python_engine
from hsfs.transformation_function import TransformationType


def test_log_transform_python_engine():
    # Arrange
    df = pd.DataFrame(
        {
            "col_0": [1.0, 2.0, 0.0, -5.0, None],
            "other": ["a", "b", "c", "d", "e"],
        }
    )

    from hsfs.builtin_transformations import log_transform

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=log_transform("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")

    # Act
    result = TransformationFunctionEngine.apply_transformation_functions(
        transformation_functions=[tf], data=df
    )

    # Assert
    assert list(result.columns) == [
        "other",
        "log_transform_col_0_",
    ]
    expected = pd.Series([0.0, math.log(2.0), math.nan, math.nan, math.nan])
    expected.name = "log_transform_col_0_"
    pd.testing.assert_series_equal(
        result["log_transform_col_0_"], expected, check_names=True, check_dtype=False
    )
