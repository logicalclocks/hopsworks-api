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

import pandas as pd
import pytest
from hsfs import transformation_function
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.engine import python as python_engine
from hsfs.transformation_function import TransformationType


def test_quantile_transformer():
    """Test quantile transformer maps values to [0, 1] uniform distribution."""
    # Arrange
    df = pd.DataFrame(
        {
            "col_0": [1.0, 5.0, 10.0, 15.0, 20.0],
            "other": ["a", "b", "c", "d", "e"],
        }
    )

    from hsfs.builtin_transformations import quantile_transformer

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=quantile_transformer("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    # Create mock percentiles (0 to 99th percentile) from 1 to 20
    percentiles = [1.0 + (19.0 * i / 99) for i in range(100)]
    tf.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
    ]

    engine = python_engine.Engine()

    # Act
    result = engine._apply_transformation_function([tf], df)

    # Assert
    assert list(result.columns) == ["other", "quantile_transformer_col_0_"]
    # Values should be mapped to approximately [0, 0.25, 0.5, 0.75, 1.0]
    transformed = result["quantile_transformer_col_0_"].tolist()
    assert transformed[0] == pytest.approx(0.0, abs=0.01)
    assert transformed[2] == pytest.approx(0.5, abs=0.05)
    assert transformed[4] == pytest.approx(1.0, abs=0.01)


def test_rank_normalizer():
    """Test rank normalizer replaces values with percentile ranks."""
    # Arrange
    df = pd.DataFrame(
        {
            "col_0": [1.0, 5.0, 10.0, 15.0, 20.0],
            "other": ["a", "b", "c", "d", "e"],
        }
    )

    from hsfs.builtin_transformations import rank_normalizer

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=rank_normalizer("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    # Create mock percentiles (0 to 99th percentile) from 1 to 20
    percentiles = [1.0 + (19.0 * i / 99) for i in range(100)]
    tf.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
    ]

    engine = python_engine.Engine()

    # Act
    result = engine._apply_transformation_function([tf], df)

    # Assert
    assert list(result.columns) == ["other", "rank_normalizer_col_0_"]
    # Values should get ranks based on their position in the percentiles
    transformed = result["rank_normalizer_col_0_"].tolist()
    # All ranks should be in [0, 1]
    assert all(0.0 <= r <= 1.0 for r in transformed)
    # Ranks should be monotonically increasing
    assert transformed == sorted(transformed)


def test_quantile_transformer_handles_nan():
    """Test quantile transformer handles NaN values correctly."""
    # Arrange
    df = pd.DataFrame(
        {
            "col_0": [1.0, None, 10.0],
            "other": ["a", "b", "c"],
        }
    )

    from hsfs.builtin_transformations import quantile_transformer

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=quantile_transformer("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    percentiles = [float(i) for i in range(100)]
    tf.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
    ]

    engine = python_engine.Engine()

    # Act
    result = engine._apply_transformation_function([tf], df)

    # Assert
    assert pd.isna(result["quantile_transformer_col_0_"].iloc[1])
    assert not pd.isna(result["quantile_transformer_col_0_"].iloc[0])
    assert not pd.isna(result["quantile_transformer_col_0_"].iloc[2])
