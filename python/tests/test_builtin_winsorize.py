import math

import pandas as pd
from hsfs import engine as hopsworks_engine
from hsfs import transformation_function
from hsfs.builtin_transformations import winsorize
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.engine import python as python_engine
from hsfs.transformation_function import TransformationType


def test_winsorize_default_thresholds():
    # Arrange
    df = pd.DataFrame(
        {
            "col_0": [0.0, 1.0, 50.0, 99.0, 120.0, math.nan],
            "other": ["a", "b", "c", "d", "e", "f"],
        }
    )

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=winsorize("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    # Percentiles from 0th to 100th (101 values)
    percentiles = [float(i) for i in range(101)]
    tf.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
    ]

    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")

    # Act
    result = engine._apply_transformation_function([tf], df)

    # Assert - defaults clip at 1st and 99th percentiles (values 1 and 99)
    assert list(result.columns) == ["other", "winsorize_col_0_"]
    expected = pd.Series([1.0, 1.0, 50.0, 99.0, 99.0, math.nan])
    expected.name = "winsorize_col_0_"

    for got, want in zip(result["winsorize_col_0_"].tolist(), expected.tolist()):
        if math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want


def test_winsorize_context_override():
    # Arrange
    df = pd.DataFrame(
        {
            "col_0": [2.0, 5.0, 95.0, 96.0, math.nan],
            "other": ["a", "b", "c", "d", "e"],
        }
    )

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=winsorize("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    # Percentiles from 0th to 100th (101 values)
    percentiles = [float(i) for i in range(101)]
    tf.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
    ]

    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")

    # Act - Override percentile thresholds via context parameter
    result = engine._apply_transformation_function(
        [tf], df, transformation_context={"p_low": 5, "p_high": 95}
    )

    # Assert - clips at 5th and 95th percentiles (values 5 and 95)
    assert list(result.columns) == ["other", "winsorize_col_0_"]
    expected = pd.Series([5.0, 5.0, 95.0, 95.0, math.nan])
    expected.name = "winsorize_col_0_"

    for got, want in zip(result["winsorize_col_0_"].tolist(), expected.tolist()):
        if math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want
