import math

import pandas as pd
from hsfs import engine as hopsworks_engine
from hsfs.builtin_transformations import winsorize
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.engine import python as python_engine
from python.hsfs import transformation_function
from python.hsfs.transformation_function import TransformationType


def _set_percentiles(pcts):
    winsorize.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="feature", percentiles=pcts)
    ]
    winsorize.transformation_context = None  # reset each test


def _run_raw(series: pd.Series) -> pd.Series:
    """Call the raw UDF function directly so input stays a Series."""
    fn = winsorize.get_udf(online=True)

    return fn(series)


def test_winsorize_default_thresholds():
    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")

    percentiles = list(range(100))  # [0..99]
    _set_percentiles(percentiles)

    s = pd.Series([0.0, 1.0, 50.0, 99.0, 120.0, math.nan])

    tf = transformation_function.TransformationFunction(
        hopsworks_udf=winsorize("col_0"),
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )

    percentiles = [float(i) for i in range(100)]
    tf.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="col_0", percentiles=percentiles)
    ]

    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")

    # Act
    out = engine._apply_transformation_function([tf], s)

    out = _run_raw(s)

    expected = pd.Series([1.0, 1.0, 50.0, 99.0, 99.0, math.nan])

    for got, want in zip(out.tolist(), expected.tolist()):
        if math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want


def test_winsorize_context_override():
    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")

    percentiles = list(range(100))
    _set_percentiles(percentiles)

    s = pd.Series([2.0, 5.0, 95.0, 96.0, math.nan])

    winsorize.transformation_context = {"p_low": 5, "p_high": 95}

    out = _run_raw(s)

    expected = pd.Series([5.0, 5.0, 95.0, 95.0, math.nan])

    for got, want in zip(out.tolist(), expected.tolist()):
        if math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want
