import math
import pandas as pd

from hsfs.builtin_transformations import winsorize
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


def _set_percentiles(pcts):
    # Attach transformation statistics to the winsorize UDF so the injected `statistics` is available in scope
    winsorize.transformation_statistics = [
        FeatureDescriptiveStatistics(feature_name="feature", percentiles=pcts)
    ]


def test_winsorize_default_thresholds():
    # Build a simple monotonic percentile array so index == value
    percentiles = list(range(100))  # 0..99
    _set_percentiles(percentiles)

    # Input data within and outside bounds
    s = pd.Series([0.0, 1.0, 50.0, 99.0, 120.0, math.nan])

    # Get callable for Python execution
    fn = winsorize.get_udf(online=True)
    out = fn(s)

    # Default thresholds: [1st, 99th] => [1.0, 99.0]
    expected = pd.Series([1.0, 1.0, 50.0, 99.0, 99.0, math.nan])

    # Compare element-wise allowing NaNs
    for got, want in zip(out.tolist(), expected.tolist()):
        if math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want


def test_winsorize_context_override():
    percentiles = list(range(100))
    _set_percentiles(percentiles)

    s = pd.Series([2.0, 5.0, 95.0, 96.0, math.nan])

    # Override to [5th, 95th]
    winsorize.transformation_context = {"p_low": 5, "p_high": 95}

    fn = winsorize.get_udf(online=True)
    out = fn(s)

    expected = pd.Series([5.0, 5.0, 95.0, 95.0, math.nan])

    for got, want in zip(out.tolist(), expected.tolist()):
        if math.isnan(want):
            assert math.isnan(got)
        else:
            assert got == want
