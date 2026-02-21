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
import pytest
from hsfs import engine as hopsworks_engine
from hsfs import transformation_function
from hsfs.builtin_transformations import (
    impute_category,
    impute_constant,
    impute_mean,
    impute_median,
    impute_mode,
)
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.transformation_function_engine import TransformationFunctionEngine
from hsfs.engine import python as python_engine
from hsfs.transformation_function import TransformationType


def _make_tf(udf_fn, stats: FeatureDescriptiveStatistics):
    tf = transformation_function.TransformationFunction(
        hopsworks_udf=udf_fn,
        featurestore_id=1,
        transformation_type=TransformationType.MODEL_DEPENDENT,
    )
    tf.transformation_statistics = [stats]
    return tf


def _apply(tf, df, context=None):
    engine = python_engine.Engine()
    hopsworks_engine.set_instance(engine=engine, engine_type="python")
    return TransformationFunctionEngine.apply_transformation_functions(
        transformation_functions=[tf],
        data=df,
        transformation_context=context,
    )


# ── impute_mean ───────────────────────────────────────────────────────────────


def test_impute_mean_fills_nan():
    df = pd.DataFrame({"col": [1.0, None, 3.0, None], "other": list("abcd")})
    stats = FeatureDescriptiveStatistics(feature_name="col", mean=2.0)
    result = _apply(_make_tf(impute_mean("col"), stats), df)

    assert list(result.columns) == ["other", "impute_mean_col_"]
    assert result["impute_mean_col_"].tolist() == pytest.approx([1.0, 2.0, 3.0, 2.0])


def test_impute_mean_leaves_nan_when_no_mean():
    """If training mean is NaN (no non-null data), NaNs stay NaN."""
    df = pd.DataFrame({"col": [1.0, None, 3.0], "other": list("abc")})
    stats = FeatureDescriptiveStatistics(feature_name="col", mean=None)
    result = _apply(_make_tf(impute_mean("col"), stats), df)

    assert math.isnan(result["impute_mean_col_"].iloc[1])


def test_impute_mean_non_nan_unchanged():
    df = pd.DataFrame({"col": [10.0, 20.0, 30.0]})
    stats = FeatureDescriptiveStatistics(feature_name="col", mean=99.0)
    result = _apply(_make_tf(impute_mean("col"), stats), df)

    assert result["impute_mean_col_"].tolist() == pytest.approx([10.0, 20.0, 30.0])


# ── impute_median ─────────────────────────────────────────────────────────────


def test_impute_median_fills_nan():
    # percentiles[49] = 5.0 (median)
    percentiles = [float(i) for i in range(101)]
    df = pd.DataFrame({"col": [1.0, None, 10.0, None], "other": list("abcd")})
    stats = FeatureDescriptiveStatistics(feature_name="col", percentiles=percentiles)
    result = _apply(_make_tf(impute_median("col"), stats), df)

    assert list(result.columns) == ["other", "impute_median_col_"]
    assert result["impute_median_col_"].iloc[1] == pytest.approx(49.0)
    assert result["impute_median_col_"].iloc[3] == pytest.approx(49.0)


def test_impute_median_leaves_nan_when_no_percentiles():
    df = pd.DataFrame({"col": [1.0, None, 3.0]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(_make_tf(impute_median("col"), stats), df)

    assert math.isnan(result["impute_median_col_"].iloc[1])


def test_impute_median_non_nan_unchanged():
    percentiles = [float(i) for i in range(101)]
    df = pd.DataFrame({"col": [5.0, 15.0, 25.0]})
    stats = FeatureDescriptiveStatistics(feature_name="col", percentiles=percentiles)
    result = _apply(_make_tf(impute_median("col"), stats), df)

    assert result["impute_median_col_"].tolist() == pytest.approx([5.0, 15.0, 25.0])


# ── impute_constant ───────────────────────────────────────────────────────────


def test_impute_constant_default_fills_zero():
    df = pd.DataFrame({"col": [1.0, None, 3.0, None]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(_make_tf(impute_constant("col"), stats), df)

    assert list(result.columns) == ["impute_constant_col_"]
    assert result["impute_constant_col_"].tolist() == pytest.approx([1.0, 0.0, 3.0, 0.0])


def test_impute_constant_custom_value():
    df = pd.DataFrame({"col": [1.0, None, None, 4.0], "other": list("abcd")})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(_make_tf(impute_constant("col"), stats), df, context={"value": -999.0})

    assert result["impute_constant_col_"].tolist() == pytest.approx(
        [1.0, -999.0, -999.0, 4.0]
    )


def test_impute_constant_non_nan_unchanged():
    df = pd.DataFrame({"col": [7.0, 8.0, 9.0]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(_make_tf(impute_constant("col"), stats), df, context={"value": 0.0})

    assert result["impute_constant_col_"].tolist() == pytest.approx([7.0, 8.0, 9.0])


# ── impute_mode ───────────────────────────────────────────────────────────────


def test_impute_mode_fills_nan_with_most_frequent():
    histogram = [
        {"value": "cat", "count": 50},
        {"value": "dog", "count": 30},
        {"value": "bird", "count": 5},
    ]
    df = pd.DataFrame({"col": ["dog", None, "cat", None], "other": list("abcd")})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    stats._extended_statistics = {"histogram": histogram}
    result = _apply(_make_tf(impute_mode("col"), stats), df)

    assert list(result.columns) == ["other", "impute_mode_col_"]
    assert result["impute_mode_col_"].iloc[1] == "cat"
    assert result["impute_mode_col_"].iloc[3] == "cat"


def test_impute_mode_non_nan_unchanged():
    histogram = [{"value": "cat", "count": 100}, {"value": "dog", "count": 1}]
    df = pd.DataFrame({"col": ["dog", "bird"]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    stats._extended_statistics = {"histogram": histogram}
    result = _apply(_make_tf(impute_mode("col"), stats), df)

    assert result["impute_mode_col_"].tolist() == ["dog", "bird"]


def test_impute_mode_no_histogram_no_crash():
    """No histogram available → NaN stays NaN (no crash)."""
    df = pd.DataFrame({"col": ["a", None, "b"]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    # no histogram set
    result = _apply(_make_tf(impute_mode("col"), stats), df)

    assert result["impute_mode_col_"].iloc[0] == "a"
    assert pd.isna(result["impute_mode_col_"].iloc[1])


# ── impute_category ───────────────────────────────────────────────────────────


def test_impute_category_default_sentinel():
    df = pd.DataFrame({"col": ["US", None, "FR", None], "other": list("abcd")})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(_make_tf(impute_category("col"), stats), df)

    assert list(result.columns) == ["other", "impute_category_col_"]
    assert result["impute_category_col_"].iloc[1] == "__MISSING__"
    assert result["impute_category_col_"].iloc[3] == "__MISSING__"


def test_impute_category_custom_sentinel():
    df = pd.DataFrame({"col": ["US", None, "FR"]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(
        _make_tf(impute_category("col"), stats), df, context={"value": "Unknown"}
    )

    assert result["impute_category_col_"].tolist() == ["US", "Unknown", "FR"]


def test_impute_category_non_nan_unchanged():
    df = pd.DataFrame({"col": ["a", "b", "c"]})
    stats = FeatureDescriptiveStatistics(feature_name="col")
    result = _apply(_make_tf(impute_category("col"), stats), df)

    assert result["impute_category_col_"].tolist() == ["a", "b", "c"]
