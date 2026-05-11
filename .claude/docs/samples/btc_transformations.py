"""Retrieval-time transformations for the BTC feature view.

Two user-defined transformations applied at FV read time (training data
and online inference both honour these so the training/serving skew is
controlled in one place):

- `log1p_views` flattens the very long tail on `wiki_views` (raw range
  observed: 0 to 1881, stddev ~ mean*4) so a linear model does not
  drown the smaller everyday values.
- `standardize_pressure` centres and scales `social_pressure` using
  feature-store-side mean / stddev statistics so the model trains and
  serves on the same scale without the caller having to recompute it.

Both functions receive a pandas Series and must return a Series of the
same length; scalar control flow on the input would coerce it to bool
and fail with `truth value of a Series is ambiguous`.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from hopsworks import udf
from hsfs.transformation_statistics import TransformationStatistics


pressure_stats = TransformationStatistics("social_pressure")


@udf(return_type=float, drop=["wiki_views"])
def log1p_views(wiki_views: pd.Series) -> pd.Series:
    return np.log1p(wiki_views.clip(lower=0).fillna(0.0))


@udf(return_type=float, drop=["social_pressure"])
def standardize_pressure(
    social_pressure: pd.Series, statistics=pressure_stats
) -> pd.Series:
    stddev = statistics.social_pressure.stddev or 1.0
    return (social_pressure - statistics.social_pressure.mean) / stddev
