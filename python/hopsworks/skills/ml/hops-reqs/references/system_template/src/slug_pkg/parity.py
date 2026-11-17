"""Comparing the batch path with the online path, for tests/integration/test_parity.py.

Null policy: a value null on both sides (None or NaN) is equal, null on one side only is a
mismatch. An infinity is never expected, on either side, and is always a mismatch. A numeric
value matches when the two sides differ by at most the tolerance; any other value when equal.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _row_mismatches(left: pd.Series, right: pd.Series, tolerance: float) -> np.ndarray:
    left_null = left.isna().to_numpy()
    right_null = right.isna().to_numpy()
    if pd.api.types.is_numeric_dtype(left) and pd.api.types.is_numeric_dtype(right):
        a = left.to_numpy(dtype=float)
        b = right.to_numpy(dtype=float)
        with np.errstate(invalid="ignore"):
            far = ~(np.abs(a - b) <= tolerance)
        differs = far | np.isinf(a) | np.isinf(b)
    else:
        differs = (left.astype(str) != right.astype(str)).to_numpy()
    return np.where(left_null & right_null, False, differs | (left_null != right_null))


def feature_problems(
    offline: pd.DataFrame,
    online: pd.DataFrame,
    expected: list[str],
    entities: int,
    tolerance: float,
    max_mismatch: int,
) -> list[str]:
    """What makes the offline and online feature vectors of the same entities disagree.

    `expected` are the feature columns both sides must have, and `entities` the rows each must
    have, in the same entity order.
    """
    found = []
    for side, frame in (("offline", offline), ("online", online)):
        missing = [c for c in expected if c not in frame.columns]
        if missing:
            found.append(f"{side} vectors lack {missing}")
        if len(frame) != entities:
            found.append(f"{side} has {len(frame)} rows for {entities} entities")
    if not expected:
        found.append("no feature columns to compare")
    if found:
        return found
    offline = offline.reset_index(drop=True)
    online = online.reset_index(drop=True)
    rows = np.zeros(entities, dtype=bool)
    for column in expected:
        rows |= _row_mismatches(offline[column], online[column], tolerance)
    if int(rows.sum()) > max_mismatch:
        found.append(
            f"{int(rows.sum())} of {entities} entities have different transformed features "
            "offline and online"
        )
    return found


def prediction_problems(
    local, served, entities: int, tolerance: float, max_mismatch: int
) -> list[str]:
    """What makes the downloaded model's predictions and the deployment's disagree, per entity."""
    local = np.asarray(local, dtype=object)
    served = np.asarray(served, dtype=object)
    if entities == 0 or len(local) != entities or len(served) != entities:
        return [f"{len(local)} local and {len(served)} served predictions for {entities} entities"]
    local = local.reshape(entities, -1)
    served = served.reshape(entities, -1)
    if local.shape != served.shape:
        return [f"local predictions have shape {local.shape}, served {served.shape}"]
    rows = np.zeros(entities, dtype=bool)
    for column in range(local.shape[1]):
        left = pd.Series(local[:, column]).infer_objects()
        right = pd.Series(served[:, column]).infer_objects()
        rows |= _row_mismatches(left, right, tolerance)
    if int(rows.sum()) > max_mismatch:
        return [
            f"{int(rows.sum())} of {entities} predictions differ between the downloaded model "
            "and the deployment"
        ]
    return []
