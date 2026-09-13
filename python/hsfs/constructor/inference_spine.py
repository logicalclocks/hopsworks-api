#
#   Copyright 2026 Hopsworks AB
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
from __future__ import annotations

import logging
import os
import re
import uuid
import warnings
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.constants import HAS_POLARS
from hsfs.constructor.prediction_times import PredictionTimes


if TYPE_CHECKING:
    import pandas as pd
    from hsfs.feature_view import FeatureView


_logger = logging.getLogger(__name__)

ROW_ID_COLUMN = "__hopsworks_spine_row_id"
SPINE_DIR = "Resources/.hopsworks_spine"
_TABLE_PREFIX = "__hopsworks_spine_"

_DECIMAL = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.IGNORECASE)


def _pyarrow_type(hive_type: str) -> Any:
    import pyarrow as pa

    t = (hive_type or "string").strip().lower()
    simple = {
        "string": pa.string(),
        "boolean": pa.bool_(),
        "tinyint": pa.int8(),
        "smallint": pa.int16(),
        "int": pa.int32(),
        "integer": pa.int32(),
        "bigint": pa.int64(),
        "float": pa.float32(),
        "double": pa.float64(),
        "timestamp": pa.timestamp("ms"),
        "date": pa.date32(),
        "binary": pa.binary(),
    }
    if t in simple:
        return simple[t]
    match = _DECIMAL.match(t)
    if match:
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    return pa.string()


class InferenceSpine:
    """The rows a batch-inference read is anchored on, in place of the root feature group.

    Built from an `entries` frame of serving keys and passed features crossed with a set of
    prediction times. Validates both against the feature view before anything is sent.
    """

    def __init__(
        self,
        feature_view: FeatureView,
        entries: Any,
        prediction_times: PredictionTimes | list[Any] | None,
        max_feature_age: timedelta | dict[str, timedelta] | None = None,
    ) -> None:
        self._feature_view = feature_view
        self._table_name = _TABLE_PREFIX + uuid.uuid4().hex[:8]
        self._basename = f"{uuid.uuid4().hex}.parquet"
        self._max_feature_age = _normalize_age(max_feature_age)
        # Only the Hopsworks Query Service reads the spine from a file. Spark takes a session
        # temporary view, so nothing is staged and the backend must not be told to look for one.
        self._parquet_staged = False

        root_fg = feature_view.query._left_feature_group
        self._event_time = root_fg.event_time
        if not self._event_time:
            raise FeatureStoreException(
                f"Feature group `{root_fg.name}` anchors this feature view but has no event time,"
                " so there is no column to bind the prediction time to."
            )

        frame = _to_pandas(entries)
        if frame is None or len(frame) == 0:
            raise FeatureStoreException(
                "`entries` must carry at least one row: batch data was requested for no entities."
            )

        self._types = _column_types(feature_view)
        required_keys = {
            sk.required_serving_key for sk in feature_view.serving_keys if sk.required
        }
        root_features = {f.name for f in root_fg.features}
        recognized = required_keys | root_features | {self._event_time}

        unknown = [
            c for c in frame.columns if c not in recognized or c == ROW_ID_COLUMN
        ]
        if unknown:
            raise FeatureStoreException(
                f"`entries` column(s) {sorted(unknown)} match nothing in feature view"
                f" `{feature_view.name}`. Accepted columns: {sorted(recognized)}."
            )
        if not [c for c in frame.columns if c != self._event_time]:
            raise FeatureStoreException(
                "`entries` carries no serving key and no feature of the root feature group,"
                " so nothing in the feature view can be looked up."
                f" Accepted columns: {sorted(recognized)}."
            )

        has_time_column = self._event_time in frame.columns
        if has_time_column and prediction_times is not None:
            raise FeatureStoreException(
                f"`entries` already carries the prediction time in `{self._event_time}`;"
                " pass either that column or `prediction_times`, not both."
            )
        if not has_time_column and prediction_times is None:
            raise FeatureStoreException(
                "No prediction times: pass `prediction_times`, or carry the prediction time in"
                f" an `{self._event_time}` column of `entries`."
            )

        missing_keys = sorted(required_keys - set(frame.columns))
        if missing_keys:
            warnings.warn(
                f"Serving key(s) {missing_keys} are absent from `entries`. Every feature group"
                " they identify is skipped and its features come back as NULL.",
                stacklevel=3,
            )

        self._dataframe = self._build(frame, prediction_times, has_time_column)

    def _build(
        self, frame: pd.DataFrame, prediction_times, has_time_column
    ) -> pd.DataFrame:
        import pandas as pd

        if has_time_column:
            spine = frame.reset_index(drop=True).copy()
            times = pd.to_datetime(spine[self._event_time], utc=True, errors="coerce")
            if times.isna().any():
                raise FeatureStoreException(
                    f"`entries[{self._event_time!r}]` contains a value that is not a timestamp."
                )
            spine[self._event_time] = times
        else:
            resolved = PredictionTimes._from_user_input(prediction_times).timestamps
            if not resolved:
                raise FeatureStoreException(
                    "`prediction_times` resolved to no timestamps."
                )
            # Cross product in entries order, then ascending prediction time, so row i of the
            # result corresponds to row i here and predictions zip back positionally.
            spine = (
                frame.reset_index(drop=True)
                .loc[frame.reset_index(drop=True).index.repeat(len(resolved))]
                .reset_index(drop=True)
            )
            spine[self._event_time] = pd.to_datetime(
                [t for _ in range(len(frame)) for t in resolved], utc=True
            )

        spine.insert(0, ROW_ID_COLUMN, range(len(spine)))
        self._max_event_time = int(spine[self._event_time].max().timestamp() * 1000)
        return spine

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def basename(self) -> str:
        return self._basename

    @property
    def parquet_staged(self) -> bool:
        """Whether the spine has been written to HopsFS for the query service to read."""
        return self._parquet_staged

    @parquet_staged.setter
    def parquet_staged(self, value: bool) -> None:
        self._parquet_staged = value

    @property
    def dataframe(self) -> pd.DataFrame:
        """The spine rows: the row id, the supplied columns, and the prediction time."""
        return self._dataframe

    @property
    def event_time(self) -> str:
        return self._event_time

    @property
    def row_count(self) -> int:
        return len(self._dataframe)

    @property
    def supplied_columns(self) -> list[str]:
        """The columns the caller supplied, without the internal row id."""
        return [c for c in self._dataframe.columns if c != ROW_ID_COLUMN]

    def arrow_table(self) -> Any:
        """The spine as an Arrow table typed from the feature view's schema.

        Raises:
            FeatureStoreException: If a supplied value does not convert to the feature's type.
        """
        import pyarrow as pa

        fields = [pa.field(ROW_ID_COLUMN, pa.int64())]
        for column in self.supplied_columns:
            hive_type = (
                "timestamp"
                if column == self._event_time
                else self._types.get(column, "string")
            )
            fields.append(pa.field(column, _pyarrow_type(hive_type)))
        try:
            return pa.Table.from_pandas(
                self._dataframe, schema=pa.schema(fields), preserve_index=False
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
            raise FeatureStoreException(
                f"An `entries` value does not convert to the type the feature view declares: {e}"
            ) from e

    def write_parquet(self, directory: str) -> str:
        """Write the spine to a local Parquet file and return its path."""
        import pyarrow.parquet as pq

        path = os.path.join(directory, self._basename)
        pq.write_table(self.arrow_table(), path)
        return path

    def to_dict(self) -> dict[str, Any]:
        """The wire form: the schema and where the rows are, never the rows."""
        columns = [{"name": ROW_ID_COLUMN, "type": "bigint"}]
        for column in self.supplied_columns:
            columns.append(
                {
                    "name": column,
                    "type": "timestamp"
                    if column == self._event_time
                    else self._types.get(column, "string"),
                }
            )
        payload: dict[str, Any] = {
            "tableName": self._table_name,
            "eventTimeColumn": self._event_time,
            "columns": columns,
            "rowCount": self.row_count,
            "maxEventTime": self._max_event_time,
        }
        if self._parquet_staged:
            payload["parquetBasename"] = self._basename
        if self._max_feature_age:
            payload["maxFeatureAgeMs"] = self._max_feature_age
        return payload


def _normalize_age(
    max_feature_age: timedelta | dict[str, timedelta] | None,
) -> dict[str, int]:
    if max_feature_age is None:
        return {}
    if isinstance(max_feature_age, timedelta):
        return {"*": int(max_feature_age.total_seconds() * 1000)}
    if isinstance(max_feature_age, dict):
        out = {}
        for name, age in max_feature_age.items():
            if not isinstance(age, timedelta):
                raise TypeError(
                    f"max_feature_age[{name!r}] must be a timedelta; got {type(age)!r}."
                )
            out[name] = int(age.total_seconds() * 1000)
        return out
    raise TypeError(
        f"max_feature_age must be a timedelta or a dict of them; got {type(max_feature_age)!r}."
    )


def _to_pandas(entries: Any) -> pd.DataFrame | None:
    import pandas as pd

    if entries is None:
        return None
    if isinstance(entries, pd.DataFrame):
        return entries
    if isinstance(entries, list):
        return pd.DataFrame(entries)
    if HAS_POLARS:
        import polars as pl

        if isinstance(entries, pl.DataFrame):
            return entries.to_pandas()
    raise TypeError(
        f"`entries` must be a pandas or polars DataFrame or a list of dicts; got {type(entries)!r}."
    )


def _column_types(feature_view: FeatureView) -> dict[str, str]:
    """Feature name to Hive type across every feature group the view reads.

    Serving keys and passed features are both looked up here, so the map spans the root and
    every joined feature group. Prefixed serving keys resolve under the prefixed name too.
    """
    types: dict[str, str] = {}
    query = feature_view.query
    for fg in [query._left_feature_group] + [
        j.query._left_feature_group for j in query.joins
    ]:
        for feature in fg.features:
            types.setdefault(feature.name, feature.type)
    for sk in feature_view.serving_keys:
        if sk.feature_group is not None:
            for feature in sk.feature_group.features:
                if feature.name == sk.feature_name:
                    types[sk.required_serving_key] = feature.type
    return types
