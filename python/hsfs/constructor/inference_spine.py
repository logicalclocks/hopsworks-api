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
from typing import TYPE_CHECKING, Any

from hopsworks_common import spark_connect_utils
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.constants import HAS_POLARS


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


_PASSTHROUGH_TYPE = {
    "int64": "bigint",
    "int32": "int",
    "int16": "smallint",
    "int8": "tinyint",
    "float64": "double",
    "float32": "float",
    "bool": "boolean",
    "object": "string",
    "string": "string",
    "datetime64[ns]": "timestamp",
    "datetime64[us]": "timestamp",
    "datetime64[ns, UTC]": "timestamp",
}


class InferenceSpine:
    """The rows a batch-inference read is anchored on, in place of the root feature group.

    Built from an `spine_df` frame of serving keys and passed features crossed with a set of
    prediction times. Validates both against the feature view before anything is sent.
    """

    def __init__(
        self,
        feature_view: FeatureView,
        spine_df: Any,
        max_feature_age_secs: int | None = None,
        allow_passthrough: bool = False,
    ) -> None:
        self._feature_view = feature_view
        self._table_name = _TABLE_PREFIX + uuid.uuid4().hex[:8]
        self._basename = f"{uuid.uuid4().hex}.parquet"
        self._max_feature_age_secs = max_feature_age_secs
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

        frame = _to_pandas(spine_df)
        if frame is None or len(frame) == 0:
            raise FeatureStoreException(
                "`spine_df` must carry at least one row: batch data was requested for no entities."
            )

        self._types = _column_types(feature_view)
        required_keys = {
            sk.required_serving_key for sk in feature_view.serving_keys if sk.required
        }
        root_features = {f.name for f in root_fg.features}
        recognized = required_keys | root_features | {self._event_time}

        # Training data is built from a labels frame, so columns the view does not define are
        # carried to the output rather than refused. Inference has no labels, so it stays strict
        # and a mistyped column is still caught before anything runs.
        unknown = [
            c for c in frame.columns if c not in recognized or c == ROW_ID_COLUMN
        ]
        if allow_passthrough:
            self._passthrough = [c for c in unknown if c != ROW_ID_COLUMN]
            unknown = [c for c in unknown if c == ROW_ID_COLUMN]
        else:
            self._passthrough = []
        if unknown:
            raise FeatureStoreException(
                f"`spine_df` column(s) {sorted(unknown)} match nothing in feature view"
                f" `{feature_view.name}`. Accepted columns: {sorted(recognized)}."
            )
        if not [c for c in frame.columns if c != self._event_time]:
            raise FeatureStoreException(
                "`spine_df` carries no serving key and no feature of the root feature group,"
                " so nothing in the feature view can be looked up."
                f" Accepted columns: {sorted(recognized)}."
            )

        if self._event_time not in frame.columns:
            raise FeatureStoreException(
                f"`spine_df` must carry the prediction time in an `{self._event_time}` column,"
                " one per row. `PredictionTimes.cross()` builds that frame from a set of"
                " entities and a schedule."
            )

        missing_keys = sorted(required_keys - set(frame.columns))
        if missing_keys:
            warnings.warn(
                f"Serving key(s) {missing_keys} are absent from `spine_df`. Every feature group"
                " they identify is skipped and its features come back as NULL.",
                stacklevel=3,
            )

        self._dataframe = self._build(frame)

    def _build(self, frame: pd.DataFrame) -> pd.DataFrame:
        import pandas as pd

        # The frame is the spine as given: one row per entity and moment, in the caller's order.
        # The result comes back in that order, so predictions zip onto it positionally.
        spine = frame.reset_index(drop=True).copy()
        times = pd.to_datetime(spine[self._event_time], utc=True, errors="coerce")
        if times.isna().any():
            raise FeatureStoreException(
                f"`spine_df[{self._event_time!r}]` contains a value that is not a timestamp."
            )
        # The feature store keeps event times to the millisecond, and the file is written to
        # match. A wall-clock timestamp carries microseconds, so without this a spine built from
        # `datetime.now()` is refused for losing precision nobody asked to keep.
        spine[self._event_time] = times.dt.floor("ms")

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

    def _hive_type(self, column: str) -> str:
        """The type a spine column is written and declared as.

        The feature view's schema for a column it defines. For a passthrough column it defines
        none, so the frame's own dtype decides. Both the Parquet file and the wire form read
        this, because a file typed differently from its declaration fails at the CAST.
        """
        if column == self._event_time:
            return "timestamp"
        if column in self._passthrough:
            return _PASSTHROUGH_TYPE.get(str(self._frame_dtype(column)), "string")
        return self._types.get(column, "string")

    def arrow_table(self) -> Any:
        """The spine as an Arrow table typed from the feature view's schema.

        Raises:
            FeatureStoreException: If a supplied value does not convert to the feature's type.
        """
        import pyarrow as pa

        fields = [pa.field(ROW_ID_COLUMN, pa.int64())]
        for column in self.supplied_columns:
            fields.append(pa.field(column, _pyarrow_type(self._hive_type(column))))
        try:
            return pa.Table.from_pandas(
                self._dataframe, schema=pa.schema(fields), preserve_index=False
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as e:
            raise FeatureStoreException(
                f"An `spine_df` value does not convert to the type the feature view declares: {e}"
            ) from e

    def write_parquet(self, directory: str) -> str:
        """Write the spine to a local Parquet file and return its path."""
        import pyarrow.parquet as pq

        path = os.path.join(directory, self._basename)
        pq.write_table(self.arrow_table(), path)
        return path

    def _frame_dtype(self, column: str) -> Any:
        return self._dataframe[column].dtype

    def to_dict(self) -> dict[str, Any]:
        """The wire form: the schema and where the rows are, never the rows."""
        columns = [{"name": ROW_ID_COLUMN, "type": "bigint"}]
        for column in self.supplied_columns:
            entry = {"name": column, "type": self._hive_type(column)}
            if column in self._passthrough:
                # The backend has no feature to take a type from for these. The declared type is
                # matched against a fixed allowlist there, never rendered as it arrives.
                entry["passthrough"] = True
            columns.append(entry)
        payload: dict[str, Any] = {
            "tableName": self._table_name,
            "eventTimeColumn": self._event_time,
            "columns": columns,
            "rowCount": self.row_count,
            "maxEventTime": self._max_event_time,
        }
        if self._parquet_staged:
            payload["parquetBasename"] = self._basename
        if self._max_feature_age_secs is not None:
            payload["maxFeatureAgeSecs"] = self._max_feature_age_secs
        return payload


def _to_pandas(spine_df: Any) -> pd.DataFrame | None:
    import pandas as pd

    if spine_df is None:
        return None
    if isinstance(spine_df, pd.DataFrame):
        return spine_df
    if isinstance(spine_df, list):
        return pd.DataFrame(spine_df)
    if HAS_POLARS:
        import polars as pl

        if isinstance(spine_df, pl.DataFrame):
            return spine_df.to_pandas()
    if spark_connect_utils._is_spark_dataframe(spine_df):
        from hsfs import engine

        if engine._get_type() != "spark":
            raise FeatureStoreException(
                "`spine_df` is a Spark DataFrame but the client is running the Python engine,"
                " which has no Spark session to evaluate it. Pass a pandas or polars DataFrame,"
                " or call `.toPandas()` yourself."
            )
        # Collected to the driver rather than kept distributed. The Spark path already went
        # through the driver: the rows are handed back to `createDataFrame` to register the
        # session temporary view, so a pandas spine took this same route. A spine is one row
        # per entity per prediction time and is capped at a million rows, so it is bounded.
        #
        # toPandas returns timestamps tz-naive in the Spark session timezone, and _build then
        # reads them as UTC. Those agree only because the Spark engine pins the session
        # timezone to UTC when it starts (hsfs/engine/spark.py). Unpin that and every event
        # time here shifts by the offset, silently.
        return spine_df.toPandas()
    raise TypeError(
        "`spine_df` must be a pandas, polars or Spark DataFrame, or a list of dicts;"
        f" got {type(spine_df)!r}."
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
