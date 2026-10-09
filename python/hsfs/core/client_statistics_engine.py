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

import base64
import datetime
import logging
import math
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
from hopsworks_common.core.constants import HAS_DATASKETCHES
from hsfs.core import (
    feature_group_api,
    incremental_statistics_engine,
    statistics_engine,
)
from hsfs.statistics import Statistics


if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from hsfs import feature_group as fg_mod
    from hsfs.feature_group_commit import FeatureGroupCommit


_logger = logging.getLogger(__name__)


class ClientStatisticsEngine:
    """Statistics of a small commit computed in the client instead of a Spark job.

    With `statistics_incremental_enabled` set, an insert on the Python engine into a Delta
    feature group profiles the inserted frame in process, merges the profile into the
    statistics of the snapshot before the commit, and registers the snapshot after it.
    The commit then tells the backend not to start the statistics job.
    The state and the merge rules are those of the Spark profiler's mergeable state and its
    merger, so either side can merge into what the other registered.
    The path applies to frames of at most `MAX_ROWS` rows of scalar columns, on a feature
    group whose statistics ask for nothing without a mergeable state, when the commit before
    this one has a snapshot with that state or the feature group has no commit yet.
    Anything else leaves the commit to the backend's statistics job, as before.
    A client that dies between the commit and the registration leaves that commit without
    statistics; the next commit then finds no snapshot of the latest commit and goes to the
    statistics job, whose full profile covers the rows of both.
    """

    # larger frames go to the statistics job, which profiles them in parallel
    MAX_ROWS = 100_000
    _FORMAT = "datasketches-native-v1"
    # the default of Spark's hll_sketch_agg and the K of the Spark profiler's KLL sketches
    _HLL_LG_K = 12
    _KLL_K = 2048
    _PERCENTILE_FRACTIONS = [(ii + 1) / 100 for ii in range(99)]

    def __init__(self, feature_group: fg_mod.FeatureGroup):
        self._feature_group = feature_group
        self._statistics_engine = statistics_engine.StatisticsEngine(
            feature_group.feature_store_id, feature_group.ENTITY_TYPE
        )
        self._incremental = incremental_statistics_engine.IncrementalStatisticsEngine(
            self._statistics_engine
        )
        self._feature_group_api = feature_group_api.FeatureGroupApi()

    def _prepare(
        self, dataframe: pd.DataFrame | pl.DataFrame | pa.Table
    ) -> _PreparedStatistics | None:
        """Profile the frame before it is written, or `None` when the backend computes the statistics.

        Never raises: a failure here only means the commit keeps the backend's statistics job.
        """
        try:
            return self._prepare_or_none(dataframe)
        except Exception as e:
            _logger.info(f"Client statistics skipped: {e}")
            return None

    def _prepare_or_none(self, dataframe) -> _PreparedStatistics | None:
        config = self._feature_group.statistics_config
        if (
            not HAS_DATASKETCHES
            or config is None
            or not config.enabled
            or config.correlations
            or config.histograms
            or config.exact_uniqueness
            or config.kll
        ):
            return None
        # checked before the conversion, so a large insert pays nothing for this path
        if not hasattr(dataframe, "__len__") or len(dataframe) > self.MAX_ROWS:
            return None
        table = self._to_arrow(dataframe)
        if table is None:
            return None
        names = list(config.columns or []) or [
            feature.name for feature in self._feature_group.features
        ]
        if not names or any(name not in table.column_names for name in names):
            return None
        data_types = {}
        for name in list(names):
            arrow_type = table.schema.field(name).type
            if pa.types.is_decimal(arrow_type):
                # Spark profiles decimals, but renders them as text differently
                return None
            data_types[name] = _profile_type(arrow_type)
            if data_types[name] is None:
                # the Spark profiler skips the column, so its statistics never hold it
                names.remove(name)
                del data_types[name]
        if not names:
            return None
        if not self._incremental._enabled():
            return None

        commits = _as_list(
            self._feature_group_api._get_commit_details(self._feature_group, None, 1)
        )
        previous_end = commits[0].commit_time if commits else None
        previous_states = None
        if previous_end is not None:
            previous = self._incremental._previous_snapshot(
                self._feature_group, previous_end + 1
            )
            if previous is None or previous.window_end_commit_time != previous_end:
                return None
            by_name = {
                fds.feature_name: fds
                for fds in previous.feature_descriptive_statistics or []
            }
            previous_states = {}
            for name in names:
                state = _ColumnState._of_fds(by_name[name]) if name in by_name else None
                if state is None:
                    return None
                previous_states[name] = state
        states = {
            name: _ColumnState._of_arrow(table.column(name), data_types[name])
            for name in names
        }
        return _PreparedStatistics(
            names=names,
            states=states,
            previous_end=previous_end,
            previous_states=previous_states,
            num_rows=table.num_rows,
        )

    def _register_commit_statistics(
        self, prepared: _PreparedStatistics, commit: FeatureGroupCommit
    ) -> None:
        """Register the statistics of the snapshot after the commit.

        When the commit is not the plain append the profile describes, or a concurrent commit
        came in between, the statistics job computes them instead, as the backend would have.
        """
        try:
            if self._merges_cleanly(prepared, commit):
                self._save(prepared, commit)
                return
        except Exception as e:
            _logger.warning(
                f"Client statistics of commit {commit.commit_time} failed, starting the statistics job: {e}"
            )
        try:
            self._statistics_engine._statistics_api._compute(
                self._feature_group, end_commit_time=commit.commit_time
            )
        except Exception as e:
            _logger.warning(
                f"Could not start the statistics job of commit {commit.commit_time}: {e}"
            )

    def _merges_cleanly(
        self, prepared: _PreparedStatistics, commit: FeatureGroupCommit
    ) -> bool:
        if commit is None or commit.commit_time is None:
            return False
        if (commit.rows_updated or 0) > 0 or (commit.rows_deleted or 0) > 0:
            _logger.info("Client statistics: the commit updated or deleted rows")
            return False
        if (
            commit.rows_inserted is not None
            and commit.rows_inserted != prepared.num_rows
        ):
            _logger.info(
                "Client statistics: the commit wrote other rows than the frame"
            )
            return False
        commits = _as_list(
            self._feature_group_api._get_commit_details(
                self._feature_group, commit.commit_time, 2
            )
        )
        before = [c.commit_time for c in commits if c.commit_time != commit.commit_time]
        expected = [] if prepared.previous_end is None else [prepared.previous_end]
        if before[:1] != expected:
            _logger.info("Client statistics: another commit came in between")
            return False
        return True

    def _save(self, prepared: _PreparedStatistics, commit: FeatureGroupCommit) -> None:
        columns = []
        for name in prepared.names:
            state = prepared.states[name]
            if prepared.previous_states is not None:
                state = prepared.previous_states[name]._plus(state)
            columns.append(state._to_profile(name))
        statistics = Statistics(
            computation_time=int(
                datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
            ),
            row_percentage=1.0,
            feature_descriptive_statistics=self._statistics_engine._parse_deequ_statistics(
                {"columns": columns}, False
            ),
            window_end_commit_time=commit.commit_time,
        )
        self._statistics_engine._save_statistics(statistics, self._feature_group, None)
        _logger.info(f"Client statistics registered for commit {commit.commit_time}")

    @staticmethod
    def _to_arrow(dataframe) -> pa.Table | None:
        # the table the Delta write stores, so null and NaN mean what they mean there
        from hsfs.core import delta_engine

        if isinstance(dataframe, pa.Table):
            return delta_engine.DeltaEngine._prepare_df_for_delta(dataframe)
        if hasattr(dataframe, "to_arrow"):
            return delta_engine.DeltaEngine._prepare_df_for_delta(dataframe.to_arrow())
        if hasattr(dataframe, "copy"):
            # the preparation reassigns timezone-aware columns, which must not reach the caller
            return delta_engine.DeltaEngine._prepare_df_for_delta(
                dataframe.copy(deep=False)
            )
        return None


@dataclass
class _PreparedStatistics:
    names: list[str]
    states: dict[str, _ColumnState]
    previous_end: int | None
    previous_states: dict[str, _ColumnState] | None
    num_rows: int


@dataclass
class _ColumnState:
    """The mergeable state of a column, as the Spark profiler writes it and its merger reads it."""

    data_type: str
    nulls: int
    count: int
    total: float = 0.0
    mean: float = 0.0
    m2: float = 0.0
    minimum: float = math.nan
    maximum: float = math.nan
    hll: Any = None
    kll: Any = None

    @property
    def _numeric(self) -> bool:
        return self.data_type in ("Integral", "Fractional")

    @classmethod
    def _of_arrow(cls, column: pa.ChunkedArray, data_type: str) -> _ColumnState:
        non_null = pc.drop_null(column)
        hll = _sketches().hll_sketch(
            ClientStatisticsEngine._HLL_LG_K, _sketches().tgt_hll_type.HLL_4
        )
        # a sketch counts a value once however often it is updated with it
        for value in pc.unique(non_null).to_pylist():
            hll.update(_spark_string(value, column.type))
        state = cls(data_type=data_type, nulls=column.null_count, count=len(non_null))
        state.hll = hll
        if not state._numeric:
            return state
        values = np.asarray(non_null.to_numpy(), dtype=np.float64)
        state.kll = _sketches().kll_doubles_sketch(ClientStatisticsEngine._KLL_K)
        finite = values[np.isfinite(values)]
        if len(finite):
            state.kll.update(finite)
        if state.count:
            with np.errstate(all="ignore"), warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                # Spark's sum, mean and population variance; NaN sorts above every number,
                # so it is the maximum as soon as one value is NaN
                state.total = float(np.sum(values))
                state.mean = state.total / state.count
                state.m2 = float(np.sum((values - state.mean) ** 2))
                state.minimum = float(np.nanmin(values))
                state.maximum = (
                    math.nan if np.isnan(values).any() else float(np.max(values))
                )
        return state

    @classmethod
    def _of_fds(cls, fds) -> _ColumnState | None:
        mergeable = (fds.extended_statistics or {}).get("mergeable") or {}
        if mergeable.get(
            "format"
        ) != ClientStatisticsEngine._FORMAT or not mergeable.get("hll"):
            return None
        moments = mergeable.get("moments") or {}
        state = cls(
            data_type=fds.feature_type,
            nulls=fds.num_null_values or 0,
            count=int(moments.get("n", 0)),
        )
        if state.count and "mean" in moments:
            state.total = float(moments["sum"])
            state.mean = float(moments["mean"])
            state.m2 = float(moments["m2"])
            state.minimum = float(moments["min"])
            state.maximum = float(moments["max"])
        state.hll = _sketches().hll_sketch.deserialize(
            base64.b64decode(mergeable["hll"])
        )
        if mergeable.get("kll"):
            state.kll = _sketches().kll_doubles_sketch.deserialize(
                base64.b64decode(mergeable["kll"])
            )
        return state

    def _plus(self, other: _ColumnState) -> _ColumnState:
        union = _sketches().hll_union(ClientStatisticsEngine._HLL_LG_K)
        union.update(self.hll)
        union.update(other.hll)
        merged = _ColumnState(
            data_type=other.data_type,
            nulls=self.nulls + other.nulls,
            count=self.count + other.count,
            hll=union.get_result(_sketches().tgt_hll_type.HLL_4),
        )
        sketches = [s for s in (self.kll, other.kll) if s is not None]
        if sketches:
            merged.kll = _sketches().kll_doubles_sketch.deserialize(
                sketches[0].serialize()
            )
            for sketch in sketches[1:]:
                merged.kll.merge(sketch)
        if self.count == 0 or other.count == 0:
            source = other if self.count == 0 else self
            merged.total, merged.mean, merged.m2 = source.total, source.mean, source.m2
            merged.minimum, merged.maximum = source.minimum, source.maximum
            return merged
        # the parallel variance formula, as the Spark profiler's merger applies it
        mean_delta = other.mean - self.mean
        merged.total = self.total + other.total
        merged.mean = (self.mean * self.count + other.mean * other.count) / merged.count
        merged.m2 = (
            self.m2
            + other.m2
            + mean_delta * mean_delta * (self.count * other.count / merged.count)
        )
        merged.minimum = _spark_min(self.minimum, other.minimum)
        merged.maximum = (
            math.nan
            if math.isnan(self.maximum) or math.isnan(other.maximum)
            else max(self.maximum, other.maximum)
        )
        return merged

    def _to_profile(self, name: str) -> dict[str, Any]:
        total = self.count + self.nulls
        profile = {
            "column": name,
            "dataType": self.data_type,
            "isDataTypeInferred": "false",
            "completeness": 0.0 if total == 0 else self.count / total,
            "numRecordsNonNull": self.count,
            "numRecordsNull": self.nulls,
            "approximateNumDistinctValues": round(self.hll.get_estimate()),
        }
        mergeable = {
            "format": ClientStatisticsEngine._FORMAT,
            "hll": base64.b64encode(self.hll.serialize_compact()).decode("ascii"),
        }
        if self._numeric and self.count > 0:
            std_dev = math.sqrt(max(0.0, self.m2 / self.count))
            # NaN and the infinities are not valid JSON; the moments keep them as text
            for key, value in (
                ("mean", self.mean),
                ("maximum", self.maximum),
                ("minimum", self.minimum),
                ("sum", self.total),
                ("stdDev", std_dev),
            ):
                if math.isfinite(value):
                    profile[key] = value
        if self._numeric and self.kll is not None and not self.kll.is_empty():
            profile["approxPercentiles"] = list(
                self.kll.get_quantiles(
                    ClientStatisticsEngine._PERCENTILE_FRACTIONS, inclusive=True
                )
            )
            mergeable["kll"] = base64.b64encode(self.kll.serialize()).decode("ascii")
        moments: dict[str, Any] = {"n": self.count}
        if self._numeric and self.count > 0:
            moments.update(
                {
                    "sum": _java_double(self.total),
                    "mean": _java_double(self.mean),
                    "m2": _java_double(self.m2),
                    "min": _java_double(self.minimum),
                    "max": _java_double(self.maximum),
                }
            )
        mergeable["moments"] = moments
        profile["mergeable"] = mergeable
        return profile


def _sketches():
    # imported on use: a broken optional install must not break importing the SDK
    import datasketches

    return datasketches


def _as_list(commits) -> list:
    if commits is None:
        return []
    return commits if isinstance(commits, list) else [commits]


def _spark_min(a: float, b: float) -> float:
    if math.isnan(a):
        return b
    if math.isnan(b):
        return a
    return min(a, b)


def _profile_type(arrow_type: pa.DataType) -> str | None:
    """The Spark profiler's type of a column, or `None` for a column it skips.

    The two profilers must cover the same columns: a merge emits only the columns of the
    batch, so a column one of them skips would drop out of the merged snapshot.
    """
    if pa.types.is_integer(arrow_type):
        return "Integral"
    if pa.types.is_floating(arrow_type):
        return "Fractional"
    if pa.types.is_boolean(arrow_type):
        return "Boolean"
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "String"
    return None


def _spark_string(value: Any, arrow_type: pa.DataType) -> str:
    """The text of a value as Spark's cast to string renders it, which the HLL sketch hashes.

    Both profilers must render a value alike for a union to count it once.
    """
    if pa.types.is_boolean(arrow_type):
        return "true" if value else "false"
    if pa.types.is_floating(arrow_type):
        return _java_double(value, single=pa.types.is_float32(arrow_type))
    if pa.types.is_timestamp(arrow_type):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc)
        text = value.strftime("%Y-%m-%d %H:%M:%S")
        if value.microsecond:
            text += f".{value.microsecond:06d}".rstrip("0")
        return text
    if pa.types.is_date(arrow_type):
        return value.isoformat()
    return str(value)


def _java_double(value: float, single: bool = False) -> str:
    """Java's `Double.toString` (or `Float.toString`), which Spark uses for a cast to string and the profiler for the moments."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "-0.0" if math.copysign(1.0, value) < 0 else "0.0"
    number = np.float32(value) if single else np.float64(value)
    if 1e-3 <= abs(value) < 1e7:
        return np.format_float_positional(number, unique=True, trim="0")
    mantissa, exponent = np.format_float_scientific(
        number, unique=True, trim="0"
    ).split("e")
    return f"{mantissa}E{int(exponent)}"
