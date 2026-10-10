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

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from hopsworks_common.core import variable_api
from hsfs import engine, feature_group
from hsfs.core import feature_group_api
from hsfs.core import monitoring_window_config as mwc
from hsfs.statistics import Statistics


if TYPE_CHECKING:
    from hsfs.core import statistics_engine


_logger = logging.getLogger(__name__)


class IncrementalStatisticsEngine:
    """Statistics of a commit from the statistics of the commit before it.

    An ingestion statistics run profiles the whole table as of its commit, so a feature group
    that keeps growing pays more for every commit.
    When `statistics_incremental_enabled` is set, the run profiles only the rows the commits
    since the previous snapshot added, with the state a profile can be merged with, and merges
    them into the previous snapshot's statistics in the JVM.
    The result is the snapshot's statistics, as before: counts, sums, min and max are exact,
    mean and standard deviation follow the parallel variance formula, the percentiles come
    from a merged KLL sketch and the distinct count from a merged HLL sketch.
    The merge applies to an append-only stretch of commits on a Delta or Hudi feature group
    whose statistics ask for nothing without a mergeable state, that is no correlations,
    histograms or exact uniqueness; anything else profiles the whole snapshot as before.
    """

    VARIABLE = "statistics_incremental_enabled"
    # commits looked at between two snapshots; more than this profiles the snapshot in full
    MAX_COMMITS = 100
    _MERGEABLE_FORMAT = "datasketches-native-v1"

    def __init__(self, statistics_engine: statistics_engine.StatisticsEngine):
        self._statistics_engine = statistics_engine
        self._feature_group_api = feature_group_api.FeatureGroupApi()
        self._variable_api = variable_api.VariableApi()

    def _enabled(self) -> bool:
        """Whether the cluster allows incremental statistics; a backend without the setting does not."""
        try:
            return (
                str(self._variable_api._get_variable(self.VARIABLE)).lower() == "true"
            )
        except Exception as e:
            _logger.debug(f"Incremental statistics setting not readable: {e}")
            return False

    def _applies(
        self,
        entity,
        monitoring_window_config: mwc.MonitoringWindowConfig,
        profile_flags: dict | None,
        start_time: int | None,
        end_time: int | None,
    ) -> bool:
        """Whether the window is one whose statistics can be merged from the previous snapshot."""
        if not isinstance(
            entity, feature_group.FeatureGroup
        ) or entity.time_travel_format not in (
            "DELTA",
            "HUDI",
        ):
            return False
        if (
            monitoring_window_config.window_config_type != mwc.WindowConfigType.ALL_TIME
            or (monitoring_window_config.row_percentage or 1.0) != 1.0
            or start_time not in (None, 0)
            or end_time is None
        ):
            return False
        flags = profile_flags or {}
        if any(
            flags.get(name)
            for name in ("correlations", "histograms", "exact_uniqueness")
        ):
            return False
        try:
            if engine._get_type() != "spark":
                return False
        except Exception:
            # no engine set up: nothing to merge with
            return False
        return self._enabled()

    def _compute(
        self,
        entity,
        feature_names: list[str] | None,
        end_commit_time: int,
        kll: bool = False,
        histogram_bins: int | None = None,
    ) -> Statistics | None:
        """Merge the rows committed since the previous snapshot into its statistics.

        Returns the saved statistics, or `None` when the merge does not apply or fails, and
        the caller profiles the whole snapshot.
        """
        try:
            return self._merge(
                entity, feature_names, end_commit_time, kll, histogram_bins
            )
        except Exception as e:
            _logger.warning(
                f"Incremental statistics failed, profiling the whole snapshot: {e}"
            )
            return None

    def _merge(
        self,
        entity,
        feature_names: list[str] | None,
        end_commit_time: int,
        kll: bool,
        histogram_bins: int | None,
    ) -> Statistics | None:
        """The merge itself, or `None` when it does not apply.

        It does not apply without a previous snapshot with a mergeable state, with a feature
        the snapshot does not cover, or with a commit in between that updated or deleted rows.
        """
        previous = self._previous_snapshot(entity, end_commit_time)
        if previous is None:
            return None
        names = self._profiled_names(
            entity, feature_names or [feature.name for feature in entity.features]
        )
        if not names:
            return None
        previous_fds = {
            fds.feature_name: fds
            for fds in previous.feature_descriptive_statistics or []
            if ((fds.extended_statistics or {}).get("mergeable") or {}).get("format")
            == self._MERGEABLE_FORMAT
        }
        missing = [name for name in names if name not in previous_fds]
        if missing:
            _logger.info(
                f"Incremental statistics: previous snapshot has no mergeable state for {missing}"
            )
            return None
        if not self._append_only_between(
            entity, previous.window_end_commit_time, end_commit_time
        ):
            return None
        delta_df = self._read_commits_after(
            entity, names, previous.window_end_commit_time, end_commit_time
        )
        if delta_df is None:
            return None
        spark_engine = engine._get_instance()
        delta_profile = spark_engine._profile(
            delta_df,
            names,
            False,
            False,
            False,
            kll,
            histogram_bins,
            mergeable_state=True,
        )
        previous_json = json.dumps([previous_fds[name].to_dict() for name in names])
        merged = spark_engine._jvm.com.logicalclocks.hsfs.spark.engine.profile.ProfileMerger.merge(
            previous_json, delta_profile
        )
        statistics = Statistics(
            computation_time=int(datetime.now(timezone.utc).timestamp() * 1000),
            row_percentage=1.0,
            feature_descriptive_statistics=self._statistics_engine._parse_deequ_statistics(
                merged, False
            ),
            window_end_commit_time=end_commit_time,
        )
        _logger.info(
            f"Incremental statistics: merged the commits after {previous.window_end_commit_time} "
            f"into the snapshot at {end_commit_time}"
        )
        return self._statistics_engine._save_statistics(statistics, entity, None)

    # offline types the Spark profiler profiles; it skips every other column (timestamps,
    # dates, arrays, structs, binary), so no statistics row ever holds one
    _PROFILED_TYPES = {
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "float",
        "double",
        "string",
        "boolean",
    }

    def _profiled_names(self, entity, names: list[str]) -> list[str]:
        types = {
            feature.name: (feature.type or "").lower() for feature in entity.features
        }
        return [
            name
            for name in names
            if types.get(name) is None
            or types[name] in self._PROFILED_TYPES
            or types[name].startswith("decimal")
        ]

    def _read_commits_after(
        self, entity, names: list[str], previous_end: int, end_commit_time: int
    ):
        """The rows the commits in (previous_end, end_commit_time] added, or `None`."""
        if entity.time_travel_format != "DELTA":
            return (
                entity.select(names)
                .as_of(exclude_until=previous_end, wallclock_time=end_commit_time)
                .read()
            )
        # Delta resolves the timestamp bounds of a change feed against the modification
        # times of the commit files, which are later than the commit times the backend
        # records: a timestamp-bounded read would take in the previous snapshot's commit and
        # leave out the last one. The bounds are read from the log as versions instead, and
        # each must be the very commit the backend recorded.
        from hsfs.core import delta_engine
        from pyspark.sql import functions as F

        spark = engine._get_instance()._spark_session
        reader = delta_engine.DeltaEngine(
            entity.feature_store_id,
            entity.feature_store_name,
            entity,
            spark,
            spark.sparkContext,
        )
        location = entity.prepare_spark_location()
        versions = []
        for commit_time in (previous_end, end_commit_time):
            version = reader._delta_version_at(location, commit_time)
            if version is None or self._delta_commit_time(
                reader, spark, location, version
            ) != int(commit_time):
                _logger.info(
                    f"Incremental statistics: no Delta version recorded at {commit_time}"
                )
                return None
            versions.append(version)
        if versions[1] <= versions[0]:
            return None
        return (
            spark.read.format(delta_engine.DeltaEngine.DELTA_SPARK_FORMAT)
            .option("readChangeFeed", "true")
            .option("startingVersion", versions[0] + 1)
            .option("endingVersion", versions[1])
            .load(location)
            .filter(F.col("_change_type") == "insert")
            .select(*names)
        )

    @staticmethod
    def _delta_commit_time(reader, spark, location: str, version: int) -> int | None:
        jvm = spark._jvm
        log_path = jvm.org.apache.hadoop.fs.Path(location.rstrip("/") + "/_delta_log")
        fs = log_path.getFileSystem(spark._jsc.hadoopConfiguration())
        return reader._delta_commit_timestamp(jvm, fs, log_path, version)

    def _previous_snapshot(self, entity, end_commit_time: int) -> Statistics | None:
        # the newest snapshot row (no start bound) before this commit, with its content
        rows = self._statistics_engine._statistics_api._get_all_in_window(
            entity, end_commit_time=end_commit_time - 1, limit=5
        )
        for row in rows or []:
            if (
                row.window_start_commit_time in (None, 0)
                and row.window_end_commit_time is not None
            ):
                return row
        return None

    def _append_only_between(
        self, entity, previous_end: int, end_commit_time: int
    ) -> bool:
        commits = self._feature_group_api._get_commit_details(
            entity, end_commit_time, self.MAX_COMMITS
        )
        if not isinstance(commits, list):
            commits = [commits] if commits is not None else []
        between = [
            c
            for c in commits
            if c.commit_time is not None and c.commit_time > previous_end
        ]
        if not between:
            _logger.info(
                "Incremental statistics: no commit after the previous snapshot"
            )
            return False
        if len(commits) >= self.MAX_COMMITS and len(between) == len(commits):
            _logger.info(
                "Incremental statistics: too many commits since the previous snapshot"
            )
            return False
        for commit in between:
            if (commit.rows_updated or 0) > 0 or (commit.rows_deleted or 0) > 0:
                _logger.info(
                    f"Incremental statistics: commit {commit.commit_time} updated or deleted rows"
                )
                return False
        return True
