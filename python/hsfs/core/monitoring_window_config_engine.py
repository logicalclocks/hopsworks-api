#
#   Copyright 2024 Hopsworks AB
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
import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypeVar

from hopsworks_common.client.exceptions import RestAPIError
from hsfs import feature_group, feature_view, util
from hsfs.core import monitoring_window_config as mwc
from hsfs.core import statistics_engine
from hsfs.feature import Feature
from hsfs.training_dataset_split import TrainingDatasetSplit


if TYPE_CHECKING:
    from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


logger = logging.getLogger(__name__)

# Maximum number of commits to enumerate when building a rolling reference via merge.
_MAX_COMMITS_FOR_MERGE = 100


class MonitoringWindowConfigEngine:
    _MAX_TIME_RANGE_LENGTH = 12

    def __init__(self, **kwargs):
        # No need to initialize anything
        pass

    def _init_statistics_engine(self, feature_store_id: int, entity_type: str):
        self._statistics_engine = statistics_engine.StatisticsEngine(
            feature_store_id=feature_store_id,
            entity_type=entity_type,
        )

    def _validate_monitoring_window_config(
        self,
        time_offset: str | None = None,
        window_length: str | None = None,
        training_dataset_version: int | None = None,
        row_percentage: float | None = None,
    ) -> mwc.WindowConfigType:
        if isinstance(training_dataset_version, int):
            if any(
                [
                    time_offset is not None,
                    window_length is not None,
                    row_percentage is not None,
                ]
            ):
                raise ValueError(
                    "If training_dataset_version is set, no other window parameters can be set."
                )
            return mwc.WindowConfigType.TRAINING_DATASET

        if isinstance(time_offset, str):
            return mwc.WindowConfigType.ROLLING_TIME

        if isinstance(window_length, str):
            raise ValueError("window_length can only be set if time_offset is set.")

        return mwc.WindowConfigType.ALL_TIME

    def _build_monitoring_window_config(
        self,
        id: int | None = None,
        window_config_type: mwc.WindowConfigType | str | None = None,
        time_offset: str | None = None,
        window_length: str | None = None,
        training_dataset_version: int | None = None,
        row_percentage: float | None = None,
    ) -> mwc.MonitoringWindowConfig:
        """Builds a monitoring window config.

        Parameters:
            id: int, optional
                Id of the monitoring window config in hopsworks.
            window_config_type: str, required
                Type of the window config, can be either
                `ROLLING_TIME`, `ALL_TIME`,`TRAINING_DATASET`.
            time_offset: str, optional
                monitoring window start time is computed as "now - time_offset".
            window_length: str, optional
                monitoring window end time is computed as
                    "now - time_offset + window_length".
            training_dataset_version: int, optional
                Specific id of an entity that has fixed statistics.
            row_percentage: float, optional
                Percentage of rows to be used for statistics computation.

        Returns:
            The monitoring window configuration.
        """
        detected_window_config_type = self._validate_monitoring_window_config(
            time_offset=time_offset,
            window_length=window_length,
            training_dataset_version=training_dataset_version,
            row_percentage=row_percentage,
        )

        if (
            isinstance(window_config_type, str)
            and window_config_type != detected_window_config_type
        ):
            raise ValueError(
                "The window_config_type parameter does not match the window parameters set."
            )

        if (
            window_config_type
            in [mwc.WindowConfigType.ROLLING_TIME, mwc.WindowConfigType.ALL_TIME]
            and row_percentage is None
        ):
            row_percentage = 1.0

        return mwc.MonitoringWindowConfig(
            id=id,
            window_config_type=detected_window_config_type,
            time_offset=time_offset,
            window_length=window_length,
            training_dataset_version=training_dataset_version,
            row_percentage=row_percentage,
        )

    def _time_range_str_to_time_delta(
        self, time_range: str, field_name: str | None = "time_offset"
    ) -> timedelta:
        # sanitize input
        value_error_message = f"Invalid {field_name} format: {time_range}. Use format: 1w2d3h for 1 week, 2 days and 3 hours."
        if (
            len(time_range) > self._MAX_TIME_RANGE_LENGTH
            or re.search(r"([^dwh\d]+)", time_range) is not None
        ):
            raise ValueError(value_error_message)

        matches = re.search(
            # r"^(?!$)(?:.*(?P<week>\d+)w)?(?:.*(?P<day>\d+)d)?(?:.*(?P<hour>\d+)h)?$",
            r"(?:(?P<week>\d+w)()|(?P<day>\d+d)()|(?P<hour>\d+h)())+",
            time_range,
        )
        if matches is None:
            raise ValueError(value_error_message)

        weeks = (
            int(matches.group("week").replace("w", ""))
            if matches.group("week") is not None
            else 0
        )
        days = (
            int(matches.group("day").replace("d", ""))
            if matches.group("day") is not None
            else 0
        )
        hours = (
            int(matches.group("hour").replace("h", ""))
            if matches.group("hour") is not None
            else 0
        )

        return timedelta(weeks=weeks, days=days, hours=hours)

    def _get_window_start_end_times(
        self,
        monitoring_window_config: mwc.MonitoringWindowConfig,
        anchor_end_ms: int | None = None,
    ) -> tuple[int | None, int]:
        """Compute the (start_time, end_time) pair for a monitoring window.

        Both values are millisecond timestamps compatible with the Feature Store
        commit-time API.

        When ``anchor_end_ms`` is provided (e.g. an ingestion-triggered commit
        time or the latest FG commit for model-monitoring), the window end is
        pinned to that value instead of ``datetime.now()``.
        For offset-based windows (ROLLING_TIME with a ``time_offset``), the
        window start is also recomputed relative to ``anchor_end_ms`` so that
        the resulting window is always ``[anchor_end - offset, anchor_end]``.
        This avoids inverted windows when the offset is shorter than the
        materialization lag (e.g. ``time_offset="30m"`` on a feature group that
        materializes data hourly).

        ALL_TIME windows and windows without a ``time_offset`` always return
        ``start_time=None`` regardless of ``anchor_end_ms``.

        Parameters:
            monitoring_window_config: Window configuration describing the type,
                offset, and optional window length.
            anchor_end_ms: Optional commit timestamp in milliseconds to use as
                the window end anchor instead of the current wall-clock time.
                When set, the start time is also derived from this anchor for
                offset-based windows.

        Returns:
            A (start_time_ms, end_time_ms) tuple where start_time_ms is None
            for ALL_TIME and no-offset windows.
        """
        if anchor_end_ms is not None:
            # Work in milliseconds directly to avoid the datetime→ms round-trip
            # producing a wrong value when the local timezone is not UTC.
            # (convert_event_time_to_timestamp replaces tz=None with UTC, which
            # shifts the timestamp by the local UTC offset.)
            if monitoring_window_config.window_config_type not in [
                mwc.WindowConfigType.ROLLING_TIME,
                mwc.WindowConfigType.ALL_TIME,
            ]:
                return (None, anchor_end_ms)

            if monitoring_window_config.time_offset is not None:
                offset_ms = int(
                    self.time_range_str_to_time_delta(
                        monitoring_window_config.time_offset
                    ).total_seconds()
                    * 1000
                )
                start_ms = anchor_end_ms - offset_ms
                end_ms = anchor_end_ms

                if monitoring_window_config.window_length is not None:
                    window_ms = int(
                        self.time_range_str_to_time_delta(
                            monitoring_window_config.window_length
                        ).total_seconds()
                        * 1000
                    )
                    # Only shrink end_time when start+window is before the anchor.
                    if start_ms + window_ms < end_ms:
                        end_ms = start_ms + window_ms

                return (start_ms, end_ms)

            # ALL_TIME or no-offset: start=None, end=anchor.
            return (None, anchor_end_ms)

        end_time = datetime.now()

        if monitoring_window_config.window_config_type not in [
            mwc.WindowConfigType.ROLLING_TIME,
            mwc.WindowConfigType.ALL_TIME,
        ]:
            return (
                None,
                self._round_and_convert_event_time(event_time=end_time),
            )

        if monitoring_window_config.time_offset is not None:
            time_offset = self._time_range_str_to_time_delta(
                monitoring_window_config.time_offset
            )
            start_time = end_time - time_offset
        else:
            # case where time_offset is None and window_length is None
            return (
                None,
                self._round_and_convert_event_time(event_time=end_time),
            )

        if monitoring_window_config.window_length is not None:
            window_length = self._time_range_str_to_time_delta(
                monitoring_window_config.window_length
            )
            end_time = (
                start_time + window_length
                if start_time + window_length < end_time
                else end_time
            )

        return (
            self._round_and_convert_event_time(event_time=start_time),
            self._round_and_convert_event_time(event_time=end_time),
        )

    def _run_single_window_monitoring(
        self,
        entity: feature_group.FeatureGroup | feature_view.FeatureView,
        monitoring_window_config: mwc.MonitoringWindowConfig,
        feature_names: list[str],
        profile_flags: dict | None = None,
        end_commit_time_override: int | None = None,
        model_filter: tuple[str, int] | None = None,
    ) -> list[FeatureDescriptiveStatistics]:
        """Fetch the entity data based on monitoring window configuration and compute statistics.

        Parameters:
            entity: The entity to monitor.
            monitoring_window_config: Monitoring window config.
            feature_names: Names of the features to monitor.
            profile_flags: optional statistics profiling flags (e.g. histograms,
                correlations, kll) forwarded to the statistics engine.
            end_commit_time_override: when set, pins the detection window end time to this
                commit timestamp (ms) instead of ``now``. Used by ingestion-triggered configs
                to tag the resulting Statistics row with the exact triggering commit.
            model_filter: optional (model_name, model_version) tuple. When set, the live
                read path filters rows by ``model_name = X AND model_version = str(Y)``
                and the precomputed-stats lookup is skipped (registered stats are
                aggregated over the whole entity and are not model-aware).

        Returns:
            List of Descriptive statistics.
        """
        self._init_statistics_engine(entity._feature_store_id, entity.ENTITY_TYPE)
        (
            start_time,
            end_time,
        ) = self._get_window_start_end_times(
            monitoring_window_config=monitoring_window_config,
            anchor_end_ms=end_commit_time_override,
        )

        registered_stats = None  # no stats by default value

        if (
            monitoring_window_config.window_config_type
            == mwc.WindowConfigType.TRAINING_DATASET
            and isinstance(entity, feature_view.FeatureView)
        ):
            # split untransformed and transformed features
            after_transf_features, before_transf_features = [], []
            if entity.transformation_functions is None:
                after_transf_features = feature_names
            else:
                transformation_features = {
                    transformation_feature
                    for transformation_function in entity.transformation_functions
                    for transformation_feature in transformation_function.hopsworks_udf.transformation_features
                }
                for feat_name in feature_names:
                    if feat_name in transformation_features:
                        before_transf_features.append(feat_name)
                    else:
                        after_transf_features.append(feat_name)

            if after_transf_features:
                registered_stats = entity.get_training_dataset_statistics(
                    training_dataset_version=monitoring_window_config.training_dataset_version,
                    before_transformation=False,
                    feature_names=after_transf_features,
                )

                if (
                    registered_stats.feature_descriptive_statistics is None
                    and registered_stats.split_statistics is not None
                ):
                    # if td splits, we use the train set statistics
                    for split in registered_stats.split_statistics:
                        if split.name == TrainingDatasetSplit.TRAIN:
                            registered_stats = split
                assert registered_stats.feature_descriptive_statistics, (
                    "Registered statistics must have feature descriptive statistics"
                )

            if before_transf_features:
                before_transf_stats = entity.get_training_dataset_statistics(
                    training_dataset_version=monitoring_window_config.training_dataset_version,
                    before_transformation=True,
                    feature_names=before_transf_features,
                )
                assert before_transf_stats.feature_descriptive_statistics, (
                    "Registered statistics before transformations must have feature descriptive statistics"
                )

                if registered_stats is None:
                    registered_stats = before_transf_stats
                else:
                    registered_stats.feature_descriptive_statistics.extend(
                        before_transf_stats.feature_descriptive_statistics
                    )
        elif model_filter is None:
            # Check if statistics already exists. Skip when a model_filter is in play —
            # registered stats are aggregated over the whole logging FG, not per-model,
            # and would conflate inference logs across deployments.
            registered_stats = self._statistics_engine._get_by_time_window(
                metadata_instance=entity,
                start_commit_time=start_time,
                end_commit_time=end_time,
                feature_names=feature_names,
                row_percentage=monitoring_window_config.row_percentage,
            )

        if registered_stats is None:  # if statistics don't exist
            # TODO: What happens if window is TRAINING_DATASET and the TD statistics were not computed???

            # Try the KLL-merge path for rolling reference windows on HUDI/DELTA FGs
            # with distribution (PDF) monitoring. Only fires for the reference window
            # since the caller (feature_monitoring_config_engine) passes profile_flags
            # only when distribution metrics are configured.
            merged_fds_list = None
            if self._should_use_merge_path(
                entity, monitoring_window_config, profile_flags
            ):
                merged_fds_list = self._resolve_rolling_reference_via_merge(
                    entity=entity,
                    feature_names=feature_names,
                    start_time=start_time,
                    end_time=end_time,
                    histogram_bins=profile_flags.get("histogram_bins") or 20,
                )

            if merged_fds_list is not None:
                return merged_fds_list

            # Fetch the actual data for which to compute statistics based on row_percentage and time window
            entity_feature_df = self._fetch_entity_data_in_monitoring_window(
                entity=entity,
                feature_names=feature_names,
                start_time=start_time,
                end_time=end_time,
                row_percentage=monitoring_window_config.row_percentage,
                model_filter=model_filter,
            )

            # Compute statistics on the feature dataframe.
            # `for_distribution_comparison` is a routing flag for the merge-path
            # dispatcher above, not a profile flag accepted by the statistics engine.
            extra_profile_flags = {
                k: v
                for k, v in (profile_flags or {}).items()
                if k != "for_distribution_comparison"
            }
            registered_stats = (
                self._statistics_engine._compute_and_save_monitoring_statistics(
                    entity,
                    feature_dataframe=entity_feature_df,
                    window_start_commit_time=start_time,
                    window_end_commit_time=end_time,
                    row_percentage=monitoring_window_config.row_percentage,
                    feature_name=feature_names,
                    **extra_profile_flags,
                )
            )

        assert registered_stats.feature_descriptive_statistics is not None, (
            "statistics should contain the feature descriptive statistics"
        )

        return registered_stats.feature_descriptive_statistics

    def _fetch_entity_data_in_monitoring_window(
        self,
        entity: feature_group.FeatureGroup | feature_view.FeatureView,
        feature_names: list[str],
        start_time: int | None,
        end_time: int | None,
        row_percentage: float,
        model_filter: tuple[str, int] | None = None,
    ) -> TypeVar("pyspark.sql.DataFrame"):
        """Fetch the entity data based on time window and row percentage.

        Parameters:
            entity: Entity to monitor.
            feature_names: Names of the features to monitor.
            start_time: Window start commit or event time
            end_time: Window end commit or event time
            row_percentage: fraction of rows to include [0, 1.0]
            model_filter: optional (model_name, model_version). When set, the read is
                additionally filtered by ``model_name = X AND model_version = str(Y)``.
                Only meaningful when the entity is a logging FG that carries those columns.

        Returns:
            A Spark DataFrame with the entity data
        """
        try:
            if isinstance(entity, feature_group.FeatureGroup):
                entity_df = self._fetch_feature_group_data(
                    entity=entity,
                    feature_names=feature_names,
                    start_time=start_time,
                    end_time=end_time,
                    model_filter=model_filter,
                )
            else:
                entity_df = self._fetch_feature_view_data(
                    entity=entity,
                    feature_names=feature_names,
                    start_time=start_time,
                    end_time=end_time,
                    model_filter=model_filter,
                )

            if row_percentage < 1.0:
                entity_df = entity_df.sample(fraction=row_percentage)

        except RestAPIError as e:
            if (
                e.response.json().get("errorCode", "") == 270118
                and e.response.status_code == 404
            ):
                # creating an empty Spark DataFrame requires a valid schema [SchemaType(name, type)]
                # and takes unnecessary time and resources. We can return an empty pandas dataframe instead
                # because the computation of statistics will be discarded in statistics_engine (len(df.head()) == 0)
                import pandas as pd

                return pd.DataFrame(columns=feature_names)
            raise e

        return entity_df

    def _fetch_feature_view_data(
        self,
        entity: feature_view.FeatureView,
        feature_names: list[str] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        model_filter: tuple[str, int] | None = None,
    ) -> TypeVar("pyspark.sql.DataFrame"):
        """Fetch the feature view data based on time window and row percentage.

        Parameters:
            entity: Feature view to monitor.
            feature_names: Names of the features to monitor.
            start_time: Window start commit or event time.
            end_time: Window end commit or event time.
            model_filter: optional (model_name, model_version) for logging-FG filtering.

        Returns:
            A Spark DataFrame with the entity data
        """
        # TODO: This fails for FV on non-time-travel FGs.
        # FSTORE-2050: apply the model filter on the Query before .read() so the filter
        # is pushed down to the engine. The logging FG carries model_name STRING and
        # model_version STRING columns (FeatureLoggingController), so cast to str for
        # the version comparison.
        fv_query = entity.query
        if model_filter is not None:
            model_name, model_version = model_filter
            fv_query = fv_query.filter(Feature("model_name") == model_name).filter(
                Feature("model_version") == str(model_version)
            )
            # See fetch_feature_group_data: logging FGs lack delta.enableChangeDataFeed,
            # so windowing uses the log_time event-time column instead of Delta CDF.
            if start_time is not None:
                fv_query = fv_query.filter(
                    Feature("log_time", type="timestamp") >= start_time
                )
            if end_time is not None:
                fv_query = fv_query.filter(
                    Feature("log_time", type="timestamp") <= end_time
                )
            entity_df = fv_query.read()
        else:
            entity_df = fv_query.as_of(
                exclude_until=start_time, wallclock_time=end_time
            ).read()

        if feature_names:
            entity_df = entity_df.select(feature_names)

        return entity_df

    def _fetch_feature_group_data(
        self,
        entity: feature_group.FeatureGroup,
        feature_names: list[str] | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        model_filter: tuple[str, int] | None = None,
    ) -> TypeVar("pyspark.sql.Dataframe"):
        """Fetch the feature group data based on time window.

        Parameters:
            entity: Feature group to monitor.
            feature_names: Names of the features to monitor.
            start_time: Window start commit time.
            end_time: Window end commit time.
            model_filter: optional (model_name, model_version) for logging-FG filtering.
        """
        # FSTORE-2050: model filtering uses the FG's `Filter` constructs so it composes
        # with select() and as_of(). This works only for cached/stream FGs; the logging
        # FG is always cached (created by FeatureLoggingController), so this is safe.
        pre_df = entity.select(features=feature_names) if feature_names else entity
        if model_filter is not None:
            model_name, model_version = model_filter
            # Logging FG stores model_version as STRING (FeatureLoggingController), so
            # cast to str for the Filter to match the column type.
            pre_df = pre_df.filter(Feature("model_name") == model_name).filter(
                Feature("model_version") == str(model_version)
            )
            # Logging FGs are not created with delta.enableChangeDataFeed=true, so
            # as_of(exclude_until=...) — which compiles to a Delta CDF read — would fail
            # with DELTA_MISSING_CHANGE_DATA. Filter on the log_time event-time column
            # (added by FeatureLoggingController as a TIMESTAMP metadata feature) instead.
            if start_time is not None:
                pre_df = pre_df.filter(
                    Feature("log_time", type="timestamp") >= start_time
                )
            if end_time is not None:
                pre_df = pre_df.filter(
                    Feature("log_time", type="timestamp") <= end_time
                )
            return pre_df.read()

        return pre_df.as_of(exclude_until=start_time, wallclock_time=end_time).read()

    def _round_and_convert_event_time(self, event_time: datetime) -> int | None:
        """Round event time to the latest hour and convert to timestamp.

        Parameters:
            event_time: datetime: Event time to round and convert.

        Returns:
            datetime: Rounded and converted event time.
        """
        return util._convert_event_time_to_timestamp(event_time)

    def _should_use_merge_path(
        self,
        entity: feature_group.FeatureGroup | feature_view.FeatureView,
        monitoring_window_config: mwc.MonitoringWindowConfig,
        profile_flags: dict | None,
    ) -> bool:
        """Return True when all gating conditions for the KLL-merge path are met.

        Requires both `kll=True` AND the explicit `for_distribution_comparison=True`
        flag set by the PDF dispatcher. A caller that enables KLL on per-batch stats
        for any other reason (e.g., future use) must NOT take the merge path, because
        the synthetic reference FDS built by the merger lacks scalar fields (mean,
        stddev, correlations) that a scalar-comparison metric would need.
        """
        if (
            monitoring_window_config.window_config_type
            != mwc.WindowConfigType.ROLLING_TIME
        ):
            return False
        if not isinstance(entity, feature_group.FeatureGroup):
            return False
        if entity.time_travel_format not in ("HUDI", "DELTA"):
            return False
        if profile_flags is None:
            return False
        if not profile_flags.get("kll"):
            return False
        return profile_flags.get("for_distribution_comparison")

    def _resolve_rolling_reference_via_merge(
        self,
        entity: feature_group.FeatureGroup,
        feature_names: list[str],
        start_time: int | None,
        end_time: int | None,
        histogram_bins: int,
    ) -> list[FeatureDescriptiveStatistics] | None:
        """Resolve a rolling reference window via per-batch KLL sketch merging.

        For each feature, fetches the stored per-batch statistics rows within
        [start_time, end_time], extracts native KLL sidecars, and merges them
        via the JVM KllMerger into a synthetic reference FDS.

        Parameters:
            entity: Feature group whose per-batch statistics are merged.
            feature_names: Names of the features to resolve.
            start_time: Reference window start commit time.
            end_time: Reference window end commit time.
            histogram_bins: Number of bins for the CDF-approximated histogram.

        Returns:
            A list of synthetic FDS (one per feature) when all features merge
            successfully. Returns None if any feature falls back, so the caller
            re-profiles the whole window.
        """
        from hsfs.core.distribution_engine import DistributionEngine

        # Fetch all per-batch stats rows with content for the window. Cap at
        # _MAX_COMMITS_FOR_MERGE to bound response size (each row carries an
        # `extended_statistics` blob with base64-encoded KLL bytes per feature).
        all_stats_rows = self._statistics_engine._get_all_in_time_window(
            metadata_instance=entity,
            start_commit_time=start_time,
            end_commit_time=end_time,
            feature_names=feature_names,
            limit=_MAX_COMMITS_FOR_MERGE,
        )

        if not all_stats_rows:
            return None

        # If the response hit the limit, the merge sample would be truncated and
        # drift undetectably. Fall back to full-window re-profile.
        if len(all_stats_rows) >= _MAX_COMMITS_FOR_MERGE:
            logger.warning(
                "Rolling reference window contains %d+ commits (>= %d cap); falling back "
                "to full-window re-profile. Shorten the reference window or increase "
                "_MAX_COMMITS_FOR_MERGE to unblock merge.",
                len(all_stats_rows),
                _MAX_COMMITS_FOR_MERGE,
            )
            return None

        # Group all FDS objects by feature name across all stats rows.
        fds_by_feature: dict[str, list] = {name: [] for name in feature_names}
        for stats_row in all_stats_rows:
            if stats_row.feature_descriptive_statistics is None:
                continue
            for fds in stats_row.feature_descriptive_statistics:
                if fds.feature_name in fds_by_feature:
                    fds_by_feature[fds.feature_name].append(fds)

        dist_engine = DistributionEngine()
        merged_results = []

        for feat_name in feature_names:
            fds_list = fds_by_feature.get(feat_name, [])
            num_batches = len(fds_list)

            logger.info(
                "Resolving rolling reference via KLL merge over N=%d batches for feature '%s'",
                num_batches,
                feat_name,
            )

            merged_fds = dist_engine.resolve_merged_reference(fds_list, histogram_bins)
            if merged_fds is None:
                # One feature failed to merge — fall back entirely to re-profile.
                return None

            merged_results.append(merged_fds)

        return merged_results if merged_results else None
