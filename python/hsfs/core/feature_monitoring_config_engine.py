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
from datetime import date, datetime
from typing import TYPE_CHECKING

from hopsworks_common import util
from hopsworks_common.client.exceptions import FeatureStoreException, RestAPIError
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_config_api, monitoring_window_config_engine
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.feature_group_api import FeatureGroupApi
from hsfs.core.feature_monitoring_result_engine import FeatureMonitoringResultEngine
from hsfs.core.feature_statistics_config import FeatureStatisticsConfig
from hsfs.core.feature_statistics_result import FeatureStatisticsResult
from hsfs.core.job_api import JobApi
from hsfs.core.statistics_comparison_result import StatisticsComparisonResult


logger = logging.getLogger(__name__)


VALID_CATEGORICAL_METRICS = [
    "completeness",
    "num_non_null_values",
    "num_null_values",
    "distinctness",
    "entropy",
    "uniqueness",
    "approx_num_distinct_values",
    "exact_num_distinct_values",
]
VALID_FRACTIONAL_METRICS = [
    "completeness",
    "num_non_null_values",
    "num_null_values",
    "distinctness",
    "entropy",
    "uniqueness",
    "approx_num_distinct_values",
    "exact_num_distinct_values",
    "mean",
    "max",
    "min",
    "sum",
    "std_dev",
    "count",
]

# Scalar metrics that apply only to numeric features (present in fractional but not categorical)
NUMERIC_ONLY_SCALAR_METRICS = set(VALID_FRACTIONAL_METRICS) - set(
    VALID_CATEGORICAL_METRICS
)

# Distribution metrics restricted to numeric features only
NUMERIC_ONLY_DISTRIBUTION_METRICS = {"WASSERSTEIN", "KOLMOGOROV_SMIRNOV"}

# All supported distribution metrics
ALL_DISTRIBUTION_METRICS = {
    "PSI",
    "KL_DIVERGENCE",
    "JS_DIVERGENCE",
    "WASSERSTEIN",
    "HELLINGER",
    "KOLMOGOROV_SMIRNOV",
}

# Numeric Hive types (lowercase, use startswith for decimal variants)
NUMERIC_HIVE_TYPES = {"int", "bigint", "tinyint", "smallint", "float", "double"}

# Phase-2 types: skip silently in fan-out but reject explicitly if named
PHASE2_TYPES = {"timestamp", "date", "binary"}


if TYPE_CHECKING:
    import pandas as pd
    from hsfs import feature_group, feature_view
    from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
    from hsfs.core.job import Job
    from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig


class FeatureMonitoringConfigEngine:
    """Logic and helper methods to deal with configs from a feature monitoring job.

    Attributes:
        feature_store_id: int. Id of the respective Feature Store.
        feature_group_id: int. Id of the feature group, if monitoring a feature group.
        feature_view_name: str. Name of the feature view, if monitoring a feature view.
        feature_view_version: int. Version of the feature view, if monitoring a feature view.
    """

    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: int | None = None,
        feature_view_name: str | None = None,
        feature_view_version: int | None = None,
        **kwargs,
    ):
        """Business logic for feature monitoring configuration.

        This class encapsulates the business logic for feature monitoring configuration.
        It is responsible for routing methods from the public python API to the
        appropriate REST calls. It should also contain validation and error handling logic
        for payloads or default object. Additionally, it contains logic necessary
        to run the feature monitoring job, including taking a monitoring window configuration
        and fetching the associated data.

        Parameters:
            feature_store_id: ID of the respective Feature Store.
            feature_group_id: ID of the feature group, if monitoring a feature group.
            feature_view_name: Name of the feature view, if monitoring a feature view.
            feature_view_version: Version of the feature view, if monitoring a feature view.
        """
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        self._feature_monitoring_config_api = (
            feature_monitoring_config_api.FeatureMonitoringConfigApi(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._job_api = JobApi()
        self._monitoring_window_config_engine = (
            monitoring_window_config_engine.MonitoringWindowConfigEngine()
        )
        self._result_engine = FeatureMonitoringResultEngine(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )

    # validations

    def _validate_feature_statistics_configs(
        self,
        feature_statistics_configs: list[FeatureStatisticsConfig] | None = None,
        without_stats_configs: bool = False,
        valid_feature_names: list[str] | None = None,
    ):
        if feature_statistics_configs is None:
            return
        if not isinstance(feature_statistics_configs, list):
            raise TypeError(
                "feature_statistics_configs must be a list of dicts or None"
            )
        for fs_config in feature_statistics_configs:
            if (
                without_stats_configs
                and fs_config.statistics_comparison_configs is not None
            ):
                raise AttributeError(
                    "statistics_comparison_config is only available for feature monitoring"
                    " not for scheduled statistics."
                )
            self._validate_feature_statistics_config(
                feature_statistics_config=fs_config,
                valid_feature_names=valid_feature_names,
            )

    def _validate_feature_statistics_config(
        self,
        feature_statistics_config: FeatureStatisticsConfig,
        valid_feature_names: list[str] | None = None,
    ):
        self._validate_feature_name(
            feature_statistics_config.feature_name,
            valid_feature_names=valid_feature_names,
        )
        if feature_statistics_config.statistics_comparison_configs is not None:
            for sc_config in feature_statistics_config.statistics_comparison_configs:
                self._validate_statistics_comparison_config(sc_config)

    def _validate_statistics_comparison_config(
        self, statistics_comparison_config: StatisticsComparisonConfig
    ):
        if not isinstance(statistics_comparison_config.strict, bool):
            raise TypeError("strict must be a boolean value.")

        if statistics_comparison_config.threshold.__class__ not in (int, float):
            raise TypeError("threshold must be a numeric value.")

        if statistics_comparison_config.distribution_metric is not None:
            # Distribution child validation
            if statistics_comparison_config.specific_value is not None:
                raise ValueError(
                    "specific_value is not allowed for distribution comparisons. "
                    "Use a reference window instead."
                )
            return

        # Scalar child validation
        if not isinstance(statistics_comparison_config.relative, bool):
            raise TypeError("relative must be a boolean value.")

        if not isinstance(statistics_comparison_config.metric, str):
            raise TypeError(
                "metric must be a string value. "
                "Check the documentation for a list of supported metrics."
            )
        if (
            statistics_comparison_config.specific_value is not None
            and statistics_comparison_config.specific_value.__class__
            not in (int, float)
        ):
            raise TypeError("specific value must be a numeric value.")

        # TODO: [FSTORE-1205] Add more validation logic based on detection and reference window config.
        self._validate_statistics_metric(statistics_comparison_config.metric)

    def _validate_config_name(self, name: str):
        if not isinstance(name, str):
            raise TypeError("Invalid config name. Config name must be a string.")
        if len(name) > 64:
            raise ValueError(
                "Invalid config name. Config name must be less than 64 characters."
            )
        if not re.match(r"^[\w]*$", name):
            raise ValueError(
                "Invalid config name. Config name must be alphanumeric or underscore."
            )

    def _validate_description(self, description: str | None):
        if description is None:
            return  # noop
        if not isinstance(description, str):
            raise TypeError("Invalid description. Description must be a string.")
        if len(description) > 256:
            raise ValueError(
                "Invalid description. Description must be less than 256 characters."
            )

    def _validate_feature_name(
        self, feature_name: str | None, valid_feature_names: list[str] | None
    ):
        if feature_name is None or valid_feature_names is None:
            return  # noop
        if not isinstance(feature_name, str):
            raise TypeError("Invalid feature name. Feature name must be a string.")
        if feature_name not in valid_feature_names:
            raise ValueError(
                f"Invalid feature name. Feature name must be one of {valid_feature_names}."
            )

    def _validate_statistics_metric(self, metric: str):
        metric_lower = metric.lower()
        if (
            metric_lower not in VALID_CATEGORICAL_METRICS
            and metric_lower not in VALID_FRACTIONAL_METRICS
        ):
            raise ValueError(
                f"Invalid metric {metric_lower}. Supported metrics are {{}}.".format(
                    set(VALID_FRACTIONAL_METRICS).union(set(VALID_CATEGORICAL_METRICS))
                )
            )

    # CRUD

    def _save(self, config: fmc.FeatureMonitoringConfig) -> fmc.FeatureMonitoringConfig:
        """Saves a feature monitoring config.

        Parameters:
            config: FeatureMonitoringConfig, required
                The feature monitoring config to save.

        Returns:
            FeatureMonitoringConfig The saved feature monitoring config.

        Raises:
            FeatureStoreException: If the config is already registered.
        """
        if config._id is not None:
            raise FeatureStoreException(
                "Cannot save a config that is already registered."
                " Please use update() instead."
            )
        if config.trigger_type == fmc.TriggerType.INGESTION:
            raise FeatureStoreException(
                "Cannot create a feature monitoring config with trigger_type='INGESTION'."
                " Ingestion-triggered configs are managed automatically by the backend."
            )
        registered_config = self._feature_monitoring_config_api._create(config)
        if registered_config.id is not None:
            print(
                "Feature monitoring created successfully, explore it at \n"
                + util.get_feature_monitoring_url(
                    feature_store_id=registered_config.feature_store_id,
                    feature_monitoring_config_id=registered_config.id,
                    feature_group_id=registered_config.feature_group_id,
                    feature_view_name=registered_config.feature_view_name,
                    feature_view_version=registered_config.feature_view_version,
                )
            )
        return registered_config

    def _update(
        self, config: fmc.FeatureMonitoringConfig
    ) -> fmc.FeatureMonitoringConfig:
        """Updates a feature monitoring config.

        Parameters:
            config: FeatureMonitoringConfig, required
                The feature monitoring config to update.

        Returns:
            FeatureMonitoringConfig The updated feature monitoring config.

        Raises:
            FeatureStoreException: If the config is not registered.
        """
        if config._id is None:
            raise FeatureStoreException(
                "Cannot update a config that is not registered."
                " Please use save() instead."
            )
        return self._feature_monitoring_config_api._update(config)

    def _delete(self, config_id: int) -> None:
        """Deletes a feature monitoring config.

        Parameters:
            config_id: int, required
                The id of the feature monitoring config to delete.
        """
        self._feature_monitoring_config_api._delete(config_id=config_id)

    def _get_feature_monitoring_configs(
        self,
        name: str | None = None,
        feature_name: str | None = None,
        config_id: int | None = None,
    ) -> fmc.FeatureMonitoringConfig | list[fmc.FeatureMonitoringConfig] | None:
        """Fetch feature monitoring configuration by entity, name or feature name.

        If no arguments are provided, fetches all feature monitoring configurations
        attached to the given entity. If a name is provided, it fetches a single configuration
        and returns None if not found. If a feature name is provided, it fetches all
        configurations attached to that feature (not including those attached to the full
        entity) and returns an empty list if none are found. If a config_id is provided,
        it fetches a single configuration and returns None if not found.

        Parameters:
            name: If provided, fetch only configuration with given name.
            feature_name: If provided, fetch all configurations attached to a specific feature.
            config_id: If provided, fetch only configuration with given id.

        Raises:
            ValueError: If both name and feature_name are provided.
            TypeError: If name or feature_name are not strings.

        Returns:
            The monitoring configuration(s).
        """
        if any(
            [
                name and feature_name,
                feature_name and config_id,
                config_id and name,
            ]
        ):
            raise ValueError("Provide at most one of name, feature_name, or config_id.")

        if name is not None:
            if not isinstance(name, str):
                raise TypeError("name must be a string or None.")
            return self._feature_monitoring_config_api._get_by_name(name=name)
        if feature_name is not None:
            if not isinstance(feature_name, str):
                raise TypeError("feature_name must be a string or None.")
            return self._feature_monitoring_config_api._get_by_feature_name(
                feature_name=feature_name
            )
        if config_id is not None:
            if not isinstance(config_id, int):
                raise TypeError("config_id must be an integer or None.")
            return self._feature_monitoring_config_api._get_by_id(config_id=config_id)

        return self._feature_monitoring_config_api._get_by_entity()

    # operations

    def _trigger_monitoring_job(
        self,
        job_name: str,
    ) -> Job:
        """Make a REST call to start an execution of the monitoring job.

        Parameters:
            job_name: Name of the job to trigger.

        Returns:
            Job object.
        """
        self._job_api.launch(name=job_name)

        return self._job_api.get_job(name=job_name)

    def _get_monitoring_job(
        self,
        job_name: str,
    ) -> Job:
        """Make a REST call to fetch the job entity.

        Parameters:
            job_name: Name of the job to trigger.

        Returns:
            `Job` A Hopsworks job with its metadata and execution history.
        """
        return self._job_api.get_job(name=job_name)

    def _get_latest_fg_commit_time(
        self,
        entity: feature_group.FeatureGroup,
    ) -> int | None:
        """Fetch the commit timestamp of the most recent commit on a feature group.

        Parameters:
            entity: The feature group to query.

        Returns:
            The most recent ``commit_time`` in milliseconds, or ``None`` if the
            feature group has no commits yet.
        """
        try:
            commits = FeatureGroupApi().get_commit_details(
                feature_group_instance=entity,
                wallclock_timestamp=None,
                limit=1,
            )
        except Exception:
            logger.warning(
                "Could not fetch latest commit time for feature group '%s' (id=%s); "
                "falling back to wallclock-anchored detection window.",
                getattr(entity, "name", "<unknown>"),
                getattr(entity, "id", "<unknown>"),
            )
            return None
        return commits[0].commit_time if commits else None

    def _rebuild_reused_feature_statistics_result(
        self,
        prior_fsr: FeatureStatisticsResult,
    ) -> FeatureStatisticsResult:
        """Rebuild a `FeatureStatisticsResult` for the model-monitoring reuse path.

        The prior result was fetched with `expand=statistics`, so the nested FDS
        objects (`detection_statistics`, `reference_statistics`) are populated but
        `detection_statistics_id` / `reference_statistics_id` are not. The create
        endpoint requires the opposite: ids only, no nested objects. Comparison
        results are also rebuilt without ids/parent ids so the backend creates
        fresh `StatisticsComparisonResult` rows.
        """
        detection_id = prior_fsr.detection_statistics_id
        if detection_id is None and prior_fsr.detection_statistics is not None:
            detection_id = prior_fsr.detection_statistics.id
        reference_id = prior_fsr.reference_statistics_id
        if reference_id is None and prior_fsr.reference_statistics is not None:
            reference_id = prior_fsr.reference_statistics.id

        reused_scr = None
        if prior_fsr.statistics_comparison_results is not None:
            reused_scr = [
                StatisticsComparisonResult(
                    statistics_comparison_config_id=scr.statistics_comparison_config_id,
                    shift_detected=scr.shift_detected,
                    difference=scr.difference,
                )
                for scr in prior_fsr.statistics_comparison_results
            ]

        return FeatureStatisticsResult(
            feature_name=prior_fsr.feature_name,
            statistics_comparison_results=reused_scr,
            detection_statistics_id=detection_id,
            reference_statistics_id=reference_id,
            shifted_metric_names=prior_fsr.shifted_metric_names,
        )

    def _run_feature_monitoring(
        self,
        entity: feature_group.FeatureGroup | feature_view.FeatureView,
        config_name: str,
        end_commit_time: int | None = None,
    ) -> FeatureMonitoringResult:
        """Main function used by the job to actually perform the monitoring.

        Parameters:
            entity: Featuregroup or Featureview object containing the feature to monitor.
            config_name: name of the monitoring config.
            end_commit_time: optional commit timestamp in milliseconds that triggered this
                job (ingestion-triggered configs only). When provided, the detection window
                end time is pinned to this commit so the resulting Statistics row records
                the correct ``window_end_commit_time``.

        Returns:
            A list of result object describing the outcome of the monitoring.
        """
        config = self._feature_monitoring_config_api._get_by_name(config_name)

        assert config is not None, "Feature monitoring config not found."
        feature_names = config.get_feature_names()

        if config.trigger_type == fmc.TriggerType.INGESTION:
            # Ingestion-triggered configs: read flags from the FG's statistics_config
            # so that user-configured histograms/correlations/kll/histogram_bins are honoured.
            sc = getattr(entity, "statistics_config", None)
            if sc is not None:
                profile_flags = {
                    "histograms": sc.histograms,
                    "correlations": sc.correlations,
                    "exact_uniqueness": sc.exact_uniqueness,
                    "kll": getattr(sc, "kll", None),
                    "histogram_bins": getattr(sc, "histogram_bins", None),
                }
            else:
                profile_flags = None
        else:
            # Determine if any child comparison is distribution-typed.
            # When yes, we need histograms and KLL to be computed during profiling.
            has_distribution_child = any(
                sc_config.distribution_metric is not None
                for fs_config in (config.feature_statistics_configs or [])
                for sc_config in (fs_config.statistics_comparison_configs or [])
            )
            # `for_distribution_comparison` marks profile runs initiated by a PDF config.
            # The merge-path dispatch in MonitoringWindowConfigEngine requires this flag
            # explicitly so that unrelated kll=True profile runs (e.g. training-dataset
            # stats) cannot accidentally take the merge path, which returns a synthetic
            # FDS lacking scalar fields that scalar-comparison metrics need.
            profile_flags = (
                {
                    "histograms": True,
                    "kll": True,
                    "histogram_bins": None,
                    "for_distribution_comparison": True,
                }
                if has_distribution_child
                else None
            )

        # FSTORE-2050: model monitoring — when both fields are set, the detection window
        # is the FV's logging FG and we must filter to inference rows produced by this
        # exact (model_name, model_version). The same filter applies to the reference
        # window only when the reference is also the same logging FG (i.e. ROLLING_TIME
        # / ALL_TIME) — never for TRAINING_DATASET references.
        model_filter = None
        if config.model_name is not None and config.model_version is not None:
            model_filter = (config.model_name, config.model_version)

        # Idea D — commit-anchored detection window for the model-monitoring path.
        #
        # For model/deployment monitoring on a logging FG (model_filter is set):
        #   1. Anchor end_time to the latest available FG commit so the detection window
        #      ends at the most recent materialized data, not wallclock now().
        #   2. If the latest commit matches the detection_window_commit_time on the most
        #      recent persisted result for this config, no new data has landed since the
        #      last run — persist a new result that reuses the previous statistics without
        #      reading or recomputing anything.
        #
        # The ingestion-triggered path already passes end_commit_time explicitly, so we
        # only activate this logic when it has not been set by the caller.
        from hsfs import feature_group as _fg_mod

        detection_window_commit_time: int | None = None
        # When the logging FG backing a model-monitoring config has no offline data
        # yet, the detection window is empty (see the no-commit branch below).
        detection_window_unmaterialized = False
        if (
            model_filter is not None
            and end_commit_time is None
            and isinstance(entity, _fg_mod.FeatureGroup)
        ):
            latest_commit_time = self._get_latest_fg_commit_time(entity)
            if latest_commit_time is None:
                # No offline commits yet: the logging FG's offline data has not been
                # materialized (the *_offline_fg_materialization job has not run), so
                # there is nothing to read for the detection window. A snapshot read of
                # the offline Delta table would fail with DELTA_TABLE_NOT_FOUND. Treat
                # this as an empty detection window so the run persists a result (which
                # the backend maps to MONITORING_EMPTY_DETECTION_WINDOW) instead of
                # failing the monitoring job.
                logger.warning(
                    "No offline commits for logging feature group '%s' (id=%s) backing "
                    "model-monitoring config '%s'; offline data is not materialized yet. "
                    "Treating the detection window as empty.",
                    getattr(entity, "name", "<unknown>"),
                    getattr(entity, "id", "<unknown>"),
                    config_name,
                )
                detection_window_unmaterialized = True
            else:
                detection_window_commit_time = latest_commit_time
                # Check for a prior result at the same commit — if found, reuse stats.
                assert config.id is not None
                prior_result = self._result_engine._get_latest_by_config_id(config.id)
                if (
                    prior_result is not None
                    and prior_result.detection_window_commit_time is not None
                    and prior_result.detection_window_commit_time == latest_commit_time
                    and prior_result.feature_statistics_results is not None
                ):
                    logger.info(
                        "No new commit since last monitoring run for config '%s' "
                        "(commit_time=%d). Reusing previous statistics.",
                        config_name,
                        latest_commit_time,
                    )
                    # Reuse previous feature statistics — no data read, no recompute.
                    # The prior results were fetched with expand=statistics, so each
                    # FeatureStatisticsResult carries the nested detection/reference FDS
                    # DTOs. The backend's create-validation rejects payloads where those
                    # nested objects are non-null (FeatureMonitoringResultInputValidation:
                    # "Descriptive statistics for the detection window should be
                    # registered prior to the monitoring result."), so rebuild lean
                    # instances that reference the existing FDS rows by id only.
                    reused_fs_results = [
                        self._rebuild_reused_feature_statistics_result(fsr)
                        for fsr in prior_result.feature_statistics_results
                    ]
                    reused_fm_result = (
                        self._result_engine._build_feature_monitoring_result(
                            feature_monitoring_config_id=config.id,
                            feature_statistics_results=reused_fs_results,
                            empty_detection_window=prior_result.empty_detection_window,
                            empty_reference_window=prior_result.empty_reference_window,
                            detection_window_commit_time=latest_commit_time,
                        )
                    )
                    return self._result_engine._save(reused_fm_result)

                # New commit (or first run): pin detection window end to the commit time.
                end_commit_time = latest_commit_time

        # TODO: [FSTORE-1206] Parallelize both single_window_monitoring calls and wait
        if detection_window_unmaterialized:
            # Offline data not materialized yet — skip the read and emit empty stats.
            detection_statistics = [
                FeatureDescriptiveStatistics(feature_name=f, count=0)
                for f in feature_names
            ]
        else:
            try:
                detection_statistics = (
                    self._monitoring_window_config_engine._run_single_window_monitoring(
                        entity=entity,
                        monitoring_window_config=config.detection_window_config,
                        feature_names=feature_names,
                        profile_flags=profile_flags,
                        end_commit_time_override=end_commit_time,
                        model_filter=model_filter,
                    )
                )
            except RestAPIError as e:
                if (
                    e.error_code
                    == RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
                ):
                    logger.warning(
                        "Detection window statistics not found for config '%s'. "
                        "Treating detection window as empty.",
                        config_name,
                    )
                    detection_statistics = [
                        FeatureDescriptiveStatistics(feature_name=f, count=0)
                        for f in feature_names
                    ]
                else:
                    raise

        reference_statistics = None
        if config.reference_window_config is not None:
            # Apply the model filter to the reference window only when it reads from the
            # same entity (logging FG) as the detection window — i.e. time-based windows.
            # For TRAINING_DATASET / other entity-divorced reference window types, the
            # filter is meaningless (the TD has no model_name/model_version columns).
            from hsfs.core import monitoring_window_config as _mwc

            ref_window_type = config.reference_window_config.window_config_type
            ref_model_filter = (
                model_filter
                if model_filter is not None
                and ref_window_type
                in (_mwc.WindowConfigType.ROLLING_TIME, _mwc.WindowConfigType.ALL_TIME)
                else None
            )
            # For rolling/all-time references on the same logging FG, also anchor the
            # reference window end to the latest commit so reference and detection use
            # the same temporal frame of reference. TRAINING_DATASET references are
            # entity-divorced and never receive a commit override.
            ref_commit_time_override = (
                detection_window_commit_time
                if ref_model_filter is not None
                and detection_window_commit_time is not None
                else None
            )
            try:
                reference_statistics = (
                    self._monitoring_window_config_engine._run_single_window_monitoring(
                        entity=entity,
                        monitoring_window_config=config.reference_window_config,
                        feature_names=feature_names,
                        profile_flags=profile_flags,
                        end_commit_time_override=ref_commit_time_override,
                        model_filter=ref_model_filter,
                    )
                )
            except RestAPIError as e:
                if (
                    e.error_code
                    == RestAPIError.FeatureStoreErrorCode.STATISTICS_NOT_FOUND
                ):
                    logger.warning(
                        "Reference window statistics not found for config '%s'. "
                        "Treating reference window as empty.",
                        config_name,
                    )
                    reference_statistics = [
                        FeatureDescriptiveStatistics(feature_name=f, count=0)
                        for f in feature_names
                    ]
                else:
                    raise

        return self._result_engine._run_and_save_statistics_comparison(
            fm_config=config,
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            detection_window_commit_time=detection_window_commit_time,
        )

    # feature-type compatibility helpers

    def is_type_compatible(self, metric: str, feature_type: str) -> bool:
        """Return True if the given metric can be computed for a feature of feature_type.

        For scalar metrics, numeric-only metrics (mean/max/min/sum/std_dev/count) require
        a numeric Hive type. Type-agnostic scalar metrics (completeness/distinctness/etc.)
        are compatible with any primitive.

        For distribution metrics, WASSERSTEIN and KOLMOGOROV_SMIRNOV require numeric types.
        PSI, KL_DIVERGENCE, JS_DIVERGENCE, and HELLINGER are compatible with any primitive.

        Complex types (MAP, ARRAY, STRUCT, UNIONTYPE) are never compatible.
        Phase-2 types (timestamp, date, binary) are out of scope and not compatible.

        Parameters:
            metric: Name of the scalar or distribution metric.
            feature_type: Hive type of the feature.

        Returns:
            True if the metric can be computed for the given feature type.
        """
        from hsfs.feature import Feature

        type_lower = feature_type.lower() if feature_type else ""

        # Complex types have no distribution/stats support.
        for complex_prefix in Feature.COMPLEX_TYPES:
            if type_lower.startswith(complex_prefix.lower()):
                return False

        # Phase-2 types: out of scope.
        if type_lower in PHASE2_TYPES:
            return False

        is_numeric = type_lower in NUMERIC_HIVE_TYPES or type_lower.startswith(
            "decimal"
        )

        metric_upper = metric.upper()

        if metric_upper in ALL_DISTRIBUTION_METRICS:
            if metric_upper in NUMERIC_ONLY_DISTRIBUTION_METRICS:
                return is_numeric
            # PSI, KL_DIVERGENCE, JS_DIVERGENCE, HELLINGER — any primitive
            return True

        # Scalar metric path
        metric_lower = metric.lower()
        if metric_lower in NUMERIC_ONLY_SCALAR_METRICS:
            return is_numeric
        # Type-agnostic scalar metrics (VALID_CATEGORICAL_METRICS)
        return True

    def resolve_compatible_features(
        self, metric: str, valid_features: dict[str, str]
    ) -> list[str]:
        """Return feature names from valid_features that are compatible with the given metric.

        Raises ValueError if no features are compatible.

        Parameters:
            metric: Name of the scalar or distribution metric.
            valid_features: Mapping of feature name to its Hive type.

        Returns:
            The names of the features compatible with the given metric.
        """
        compatible = [
            name
            for name, ftype in valid_features.items()
            if self.is_type_compatible(metric, ftype)
        ]
        if not compatible:
            from hsfs.feature import Feature

            if metric.upper() in ALL_DISTRIBUTION_METRICS:
                if metric.upper() in NUMERIC_ONLY_DISTRIBUTION_METRICS:
                    compatible_types = list(NUMERIC_HIVE_TYPES) + ["decimal"]
                else:
                    compatible_types = (
                        "any primitive (int, float, string, boolean, etc.)"
                    )
            elif metric.lower() in NUMERIC_ONLY_SCALAR_METRICS:
                compatible_types = list(NUMERIC_HIVE_TYPES) + ["decimal"]
            else:
                compatible_types = "any primitive"
            raise ValueError(
                f"No features compatible with metric '{metric}' found. "
                f"Compatible types for '{metric}': {compatible_types}. "
                f"Complex types ({Feature.COMPLEX_TYPES}) and phase-2 types "
                f"({sorted(PHASE2_TYPES)}) are excluded."
            )
        return compatible

    # builders

    def _build_default_scheduled_statistics_config(
        self,
        name: str,
        feature_names: list[str] | None,
        valid_feature_names: list[str] | None,
        start_date_time: str | int | date | datetime | pd.Timestamp | None = None,
        description: str | None = None,
        end_date_time: str | int | date | datetime | pd.Timestamp | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
        valid_features: dict[str, str] | None = None,
    ) -> fmc.FeatureMonitoringConfig:
        """Builds the default scheduled statistics config, default detection window is full snapshot.

        Parameters:
            name: str, required
                Name of the feature monitoring configuration, must be unique for
                the feature view or feature group.
            feature_name: str Compute statistics only for these features.
            valid_feature_names: List[str]
                List of the feature names for the feature view or feature group.
            start_date_time: Union[str, int, date, datetime, pd.Timestamp], optional
                Statistics will start being computed on schedule from that time.
            description: str, optional
                Description of the feature monitoring configuration.
            end_date_time: Union[str, int, date, datetime, pd.Timestamp], optional
                Statistics will stop being computed on schedule from that time.
            cron_expression: str, optional
                cron expression defining the schedule for computing statistics. The expression
                must be in UTC timezone and based on Quartz cron syntax. Default is '0 0 12 ? * * *',
                every day at 12pm UTC.

        Returns:
            A Feature Monitoring Configuration to compute the statistics of a snapshot of all data present in the entity.
        """
        assert feature_names is not None
        assert valid_feature_names is not None

        self._validate_config_name(name)
        self._validate_description(description)

        if feature_names is not None:
            for f_name in feature_names:
                self._validate_feature_name(f_name, valid_feature_names)

        feature_statistics_configs = [
            FeatureStatisticsConfig(feature_name=f_name) for f_name in feature_names
        ]

        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_COMPUTATION,
            name=name,
            description=description,
            feature_statistics_configs=feature_statistics_configs,
            job_schedule={
                "start_date_time": start_date_time or datetime.now(),
                "end_date_time": end_date_time,
                "cron_expression": cron_expression,
                "enabled": True,
            },
            valid_features=valid_features,
        ).with_detection_window()

    def _build_default_feature_monitoring_config(
        self,
        name: str,
        valid_feature_names: list[str] | None,
        start_date_time: str | int | date | datetime | pd.Timestamp | None = None,
        description: str | None = None,
        end_date_time: str | int | date | datetime | pd.Timestamp | None = None,
        cron_expression: str | None = "0 0 12 ? * * *",
        valid_features: dict[str, str] | None = None,
    ) -> fmc.FeatureMonitoringConfig:
        """Builds the default scheduled statistics config, default detection window is full snapshot.

        Parameters:
            name: str, required
                Name of the feature monitoring configuration, must be unique for
                the feature view or feature group.
            valid_feature_names: List[str], optional
                List of the feature names for the feature view or feature group.
            start_date_time: Union[str, int, date, datetime, pd.Timestamp], optional
                Statistics will start being computed on schedule from that time.
            description: str, optional
                Description of the feature monitoring configuration.
            end_date_time: Union[str, int, date, datetime, pd.Timestamp], optional
                Statistics will stop being computed on schedule from that time.
            cron_expression: str, optional
                cron expression defining the schedule for computing statistics. The expression
                must be in UTC timezone and based on Quartz cron syntax. Default is '0 0 12 ? * * *',
                every day at 12pm UTC.

        Returns:
            A Feature Monitoring Configuration to compute the statistics of a snapshot of all data present in the entity.
        """
        assert valid_feature_names is not None

        self._validate_config_name(name)
        self._validate_description(description)

        return fmc.FeatureMonitoringConfig(
            feature_store_id=self._feature_store_id,
            feature_group_id=self._feature_group_id,
            feature_view_name=self._feature_view_name,
            feature_view_version=self._feature_view_version,
            name=name,
            description=description,
            # setting feature_monitoring_type to "STATISTICS_COMPARISON" allows
            # to raise an error if no reference window and comparison config are provided
            feature_monitoring_type=fmc.FeatureMonitoringType.STATISTICS_COMPARISON,
            feature_statistics_configs=[],  # to be appended via the compare_on() method
            job_schedule={
                "start_date_time": start_date_time or datetime.now(),
                "end_date_time": end_date_time,
                "cron_expression": cron_expression,
                "enabled": True,
            },
            valid_feature_names=valid_feature_names,
            valid_features=valid_features,
        ).with_detection_window()
