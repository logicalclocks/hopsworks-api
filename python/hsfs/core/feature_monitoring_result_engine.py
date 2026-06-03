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

from datetime import date, datetime
from typing import TYPE_CHECKING

from hsfs import util
from hsfs.core import distribution_distance
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core.distribution_engine import (
    _WINDOW_DETECTION,
    _WINDOW_REFERENCE,
    DistributionEngine,
)
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.feature_monitoring_config_api import FeatureMonitoringConfigApi
from hsfs.core.feature_monitoring_result import FeatureMonitoringResult
from hsfs.core.feature_monitoring_result_api import FeatureMonitoringResultApi
from hsfs.core.feature_statistics_result import FeatureStatisticsResult
from hsfs.core.job_api import JobApi
from hsfs.core.statistics_comparison_result import StatisticsComparisonResult


if TYPE_CHECKING:
    from hsfs.core.feature_statistics_config import FeatureStatisticsConfig
    from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig


class FeatureMonitoringResultEngine:
    """Logic and helper methods to deal with results from a feature monitoring job.

    Parameters:
        feature_store_id: ID of the respective Feature Store.
        feature_group_id: ID of the feature group, if monitoring a feature group.
        feature_view_name: Name of the feature view, if monitoring a feature view.
        feature_view_version: Version of the feature view, if monitoring a feature view.
    """

    def __init__(
        self,
        feature_store_id: int,
        feature_group_id: int | None = None,
        feature_view_name: str | None = None,
        feature_view_version: int | None = None,
        **kwargs,
    ):
        if feature_group_id is None:
            assert feature_view_name is not None
            assert feature_view_version is not None

        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        self._feature_monitoring_result_api = FeatureMonitoringResultApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )
        self._feature_monitoring_config_api = FeatureMonitoringConfigApi(
            feature_store_id=feature_store_id,
            feature_group_id=feature_group_id,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
        )
        self._job_api = JobApi()
        self._distribution_engine = DistributionEngine()

    # CRUD

    def _save(
        self,
        result: FeatureMonitoringResult,
    ) -> FeatureMonitoringResult:
        """Save feature monitoring result.

        Parameters:
            result: Feature monitoring result to be saved.

        Returns:
            Saved Feature monitoring result.
        """
        return self._feature_monitoring_result_api._create(
            result,
        )

    def _save_with_exception(
        self,
        feature_monitoring_config_id: int,
        job_name: str,
    ) -> FeatureMonitoringResult:
        """Save feature monitoring result with raised_exception flag.

        Parameters:
            feature_monitoring_config_id: int. Id of the feature monitoring configuration.
            job_name: str. Name of the monitoring job.

        Returns:
            FeatureMonitoringResult. Saved Feature monitoring result.
        """
        return self._save(
            result=self._build_feature_monitoring_result(
                feature_monitoring_config_id=feature_monitoring_config_id,
                feature_statistics_results=None,
                job_name=job_name,
                raised_exception=True,
            ),
        )

    def _get_feature_monitoring_results(
        self,
        config_id: int | None = None,
        config_name: str | None = None,
        start_time: str | int | datetime | date | None = None,
        end_time: str | int | datetime | date | None = None,
        with_statistics: bool = True,
    ) -> list[FeatureMonitoringResult]:
        """Convenience method to fetch feature monitoring results from an entity.

        Parameters:
            config_id:
                ID of the feature monitoring configuration.
                Defaults to `None` if config_name is provided.
            config_name:
                Name of the feature monitoring configuration.
                Defaults to `None` if config_id is provided.
            start_time: Query results with monitoring time greater than or equal to start_time.
            end_time: Query results with monitoring time less than or equal to end_time.
            with_statistics:
                Whether to include the statistics attached to the results.
                Set to `False` to fetch only monitoring metadata.

        Returns:
            List of feature monitoring results.
        """
        if all([config_id is None, config_name is None]):
            raise ValueError(
                "Either config_id or config_name must be provided to fetch feature monitoring results."
            )
        if all([config_id is not None, config_name is not None]):
            raise ValueError(
                "Only one of config_id or config_name can be provided to fetch feature monitoring results."
            )
        if config_name is not None and isinstance(config_name, str):
            config = self._feature_monitoring_config_api._get_by_name(config_name)
            if not isinstance(config, fmc.FeatureMonitoringConfig):
                return []
            config_id = config._id
        elif config_name is not None:
            raise TypeError(
                f"config_name must be of type str. Got {type(config_name)}."
            )
        elif config_id is not None and not isinstance(config_id, int):
            raise TypeError(f"config_id must be of type int. Got {type(config_id)}.")

        return self._fetch_all_feature_monitoring_results_by_config_id(
            config_id=config_id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    def _get_latest_by_config_id(
        self,
        config_id: int,
    ) -> FeatureMonitoringResult | None:
        """Fetch the most recent feature monitoring result for a given config.

        Parameters:
            config_id: ID of the feature monitoring configuration.

        Returns:
            The most recent ``FeatureMonitoringResult``, or ``None`` if none exist.
        """
        return self._feature_monitoring_result_api._get_latest_by_config_id(
            config_id=config_id,
        )

    def _fetch_all_feature_monitoring_results_by_config_id(
        self,
        config_id: int,
        start_time: str | int | datetime | date | None = None,
        end_time: str | int | datetime | date | None = None,
        with_statistics: bool = False,
    ) -> list[FeatureMonitoringResult]:
        """Fetch all feature monitoring results by config id.

        Parameters:
            config_id: int. Id of the feature monitoring configuration.
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.
            with_statistics: bool.
                Whether to include the statistics attached to the results or not

        Returns:
            List[FeatureMonitoringResult]. List of feature monitoring results.
        """
        query_params = self._build_query_params(
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

        return self._feature_monitoring_result_api._get_by_config_id(
            config_id=config_id,
            query_params=query_params,
        )

    # operations

    def _run_and_save_statistics_comparison(
        self,
        fm_config: fmc.FeatureMonitoringConfig,
        detection_statistics: list[FeatureDescriptiveStatistics],
        reference_statistics: list[FeatureDescriptiveStatistics] | None = None,
        detection_window_commit_time: int | None = None,
    ) -> FeatureMonitoringResult:
        """Run and upload statistics comparison between detection and reference stats.

        Parameters:
            fm_config: FeatureMonitoringConfig. Feature monitoring configuration.
            detection_statistics: List[FeatureDescriptiveStatistics]. Computed statistics from detection data.
            reference_statistics: Optional[List[FeatureDescriptiveStatistics]]]. Computed statistics from reference data.
            detection_window_commit_time: int or None.
                Commit timestamp (ms) to which the detection window was anchored.
                Passed through to the persisted result for use by the reuse-without-recompute
                guard on subsequent runs (model-monitoring / logging-FG path only).

        Returns:
            Feature monitoring result.
        """
        # Clear the distribution engine's per-run cache so that a reused
        # FeatureMonitoringResultEngine instance doesn't carry state across runs.
        self._distribution_engine.clear_cache()

        # validate fds
        self._validate_detection_and_reference_statistics(
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
        )

        # create fds dicts
        detection_stats_dict, empty_detection_window = {}, False
        reference_stats_dict, empty_reference_window = None, None
        for det_fds in detection_statistics:
            detection_stats_dict[det_fds.feature_name] = det_fds
            if self._is_monitoring_window_empty(det_fds):
                empty_detection_window = True
        if reference_statistics is not None:
            reference_stats_dict, empty_reference_window = {}, False
            for ref_fds in reference_statistics:
                reference_stats_dict[ref_fds.feature_name] = ref_fds
                if self._is_monitoring_window_empty(ref_fds):
                    empty_reference_window = True

        # Build fs_results only for FSCs whose feature was actually profiled.
        # The JVM ColumnProfiler skips unprofilable types (timestamp/date/binary/complex), and
        # schema evolution may add FG features after the FSCs were materialised — so FSCs can
        # legitimately exceed the computed stats. Missing features are silently skipped here;
        # the backend result validator accepts a subset of expected features.
        fs_results = []
        for fs_config in fm_config.feature_statistics_configs:
            det_fds = detection_stats_dict.get(fs_config.feature_name)
            if det_fds is None:
                continue
            ref_fds = (
                reference_stats_dict.get(fs_config.feature_name)
                if reference_stats_dict is not None
                else None
            )
            fs_result = self._run_feature_statistics_comparisons(
                fs_config,
                det_fds,
                ref_fds,
            )
            fs_results.append(fs_result)

        # build fm result
        assert fm_config.id is not None
        fm_result = self._build_feature_monitoring_result(
            feature_monitoring_config_id=fm_config.id,
            feature_statistics_results=fs_results,
            empty_detection_window=empty_detection_window,
            empty_reference_window=empty_reference_window,
            detection_window_commit_time=detection_window_commit_time,
        )

        # save and return
        return self._save(fm_result)

    def _validate_detection_and_reference_statistics(
        self,
        detection_statistics: list[FeatureDescriptiveStatistics],
        reference_statistics: list[FeatureDescriptiveStatistics] | None,
    ):
        if reference_statistics is None:
            return

        mismatch_msg = "Detection feature statistics must contain a reference feature statistics for the same feature and feature type."
        assert len(reference_statistics) >= len(detection_statistics), mismatch_msg

        det_stats_feat_names = {fds.feature_name for fds in detection_statistics}
        ref_stats_feat_names = {fds.feature_name for fds in reference_statistics}
        det_stats_set = {
            (fds.feature_name, fds.feature_type) for fds in detection_statistics
        }
        ref_stats_set = {
            (fds.feature_name, fds.feature_type) for fds in reference_statistics
        }

        assert det_stats_feat_names.issubset(ref_stats_feat_names), mismatch_msg

        # check if detection and reference statistics contain the same features and feature types.
        # statistics computed on empty data will have feature type None, which can falsify the equality.
        # we shouldn't raise an exception in that case, so we just ignore it by checking the count statistic.

        assert (
            det_stats_set.issubset(ref_stats_set)
            or detection_statistics[0].count == 0
            or reference_statistics[0].count == 0
        ), mismatch_msg

    def _run_feature_statistics_comparisons(
        self,
        fs_config: FeatureStatisticsConfig,
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: FeatureDescriptiveStatistics | None,
    ) -> FeatureStatisticsResult:
        sc_results = None
        shifted_metric_names = set()
        if fs_config.statistics_comparison_configs is not None:
            sc_results = []
            for sc_config in fs_config.statistics_comparison_configs:
                difference, shift_detected = self._compute_difference_and_shift(
                    sc_config=sc_config,
                    detection_statistics=detection_statistics,
                    reference_statistics=reference_statistics,
                )
                assert sc_config.id is not None
                if difference is not None:
                    scr = StatisticsComparisonResult(
                        sc_config.id,
                        shift_detected=shift_detected,
                        difference=difference,
                    )
                    sc_results.append(scr)
                    if shift_detected:
                        shifted_metric_names.add(
                            sc_config.metric or sc_config.distribution_metric
                        )

        return self._build_feature_statistics_result(
            feature_name=fs_config.feature_name,
            detection_statistics=detection_statistics,
            reference_statistics=reference_statistics,
            statistics_comparison_results=sc_results if sc_results else None,
            shifted_metric_names=shifted_metric_names,
        )

    def _compute_difference_and_shift(
        self,
        sc_config: StatisticsComparisonConfig,
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: FeatureDescriptiveStatistics | None = None,
    ) -> tuple[float | None, bool]:
        """Compute the difference and detect shift between the reference and detection statistics.

        Dispatches on sc_config.distribution_metric:
          - When set: computes a distribution distance (PSI, KL, etc.) via DistributionEngine
            and distribution_distance.compute().
          - When None: falls back to the existing scalar metric path.

        Parameters:
            sc_config: Statistics comparison configuration (scalar or distribution child).
            detection_statistics: Computed statistics from detection data.
            reference_statistics: Computed statistics from reference data.

        Returns:
            The difference between the reference and detection statistics, and whether shift was detected or not.
        """
        if sc_config.distribution_metric is not None:
            difference = self._compute_distribution_distance(
                sc_config=sc_config,
                detection_statistics=detection_statistics,
                reference_statistics=reference_statistics,
            )
        else:
            difference = self._compute_difference_between_stats(
                detection_statistics=detection_statistics,
                reference_statistics=reference_statistics,
                metric=sc_config.metric,
                relative=sc_config.relative,
                specific_value=sc_config.specific_value,
            )

        if difference is None or sc_config.threshold is None:
            # if no difference can be computed or threshold not provided, no shift detected
            return difference, False

        if sc_config.strict:
            shift_detected = difference > sc_config.threshold
        else:
            shift_detected = difference >= sc_config.threshold
        return difference, shift_detected

    def _compute_distribution_distance(
        self,
        sc_config: StatisticsComparisonConfig,
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: FeatureDescriptiveStatistics | None,
    ) -> float | None:
        """Compute a distribution distance metric between detection and reference windows.

        Returns None if either window is empty.
        """
        import numpy as np

        if reference_statistics is None:
            return None
        if self._is_monitoring_window_empty(detection_statistics):
            return None
        if self._is_monitoring_window_empty(reference_statistics):
            return None

        binning_strategy = sc_config.binning_strategy or _default_binning_strategy(
            reference_statistics
        )
        bin_count = sc_config.bin_count or 10
        epsilon = sc_config.smoothing_epsilon or 1e-6

        bin_edges = self._distribution_engine.resolve_bin_edges(
            reference_fds=reference_statistics,
            detection_fds=detection_statistics,
            binning_strategy=binning_strategy,
            bin_count=bin_count,
            custom_edges=sc_config.custom_bin_edges,
        )

        ref_probs = self._distribution_engine.build_distribution(
            fds=reference_statistics,
            binning_strategy=binning_strategy,
            bin_edges=bin_edges,
            epsilon=epsilon,
            window_id=_WINDOW_REFERENCE,
        )
        det_probs = self._distribution_engine.build_distribution(
            fds=detection_statistics,
            binning_strategy=binning_strategy,
            bin_edges=bin_edges,
            epsilon=epsilon,
            window_id=_WINDOW_DETECTION,
        )

        # bin_centres is required for WASSERSTEIN; only meaningful for numeric strategies.
        bin_centres = None
        if sc_config.distribution_metric == "WASSERSTEIN" and isinstance(
            bin_edges[0], (int, float)
        ):
            edges_array = np.asarray(bin_edges, dtype=np.float64)
            bin_centres = (edges_array[:-1] + edges_array[1:]) / 2.0

        return distribution_distance.compute(
            det_probs=det_probs,
            ref_probs=ref_probs,
            metric=sc_config.distribution_metric,
            bin_centres=bin_centres,
        )

    def _compute_difference_between_stats(
        self,
        detection_statistics: FeatureDescriptiveStatistics,
        metric: str,
        relative: bool = False,
        reference_statistics: FeatureDescriptiveStatistics | None = None,
        specific_value: float | None = None,
    ) -> float | None:
        """Compute the difference between the reference and detection statistics.

        Parameters:
            detection_statistics: Computed statistics from detection data.
            metric: The metric to compute the difference for.
            relative: Whether to compute the relative difference or not.
            reference_statistics: Computed statistics from reference data, or a specific value to use as reference.
            specific_value: A specific value to use as reference.

        Returns:
            `Optional[float]`. The difference between the reference and detection statistics, or None if there
                               are no values to compare
        """
        if reference_statistics is None and specific_value is None:
            return None  # no difference can be computed

        metric_lower = metric.lower()  # ensure lower case

        if metric_lower != "count":
            # on empty data, only the count metric can be used to compute the difference
            if self._is_monitoring_window_empty(detection_statistics):
                # if det stats on empty data, no difference can be computed
                return None
            if specific_value is None and self._is_monitoring_window_empty(
                reference_statistics
            ):
                # if ref stats on empty data, no difference can be computed unless a specific value is provided
                return None

        # otherwise, both detection and reference value can be obtained
        detection_value = detection_statistics.get_value(metric_lower)
        reference_value = (
            specific_value
            if specific_value is not None
            else reference_statistics.get_value(metric_lower)
        )
        return self._compute_difference_between_specific_values(
            detection_value, reference_value, relative
        )

    def _compute_difference_between_specific_values(
        self,
        detection_value: float,
        reference_value: float,
        relative: bool = False,
    ) -> float:
        """Compute the difference between a reference and detection value.

        Parameters:
            detection_value: The detection value.
            reference_value: The reference value
            relative: Whether to compute the relative difference or not.

        Returns:
            The difference between the reference and detection values.
        """
        diff = abs(detection_value - reference_value)
        if relative:
            if reference_value == 0:
                return float("inf")
            return diff / reference_value
        return diff

    # builders

    def _build_feature_monitoring_result(
        self,
        feature_monitoring_config_id: int,
        feature_statistics_results: list[FeatureStatisticsResult] | None,
        empty_detection_window: bool = False,
        empty_reference_window: bool | None = False,
        raised_exception: bool = False,
        execution_id: int | None = None,
        job_name: str | None = None,
        detection_window_commit_time: int | None = None,
    ) -> FeatureMonitoringResult:
        """Build feature monitoring result.

        Parameters:
            feature_monitoring_config_id: int. Id of the feature monitoring configuration.
            feature_statistics_results: list of FeatureStatisticsResult or None. Statistics results per feature.
            empty_detection_window: bool. Whether the detection window is empty.
            empty_reference_window: bool or None. Whether the reference window is empty.
            raised_exception: bool. Whether an exception was raised during monitoring.
            execution_id: int or None. Id of the job execution.
            job_name: str or None. Name of the monitoring job.
            detection_window_commit_time: int or None.
                Commit timestamp (ms) to which the detection window end was anchored.
                Set only for model-monitoring configs on logging feature groups.

        Returns:
            FeatureMonitoringResult. Saved Feature monitoring result.
        """
        monitoring_time = round(
            util._convert_event_time_to_timestamp(datetime.now()), -3
        )
        if execution_id is None and job_name is not None:
            execution_id = self._get_monitoring_job_execution_id(job_name)
        else:
            execution_id = 0

        # get shifted features
        shifted_feature_names = set()
        if feature_statistics_results is not None:
            for fsr in feature_statistics_results:
                if fsr.shifted_metric_names:
                    shifted_feature_names.add(fsr.feature_name)

        return FeatureMonitoringResult(
            feature_store_id=self._feature_store_id,
            feature_monitoring_config_id=feature_monitoring_config_id,
            execution_id=execution_id,
            feature_statistics_results=feature_statistics_results,
            monitoring_time=monitoring_time,
            raised_exception=raised_exception,
            shifted_feature_names=shifted_feature_names,
            empty_detection_window=empty_detection_window,
            empty_reference_window=empty_reference_window,
            detection_window_commit_time=detection_window_commit_time,
        )

    def _build_feature_statistics_result(
        self,
        feature_name: str,
        detection_statistics: FeatureDescriptiveStatistics,
        reference_statistics: FeatureDescriptiveStatistics | None,
        statistics_comparison_results: list[StatisticsComparisonResult] | None = None,
        shifted_metric_names: set | None = None,
    ) -> FeatureStatisticsResult:
        detection_statistics_id = detection_statistics.id
        reference_statistics_id = (
            reference_statistics.id
            if isinstance(reference_statistics, FeatureDescriptiveStatistics)
            else None
        )

        return FeatureStatisticsResult(
            feature_name=feature_name,
            statistics_comparison_results=statistics_comparison_results,
            detection_statistics_id=detection_statistics_id,
            reference_statistics_id=reference_statistics_id,
            shifted_metric_names=shifted_metric_names,
        )

    def _build_query_params(
        self,
        start_time: str | int | datetime | date | None,
        end_time: str | int | datetime | date | None,
        with_statistics: bool,
    ) -> dict[str, str | list[str]]:
        """Build query parameters for feature monitoring result API calls.

        Parameters:
            start_time: Union[str, int, datetime, date, None].
                Query results with monitoring time greater than or equal to start_time.
            end_time: Union[str, int, datetime, date, None].
                Query results with monitoring time less than or equal to end_time.
            with_statistics: bool.
                Whether to include the statistics attached to the results or not

        Returns:
            Dict[str, Union[str, List[str]]]. Query parameters.
        """
        query_params = {"sort_by": "monitoring_time:desc"}

        filter_by = []
        if start_time:
            timestamp_start_time = util._convert_event_time_to_timestamp(start_time)
            filter_by.append(f"monitoring_time_gte:{timestamp_start_time}")
        if end_time:
            timestamp_end_time = util._convert_event_time_to_timestamp(end_time)
            filter_by.append(f"monitoring_time_lte:{timestamp_end_time}")
        if len(filter_by) > 0:
            query_params["filter_by"] = filter_by

        if with_statistics:
            query_params["expand"] = "statistics"

        return query_params

    def _get_monitoring_job_execution_id(
        self,
        job_name: str,
    ) -> int:
        """Get the execution id of the last execution of the monitoring job.

        The last execution is assumed to be the current execution.
        The id defaults to 0 if no execution is found.

        Parameters:
            job_name: Name of the monitoring job.

        Returns:
            Id of the last execution of the monitoring job.
            It is assumed to be the current execution.
        """
        execution = self._job_api.last_execution(self._job_api.get(name=job_name))
        return (
            execution[0]._id
            if isinstance(execution, list) and len(execution) > 0
            else 0
        )

    def _is_monitoring_window_empty(
        self,
        monitoring_window_statistics: FeatureDescriptiveStatistics | None = None,
    ) -> bool | None:
        """Check if the monitoring window is empty.

        Parameters:
            monitoring_window_statistics: Statistics computed for the monitoring window.

        Returns:
            Whether the monitoring window is empty or not.
        """
        if monitoring_window_statistics is None:
            return None
        return monitoring_window_statistics.count == 0


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _default_binning_strategy(fds: FeatureDescriptiveStatistics) -> str:
    """Return the default binning strategy for the given feature statistics.

    Uses CATEGORICAL for non-numeric features (string, boolean, etc.)
    and EQUI_FREQUENCY for numeric ones.
    """
    feature_type = (fds.feature_type or "").lower()
    numeric_types = {
        "integral",
        "fractional",
        "int",
        "bigint",
        "tinyint",
        "smallint",
        "float",
        "double",
    }
    # Deequ reports "Integral" / "Fractional"; Hive uses "int" / "double" etc.
    if feature_type in numeric_types or feature_type.startswith("decimal"):
        return "EQUI_FREQUENCY"
    return "CATEGORICAL"
