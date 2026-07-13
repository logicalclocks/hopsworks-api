#
#   Copyright 2020 Logical Clocks AB
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
import warnings
from datetime import date, datetime
from typing import TYPE_CHECKING, TypeVar

from hsfs import decorators, engine, split_statistics, statistics, util
from hsfs.client import exceptions
from hsfs.core import job, statistics_api
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics


logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    import pandas as pd
    from hsfs import feature_group, feature_view, training_dataset


class StatisticsEngine:
    def __init__(self, feature_store_id, entity_type):
        self._statistics_api = statistics_api.StatisticsApi(
            feature_store_id, entity_type
        )

    def _compute_and_save_statistics(
        self,
        metadata_instance,
        feature_dataframe=None,
        feature_group_commit_id=None,
        feature_view_obj=None,
    ) -> statistics.Statistics | job.Job:
        """Compute statistics for a dataframe and send the result json to Hopsworks.

        Parameters:
            metadata_instance: Union[FeatureGroup, TrainingDataset]. Metadata of the entity containing the data.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            feature_group_commit_id: int. Feature group commit id.
            feature_view_obj: FeatureView. Metadata of the feature view, used when computing statistics for a Training Dataset.

        Returns:
            If running on Spark, statistics metadata containing a list of single feature descriptive statistics.
            Otherwise, Spark job metadata used to compute the statistics.
        """
        if (
            engine._get_type().startswith("spark")
            or feature_view_obj is not None
            or (
                all(
                    [
                        feature_group_commit_id is not None,
                        engine._get_type() == "python",
                        feature_dataframe is not None,
                    ]
                )
            )
        ):
            # If the feature dataframe is None, then trigger a read on the metadata instance
            # We do it here to avoid making a useless request when using the Python engine
            # and calling compute_and_save_statistics
            if feature_dataframe is None:
                if feature_group_commit_id is not None:
                    feature_dataframe = (
                        metadata_instance.select_all()
                        .as_of(
                            util._get_hudi_datestr_from_timestamp(
                                feature_group_commit_id
                            )
                        )
                        .read(online=False, dataframe_type="default", read_options={})
                    )
                else:
                    feature_dataframe = metadata_instance.read()

            computation_time = int(float(datetime.now().timestamp()) * 1000)
            stats_str = self._profile_statistics_with_config(
                feature_dataframe, metadata_instance.statistics_config
            )
            desc_stats = self._parse_deequ_statistics(
                stats_str, metadata_instance.statistics_config.exact_uniqueness
            )
            if desc_stats:
                stats = statistics.Statistics(
                    computation_time=computation_time,
                    feature_descriptive_statistics=desc_stats,
                    window_end_commit_time=feature_group_commit_id,
                )
                return self._save_statistics(stats, metadata_instance, feature_view_obj)
        else:
            # Python engine
            return engine._get_instance()._profile_by_spark(metadata_instance)
        return None

    def _compute_and_save_monitoring_statistics(
        self,
        metadata_instance: feature_group.FeatureGroup
        | training_dataset.TrainingDataset,
        feature_dataframe: TypeVar("pyspark.sql.DataFrame") | pd.DataFrame,
        window_start_commit_time: int,
        window_end_commit_time: int,
        row_percentage: float,
        feature_name: str | list[str] | None = None,
        *,
        histograms: bool = False,
        exact_uniqueness: bool = False,
        correlations: bool = False,
        kll: bool = False,
        histogram_bins: int | None = None,
    ) -> statistics.Statistics:
        """Compute statistics for one or more features and send the result to Hopsworks.

        Parameters:
            metadata_instance: Metadata of the entity containing the data.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            window_start_commit_time: Window start commit time
            window_end_commit_time: Window end commit time
            row_percentage: Percentage of rows to include.
            feature_name: Feature name or list of names to compute the statistics on. If not set, statistics are computed on all features.
            histograms: Whether to compute histograms.
            exact_uniqueness: Whether to compute exact uniqueness.
            correlations: Whether to compute feature correlations.
            kll: Whether to compute KLL sketches.
            histogram_bins: Number of bins to use for histograms.

        Returns:
            Statistics metadata containing a list of single feature descriptive statistics.
        """
        feature_names = []
        if feature_name is None:
            feature_names = feature_dataframe.columns
        elif isinstance(feature_name, str):
            feature_names = [feature_name]
        elif isinstance(feature_name, list):
            feature_names = feature_name

        if engine._get_type() == "spark":
            commit_time = int(float(datetime.now().timestamp()) * 1000)

            if self._is_dataframe_empty(feature_dataframe):
                entity_name = getattr(
                    metadata_instance, "name", repr(metadata_instance)
                )
                logger.warning(
                    "Monitoring statistics registration skipped for entity '%s': "
                    "no data in window [%s, %s] for features %s.",
                    entity_name,
                    window_start_commit_time,
                    window_end_commit_time,
                    feature_names,
                )
                empty_fds = [
                    FeatureDescriptiveStatistics(feature_name=f, count=0)
                    for f in feature_names
                ]
                return statistics.Statistics(
                    computation_time=commit_time,
                    row_percentage=row_percentage,
                    feature_descriptive_statistics=empty_fds,
                    window_start_commit_time=window_start_commit_time,
                    window_end_commit_time=window_end_commit_time,
                )

            stats_str = self._profile_statistics(
                feature_dataframe,
                feature_names,
                correlations,
                histograms,
                exact_uniqueness,
                kll=kll,
                histogram_bins=histogram_bins,
            )
            desc_stats = self._parse_deequ_statistics(stats_str, exact_uniqueness)

            stats = statistics.Statistics(
                computation_time=commit_time,
                row_percentage=row_percentage,
                feature_descriptive_statistics=desc_stats,
                window_end_commit_time=window_end_commit_time,
                window_start_commit_time=window_start_commit_time,
            )
            return self._save_statistics(stats, metadata_instance, None)
        # TODO: Only compute statistics with Spark at the moment. This method is expected to be called
        # only through run_feature_monitoring(), which is the entrypoint of the feature monitoring job.
        # Pending work for next sprint is to compute statistics on the Python client as well, as part of
        # the deequ replacement work.
        raise exceptions.FeatureStoreException(
            "Descriptive statistics for feature monitoring cannot be computed with the Python engine."
        )

    @staticmethod
    def _is_dataframe_empty(feature_dataframe) -> bool:
        return len(feature_dataframe.head(1)) == 0

    @staticmethod
    def _profile_statistics_with_config(feature_dataframe, statistics_config) -> str:
        """Compute statistics on a feature DataFrame based on a given configuration.

        Parameters:
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            statistics_config: StatisticsConfig. Configuration for the statistics to be computed.

        Returns:
            str. Serialized features statistics.
        """
        return StatisticsEngine._profile_statistics(
            feature_dataframe,
            statistics_config.columns,
            statistics_config.correlations,
            statistics_config.histograms,
            statistics_config.exact_uniqueness,
            kll=getattr(statistics_config, "kll", False) or False,
            histogram_bins=getattr(statistics_config, "histogram_bins", None),
        )

    @staticmethod
    def _profile_statistics(
        feature_dataframe: TypeVar("pyspark.sql.DataFrame") | pd.DataFrame,
        columns: list[str],
        correlations: bool,
        histograms: bool,
        exact_uniqueness: bool,
        *,
        kll: bool = False,
        histogram_bins: int | None = None,
    ) -> str:
        """Compute statistics on a feature DataFrame.

        Parameters:
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.
            columns: List of feature names to compute the statistics on.
            correlations: Whether to compute correlations or not.
            histograms: Whether to compute histograms or not.
            exact_uniqueness: Whether to compute exact uniqueness values or not.
            kll: Whether to compute KLL sketches (enables percentile estimates).
            histogram_bins: Number of histogram bins. None falls back to the Deequ default (20).

        Returns:
            Serialized features statistics.
        """
        if StatisticsEngine._is_dataframe_empty(feature_dataframe):
            warnings.warn(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group.",
                category=util.StatisticsWarning,
                stacklevel=1,
            )
            # if empty data, set count to 0 and return
            col_stats = [{"column": col_name, "count": 0} for col_name in columns]
            return json.dumps({"columns": col_stats})
        return engine._get_instance()._profile(
            feature_dataframe,
            columns,
            correlations,
            histograms,
            exact_uniqueness,
            kll,
            histogram_bins,
        )

    def _compute_and_save_split_statistics(
        self, td_metadata_instance, feature_view_obj=None, feature_dataframes=None
    ) -> statistics.Statistics:
        """Compute statistics on Training Dataset splits.

        Parameters:
            td_metadata_instance: TrainingDataset. Training Dataset containing the splits.
            feature_view_obj: FeatureView. Metadata of the feature view used to create the Training Dataset. This parameter is optional.
            feature_dataframes: Spark or Pandas DataFrames containing the splits to compute the statistics on.

        Returns:
            Statistics. Statistics metadata containing a list of single feature descriptive statistics.
        """
        statistics_of_splits = []
        for split in td_metadata_instance.splits:
            split_name = split.name
            stats_str = self._profile_statistics_with_config(
                (
                    feature_dataframes.get(split_name)
                    if feature_dataframes
                    else td_metadata_instance.read(split_name)
                ),
                td_metadata_instance.statistics_config,
            )
            desc_stats = self._parse_deequ_statistics(
                stats_str, td_metadata_instance.statistics_config.exact_uniqueness
            )
            statistics_of_splits.append(
                split_statistics.SplitStatistics(
                    name=split_name,
                    feature_descriptive_statistics=desc_stats,
                )
            )
        computation_time = int(float(datetime.now().timestamp()) * 1000)
        stats = statistics.Statistics(
            computation_time=computation_time, split_statistics=statistics_of_splits
        )
        return self._save_statistics(stats, td_metadata_instance, feature_view_obj)

    def _compute_transformation_fn_statistics(
        self,
        td_metadata_instance: training_dataset.TrainingDataset,
        columns: list[str],
        label_encoder_features: list[str],
        feature_dataframe: TypeVar("pyspark.sql.DataFrame")
        | pd.DataFrame
        | None = None,
        feature_view_obj: feature_view.FeatureView | None = None,
    ) -> statistics.Statistics:
        """Compute statistics for transformation functions.

        Parameters:
            td_metadata_instance: Training Dataset containing the splits.
            columns: List of feature names where transformation functions are applied, excluding label encoded features.
            label_encoder_features: List of label encoded feature names.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on. This parameter is optional.
            feature_view_obj: Metadata of the feature view used to create the Training Dataset. This parameter is optional.

        Returns:
            Statistics metadata containing a list of single feature descriptive statistics.
        """
        stats = self._compute_transformation_fn_statistics_no_save(
            columns, label_encoder_features, feature_dataframe
        )
        return self._save_statistics(stats, td_metadata_instance, feature_view_obj)

    def _compute_transformation_fn_statistics_no_save(
        self,
        columns: list[str],
        label_encoder_features: list[str],
        feature_dataframe: TypeVar("pyspark.sql.DataFrame")
        | pd.DataFrame
        | None = None,
    ) -> statistics.Statistics:
        """Compute transformation-function statistics without persisting them.

        Used by the chained transformation fit, which profiles intermediate
        features level by level and persists the full set in a single save at
        the end (so serving retrieves one complete statistics entity rather than
        the most recent partial one).

        Parameters:
            columns: Feature names to compute statistics on, excluding label encoded features.
            label_encoder_features: Label encoded feature names.
            feature_dataframe: Spark or Pandas DataFrame to compute the statistics on.

        Returns:
            Statistics metadata containing the single-feature descriptive statistics.
        """
        computation_time = int(float(datetime.now().timestamp()) * 1000)
        stats_str = self._profile_transformation_fn_statistics(
            feature_dataframe, columns, label_encoder_features
        )
        # _profile_transformation_fn_statistics profiles with exact_uniqueness=False
        desc_stats = self._parse_deequ_statistics(stats_str, False)
        return statistics.Statistics(
            computation_time=computation_time,
            feature_descriptive_statistics=desc_stats,
            before_transformation=True,
        )

    def _save_transformation_fn_statistics(
        self,
        feature_descriptive_statistics: list[FeatureDescriptiveStatistics],
        td_metadata_instance: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
    ) -> statistics.Statistics:
        """Persist precomputed transformation-function statistics in a single save.

        Used by the chained transformation fit: the statistics were profiled
        stage by stage while the feature values were the fitted ones, so they
        are persisted as computed instead of being re-profiled on the
        transformed dataframe.

        Parameters:
            feature_descriptive_statistics: The single-feature descriptive statistics to persist.
            td_metadata_instance: Training Dataset the statistics belong to.
            feature_view_obj: Metadata of the feature view used to create the Training Dataset.

        Returns:
            Statistics metadata containing the persisted descriptive statistics.
        """
        computation_time = int(float(datetime.now().timestamp()) * 1000)
        return self._save_statistics(
            statistics.Statistics(
                computation_time=computation_time,
                feature_descriptive_statistics=feature_descriptive_statistics,
                before_transformation=True,
            ),
            td_metadata_instance,
            feature_view_obj,
        )

    @decorators._catch_not_found("hsfs.statistics.Statistics", fallback_return=None)
    def _get(
        self,
        metadata_instance: feature_group.FeatureGroup
        | training_dataset.TrainingDataset,
        feature_names: list[str] | None = None,
        computation_time: str | float | datetime | date | None = None,
        before_transformation: bool | None = None,
        training_dataset_version: int | None = None,
    ) -> statistics.Statistics | None:
        """Get statistics of an entity computed at a specific time.

        If the computation time is not provided, the most recently computed statistics will be retrieved.

        Parameters:
            metadata_instance: Metadata of the entity containing the data.
            feature_names: List of feature names of which statistics are retrieved.
            computation_time: Timestamp or computation time when statistics where computed.
            before_transformation: Whether the statistics were computed before transformation functions or not.
            training_dataset_version: Version of the training dataset on which statistics were computed.

        Returns:
            Statistics metadata containing a list of single feature descriptive statistics.
        """
        computation_timestamp = util._convert_event_time_to_timestamp(computation_time)
        return self._statistics_api._get(
            metadata_instance,
            feature_names=feature_names,
            computation_time=computation_timestamp,
            before_transformation=before_transformation,
            training_dataset_version=training_dataset_version,
        )

    @decorators._catch_not_found("hsfs.statistics.Statistics", fallback_return=None)
    def _get_all(
        self,
        metadata_instance: feature_group.FeatureGroup
        | training_dataset.TrainingDataset,
        feature_names: list[str] | None = None,
        computation_time: str | float | datetime | date | None = None,
        training_dataset_version: int | None = None,
    ) -> list[statistics.Statistics] | None:
        """Get all statistics of an entity computed before a specific time.

        If the computation time is not provided, all the statistics will be retrieved.

        Parameters:
            metadata_instance: Metadata of the entity containing the data.
            feature_names: List of feature names of which statistics are retrieved.
            computation_time: Timestamp or computation time when statistics where computed.
            training_dataset_version: Version of the training dataset on which statistics were computed.

        Returns:
            Statistics metadata containing a list of single feature descriptive statistics.
        """
        return self._statistics_api._get_all(
            metadata_instance,
            feature_names=feature_names,
            computation_time=computation_time,
            training_dataset_version=training_dataset_version,
        )

    @decorators._catch_not_found(
        "hsfs.statistics.Statistics",
        "hsfs.feature_group_commit.FeatureGroupCommit",
        fallback_return=None,
    )
    def _get_by_time_window(
        self,
        metadata_instance,
        start_commit_time: str | int | datetime | date | None = None,
        end_commit_time: str | int | datetime | date | None = None,
        feature_names: list[str] | None = None,
        row_percentage: float | None = None,
    ) -> statistics.Statistics | None:
        """Get the statistics of an entity based on a commit time window.

        Parameters:
            metadata_instance: Metadata of the entity containing the data.
            start_commit_time: Window start commit time
            end_commit_time: Window end commit time
            feature_names: List of feature names of which statistics are retrieved.
            row_percentage: Percentage of feature values used during statistics computation

        Returns:
            Statistics metadata containing a list of single feature descriptive statistics.
        """
        start_commit_time = util._convert_event_time_to_timestamp(start_commit_time)
        end_commit_time = util._convert_event_time_to_timestamp(end_commit_time)
        return self._statistics_api._get(
            metadata_instance,
            start_commit_time=start_commit_time,
            end_commit_time=end_commit_time,
            feature_names=feature_names,
            row_percentage=row_percentage,
        )

    @decorators._catch_not_found(
        "hsfs.statistics.Statistics",
        "hsfs.feature_group_commit.FeatureGroupCommit",
        fallback_return=None,
    )
    def _get_all_in_time_window(
        self,
        metadata_instance,
        start_commit_time: int | None = None,
        end_commit_time: int | None = None,
        feature_names: list[str] | None = None,
        limit: int | None = None,
    ) -> list[statistics.Statistics] | None:
        """Get all per-batch statistics rows within a commit time window, with content.

        Used by the KLL-merge path to enumerate individual commit statistics.

        Parameters:
            metadata_instance: Metadata of the entity.
            start_commit_time: Window start commit time (ms timestamp).
            end_commit_time: Window end commit time (ms timestamp).
            feature_names: Feature names to filter.
            limit: Maximum rows to fetch. Defenses against unbounded `with_content=True`
                responses on long windows.

        Returns:
            List of Statistics objects (each containing feature_descriptive_statistics),
            or None if not found.
        """
        return self._statistics_api._get_all_in_window(
            metadata_instance,
            start_commit_time=start_commit_time,
            end_commit_time=end_commit_time,
            feature_names=feature_names,
            limit=limit,
        )

    def _profile_transformation_fn_statistics(
        self, feature_dataframe, columns, label_encoder_features
    ) -> str:
        if (
            engine._get_type() == "spark"
            and len(feature_dataframe.select(*columns).head(1)) == 0
        ) or (engine._get_type() == "python" and len(feature_dataframe.head()) == 0):
            raise exceptions.FeatureStoreException(
                "There is no data in the entity that you are trying to compute "
                "statistics for. A possible cause might be that you inserted only data "
                "to the online storage of a feature group."
            )

        # compute statistics for all features with transformation fn
        all_columns = (columns or []) + (label_encoder_features or [])
        stats_str = engine._get_instance()._profile(
            feature_dataframe, all_columns, False, True, False
        )

        # add unique values profile to column stats
        return self._profile_unique_values(
            feature_dataframe, label_encoder_features, stats_str
        )

    def _profile_unique_values(
        self, feature_dataframe, label_encoder_features, stats_str
    ) -> str:
        stats = json.loads(stats_str)
        if not stats:
            stats = {"columns": []}
        stats_dict = {col_stats["column"]: col_stats for col_stats in stats["columns"]}
        for column in label_encoder_features:
            col_stats_unique_values = {
                "column": column,
                "unique_values": list(
                    engine._get_instance()._get_unique_values(feature_dataframe, column)
                ),
            }
            if column in stats_dict:
                stats_dict[column].update(col_stats_unique_values)
            else:
                stats_dict[column] = col_stats_unique_values

        stats["columns"] = list(stats_dict.values())
        return json.dumps(stats)  # the result is a JSON string

    def _save_statistics(
        self, stats, metadata_instance, feature_view_obj
    ) -> statistics.Statistics:
        # metadata_instance can be feature group or training dataset
        if feature_view_obj:
            stats = self._statistics_api._post(
                feature_view_obj,
                stats=stats,
                training_dataset_version=metadata_instance.version,
            )
        else:
            stats = self._statistics_api._post(
                metadata_instance, stats=stats, training_dataset_version=None
            )
        return stats

    def _parse_deequ_statistics(
        self, stats, exact_uniqueness: bool
    ) -> list[FeatureDescriptiveStatistics] | None:
        if stats is None:
            warnings.warn(
                "There is no Deequ statistics to deserialize. A possible cause might be that Deequ did not succeed in the statistics computation.",
                category=util.StatisticsWarning,
                stacklevel=1,
            )
            return None
        if isinstance(stats, str):
            stats = json.loads(stats)
        if not exact_uniqueness:
            # The JVM deequ profiler emits uniqueness-family metrics even when
            # the Uniqueness analyzer was not requested; drop them so they stay
            # None instead of a spurious 0.0.
            for col_stats in stats["columns"]:
                for key in (
                    "uniqueness",
                    "distinctness",
                    "entropy",
                    "exactNumDistinctValues",
                ):
                    col_stats.pop(key, None)
        return [
            FeatureDescriptiveStatistics._from_deequ_json(col_stats)
            for col_stats in stats["columns"]
        ]
