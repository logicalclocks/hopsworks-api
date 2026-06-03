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

import json
from enum import Enum
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.constants import FEATURES
from hsfs import util
from hsfs.core import (
    feature_monitoring_config_engine,
    feature_monitoring_result_engine,
    job_api,
    monitoring_window_config_engine,
)
from hsfs.core import monitoring_window_config as mwc
from hsfs.core.feature_statistics_config import FeatureStatisticsConfig
from hsfs.core.job_schedule import JobSchedule
from hsfs.core.statistics_comparison_config import StatisticsComparisonConfig


if TYPE_CHECKING:
    import builtins
    from datetime import date, datetime

    from hopsworks_common.job import Job
    from hsfs.core.feature_monitoring_result import FeatureMonitoringResult


MAX_LENGTH_DESCRIPTION = 2000


class FeatureMonitoringType(str, Enum):
    STATISTICS_COMPUTATION = "STATISTICS_COMPUTATION"  # stats computation on a schedule
    STATISTICS_COMPARISON = "STATISTICS_COMPARISON"  # stats computation and comparison
    DISTRIBUTION_COMPARISON = "DISTRIBUTION_COMPARISON"  # data distributions

    @classmethod
    def _list_str(cls) -> builtins.list[str]:
        return [c.value for c in cls]

    @classmethod
    def _list(cls) -> builtins.list[FeatureMonitoringType]:
        return list(cls)

    @classmethod
    def _from_str(cls, value: str) -> FeatureMonitoringType:
        if value in cls._list_str():
            return cls(value)
        raise ValueError(
            f"Invalid value {value} for FeatureMonitoringType, allowed values are {cls._list_str()}"
        )

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, self.__class__):
            return self is other
        return False

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


class TriggerType(str, Enum):
    CRON = "CRON"
    INGESTION = "INGESTION"

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        if isinstance(other, self.__class__):
            return self is other
        return False

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return self.value


@public
class FeatureMonitoringConfig:
    """Configuration for scheduled statistics computation and comparison on features.

    Built through a fluent API: chain a detection window, an optional reference
    window or training dataset, and a comparison, then call `save()` to register it.
    Once enabled, a scheduled job computes statistics over the detection window and,
    for comparison configs, evaluates each configured metric against its threshold.
    """

    NOT_FOUND_ERROR_CODE = 270233

    def __init__(
        self,
        feature_store_id: int,
        feature_monitoring_type: FeatureMonitoringType | str,
        name: str,
        feature_statistics_configs: list[FeatureStatisticsConfig]
        | list[dict[str, Any]],
        job_name: str | None = None,
        detection_window_config: mwc.MonitoringWindowConfig
        | dict[str, Any]
        | None = None,
        reference_window_config: mwc.MonitoringWindowConfig
        | dict[str, Any]
        | None = None,
        job_schedule: dict[str, Any] | JobSchedule | None = None,
        description: str | None = None,
        id: int | None = None,
        feature_group_id: int | None = None,
        feature_view_name: str | None = None,
        feature_view_version: int | None = None,
        model_name: str | None = None,
        model_version: int | None = None,
        valid_feature_names: list[str] | None = None,
        valid_features: dict[str, str] | None = None,
        href: str | None = None,
        trigger_type: TriggerType | str | None = None,
        training_dataset_id: int | None = None,
        feature_view_id: int | None = None,
        enabled: bool | None = None,
        **kwargs,
    ):
        self.name = name
        self._id = id
        self._href = href
        self.description = description
        self._feature_store_id = feature_store_id
        self._feature_group_id = feature_group_id
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version
        # Backend-assigned IDs for the associated TD / FV entity (read from responses).
        self._training_dataset_id = training_dataset_id
        self._feature_view_id = feature_view_id
        # Top-level enabled flag as emitted by the backend DTO. When not None, the
        # enabled property returns this value; otherwise it delegates to job_schedule.enabled.
        self._enabled = enabled
        # FSTORE-2050: model monitoring — when set, the FM job filters the logging FG by
        # model_name AND model_version. Persisted via to_dict/from_response_json.
        self._model_name = model_name
        self._model_version = model_version
        # In-memory only (not serialized): set by FeatureView.create_model_monitoring so
        # with_reference_training_dataset can default to / validate against the model's TD.
        self._associated_model_td_version: int | None = None
        self._job_name = job_name
        self._feature_monitoring_type = (
            feature_monitoring_type
            if isinstance(feature_monitoring_type, FeatureMonitoringType)
            else FeatureMonitoringType(feature_monitoring_type)
        )
        if trigger_type is None or trigger_type == "CRON":
            self._trigger_type = TriggerType.CRON
        elif isinstance(trigger_type, TriggerType):
            self._trigger_type = trigger_type
        else:
            self._trigger_type = TriggerType(trigger_type)

        self._feature_monitoring_config_engine = (
            feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._monitoring_window_config_engine = (
            monitoring_window_config_engine.MonitoringWindowConfigEngine()
        )
        self._feature_monitoring_result_engine = (
            feature_monitoring_result_engine.FeatureMonitoringResultEngine(
                feature_store_id=feature_store_id,
                feature_group_id=feature_group_id,
                feature_view_name=feature_view_name,
                feature_view_version=feature_view_version,
            )
        )
        self._job_api = job_api.JobApi()

        self.detection_window_config = detection_window_config
        self.reference_window_config = reference_window_config
        self.feature_statistics_configs = self._parse_feature_statistics_configs(
            feature_statistics_configs
        )
        self.job_schedule = job_schedule

        # valid_features (name -> type) wins over valid_feature_names when both are supplied.
        if valid_features is not None:
            self._valid_features = valid_features
            self._valid_feature_names = list(valid_features.keys())
        else:
            self._valid_features = None
            self._valid_feature_names = valid_feature_names

    def _parse_feature_statistics_configs(
        self,
        feature_statistics_configs: list[FeatureStatisticsConfig]
        | list[dict[str, Any]],
    ) -> list[FeatureStatisticsConfig]:
        fs_configs = []
        for fs_config in feature_statistics_configs:
            fs_configs.append(
                fs_config
                if isinstance(fs_config, FeatureStatisticsConfig)
                else FeatureStatisticsConfig.from_response_json(fs_config)
            )
        return fs_configs

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**config) for config in json_decamelized["items"]]
        return cls(**json_decamelized)

    def to_dict(self):
        the_dict = {
            "id": self._id,
            "featureStoreId": self._feature_store_id,
            "name": self._name,
            "description": self._description,
            "jobName": self._job_name,
            "featureMonitoringType": self._feature_monitoring_type,
            "triggerType": self._trigger_type,
            "detectionWindowConfig": self._detection_window_config.to_dict(),
            "featureStatisticsConfigs": [
                fsc.to_dict() for fsc in self._feature_statistics_configs
            ],
        }

        # job schedule
        if isinstance(self._job_schedule, JobSchedule):
            the_dict["jobSchedule"] = self._job_schedule.to_dict()

        # entity id
        if self._feature_group_id is not None:
            the_dict["featureGroupId"] = self._feature_group_id
        elif (
            self._feature_view_name is not None
            and self._feature_view_version is not None
        ):
            the_dict["featureViewName"] = self._feature_view_name
            the_dict["featureViewVersion"] = self._feature_view_version

        # FSTORE-2050: model monitoring — both nullable; emit only when set.
        if self._model_name is not None:
            the_dict["modelName"] = self._model_name
        if self._model_version is not None:
            the_dict["modelVersion"] = self._model_version

        # Backend-assigned entity IDs — emit only when set (populated from server responses).
        if self._training_dataset_id is not None:
            the_dict["trainingDatasetId"] = self._training_dataset_id
        if self._feature_view_id is not None:
            the_dict["featureViewId"] = self._feature_view_id

        # Top-level enabled flag — emit when set so the backend can round-trip it.
        if self._enabled is not None:
            the_dict["enabled"] = self._enabled

        if (
            self._feature_monitoring_type
            == FeatureMonitoringType.STATISTICS_COMPUTATION
        ):
            return the_dict

        reference_window_config = (
            self._reference_window_config.to_dict()
            if self._reference_window_config is not None
            else None
        )
        if reference_window_config is not None:
            the_dict["referenceWindowConfig"] = reference_window_config

        return the_dict

    @property
    def trigger_type(self) -> TriggerType:
        return self._trigger_type

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    def __str__(self):
        return self.json()

    def __repr__(self):
        return f"FeatureMonitoringConfig({self._name!r}, {self._feature_monitoring_type!r})"

    @public
    def with_detection_window(
        self,
        time_offset: str | None = None,
        window_length: str | None = None,
        row_percentage: float | None = None,
    ) -> FeatureMonitoringConfig:
        """Sets the detection window of data to compute statistics on.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Compute statistics on a regular basis
            fg.create_scheduled_statistics(
                name="regular_stats",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                time_offset="1d",
                window_length="1d",
                row_percentage=0.1,
            ).save()
            # Compute and compare statistics
            fg.create_feature_monitoring(
                name="regular_stats",
                feature_name="my_feature",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                time_offset="1d",
                window_length="1d",
                row_percentage=0.1,
            ).with_reference_window(...).compare_on(...).save()
            ```

        Parameters:
            time_offset: The time offset from the current time to the start of the time window.
            window_length: The length of the time window.
            row_percentage: The fraction of rows to use when computing the statistics [0, 1.0].

        Returns:
            The updated FeatureMonitoringConfig object.
        """
        # Setter is using the engine class to perform input validation.
        self.detection_window_config = {
            "window_config_type": (
                mwc.WindowConfigType.ROLLING_TIME
                if time_offset or window_length
                else mwc.WindowConfigType.ALL_TIME
            ),
            "time_offset": time_offset,
            "window_length": window_length,
            "row_percentage": row_percentage,
        }

        return self

    @public
    def with_reference_window(
        self,
        time_offset: str | None = None,
        window_length: str | None = None,
        row_percentage: float | None = None,
    ) -> FeatureMonitoringConfig:
        """Sets the reference window of data to compute statistics on.

        See also `with_reference_value(...)` and `with_reference_training_dataset(...)` for other reference options.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_feature_monitoring(...).with_detection_window(...)
            # Statistics computed on a rolling time window, e.g. same day last week
            my_monitoring_config.with_reference_window(
                time_offset="1w",
                window_length="1d",
            ).compare_on(...).save()
            ```

        Warning: Provide a comparison configuration
            You must provide a comparison configuration via `compare_on()` before saving the feature monitoring config.

        Parameters:
            time_offset: The time offset from the current time to the start of the time window.
            window_length: The length of the time window.
            row_percentage: The percentage of rows to use when computing the statistics. Defaults to 20%.

        Returns:
            The updated FeatureMonitoringConfig object.
        """
        if self.detection_window_config is None:
            self.with_detection_window()  # use default detection window

        # Setter is using the engine class to perform input validation and build monitoring window config object.
        self.reference_window_config = {
            "time_offset": time_offset,
            "window_length": window_length,
            "row_percentage": row_percentage,
        }

        return self

    @public
    def with_reference_training_dataset(
        self,
        training_dataset_version: int | None = None,
    ) -> FeatureMonitoringConfig:
        """Sets the reference training dataset to compare statistics with.

        See also `with_reference_value(...)` and `with_reference_window(...)` for other reference options.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_feature_monitoring(...).with_detection_window(...)
            # Only for feature views: Compare to the statistics computed for one of your training datasets
            # particularly useful if it has been used to train a model currently in production
            my_monitoring_config.with_reference_training_dataset(
                training_dataset_version=3,
            ).compare_on(...).save()
            ```

        Warning: Provide a comparison configuration
            You must provide a comparison configuration via `compare_on()` before saving the feature monitoring config.

        Parameters:
            training_dataset_version: The version of the training dataset to use as reference.

        Returns:
            The updated FeatureMonitoringConfig object.
        """
        if self.detection_window_config is None:
            self.with_detection_window()  # use default detection window

        # FSTORE-2050: when this config was built via FeatureView.create_model_monitoring,
        # the model's training_dataset_version is stashed on _associated_model_td_version.
        # Default to it when the user passes None; otherwise reject mismatches so the user
        # can't accidentally point a model-monitoring config at a TD that didn't train the
        # model. When _associated_model_td_version is None (the create_feature_monitoring
        # path), behavior is unchanged.
        if self._associated_model_td_version is not None:
            if training_dataset_version is None:
                training_dataset_version = self._associated_model_td_version
            elif training_dataset_version != self._associated_model_td_version:
                raise FeatureStoreException(
                    f"training_dataset_version={training_dataset_version} does not match "
                    f"the training dataset version used to train model "
                    f"'{self._model_name}' v{self._model_version} "
                    f"(={self._associated_model_td_version}). "
                    "Omit the argument to default to the model's training TD, "
                    "or pass the matching version."
                )

        self.reference_window_config = {
            "training_dataset_version": training_dataset_version,
        }

        return self

    @public
    def compare_on(
        self,
        metric: str,
        threshold: float | None,
        feature_name: str | None = None,
        strict: bool | None = False,
        relative: bool | None = False,
        specific_value: float | None = None,
    ) -> FeatureMonitoringConfig:
        """Sets the statistics comparison criteria for feature monitoring with a reference.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring, a detection window and a reference window
            my_monitoring_config = fg.create_feature_monitoring(
                ...
            ).with_detection_window(...).with_reference_window(...)
            # Choose a metric and set a threshold for the difference
            # e.g compare the relative mean of detection and reference window
            my_monitoring_config.compare_on(
                feature_name="my_feature",
                metric="mean",
                threshold=1.0,
                relative=True,
            ).save()
            ```

        Note:
            Detection window and reference window/value/training_dataset must be set prior to comparison configuration.

        Parameters:
            metric: The metric to use for comparison. Different metrics are available for different feature types.
            threshold: The threshold to apply to the difference to potentially trigger an alert.
            feature_name: Name of the feature to configure the comparison for.
                When None, the comparison is applied to all type-compatible features.
            strict: Whether to use a strict comparison (e.g. > or <) or a non-strict comparison (e.g. >= or <=).
            relative: Whether to use a relative comparison (e.g. relative mean) or an absolute comparison (e.g. absolute mean).
            specific_value: Fixed reference value to compare against instead of reference statistics.

        Returns:
            The updated FeatureMonitoringConfig object.
        """
        if self.detection_window_config is None:
            self.with_detection_window()  # use default detection window

        if self.reference_window_config is None and specific_value is None:
            raise ValueError("Reference window is required for statistics comparisons.")

        self._feature_monitoring_config_engine._validate_statistics_metric(metric)

        if feature_name is None:
            # Fan out to all type-compatible features.
            if self._valid_features is not None:
                feature_names = (
                    self._feature_monitoring_config_engine.resolve_compatible_features(
                        metric=metric, valid_features=self._valid_features
                    )
                )
            else:
                raise ValueError(
                    "Cannot fan out compare_on — feature type information is required. "
                    "Re-create the monitoring config via fg.create_feature_monitoring(...) "
                    "or fv.create_feature_monitoring(...)."
                )
        else:
            # Single named feature: reject unknown names and check type compatibility.
            if self._valid_features is not None:
                if feature_name not in self._valid_features:
                    raise ValueError(
                        f"Feature '{feature_name}' is not a valid feature for this "
                        f"monitoring config. Valid features are: "
                        f"{sorted(self._valid_features)}."
                    )
                feature_type = self._valid_features[feature_name]
                if not self._feature_monitoring_config_engine.is_type_compatible(
                    metric, feature_type
                ):
                    from hsfs.core.feature_monitoring_config_engine import (
                        NUMERIC_ONLY_SCALAR_METRICS,
                    )

                    metric_lower = metric.lower()
                    if metric_lower in NUMERIC_ONLY_SCALAR_METRICS:
                        raise ValueError(
                            f"Metric '{metric}' requires a numeric feature; "
                            f"'{feature_name}' has type '{feature_type}'."
                        )
            feature_names = [feature_name]

        for fname in feature_names:
            sc_config = StatisticsComparisonConfig(
                metric=metric,
                threshold=threshold,
                relative=relative,
                strict=strict,
                specific_value=specific_value,
            )
            fs_config = self._with_feature_statistics_config(feature_name=fname)
            self._with_statistics_comparison_config(
                feature_statistics_config=fs_config,
                statistics_comparison_config=sc_config,
            )

        # Promote type only if not already at PDF level.
        if (
            self._feature_monitoring_type
            != FeatureMonitoringType.DISTRIBUTION_COMPARISON
        ):
            self._feature_monitoring_type = FeatureMonitoringType.STATISTICS_COMPARISON

        return self

    @public
    def compare_on_distribution(
        self,
        metric: str = "PSI",
        threshold: float | None = None,
        feature_name: str | None = None,
        strict: bool | None = False,
        binning_strategy: str | None = None,
        bin_count: int | None = None,
        smoothing_epsilon: float | None = None,
        custom_bin_edges: list[float] | None = None,
    ) -> FeatureMonitoringConfig:
        """Sets the distribution-distance comparison criteria for feature monitoring.

        Example:
            ```python
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            my_monitoring_config = (
                fg.create_feature_monitoring(name="drift")
                .with_detection_window(time_offset="1d")
                .with_reference_window(time_offset="7d", window_length="7d")
                .compare_on_distribution(
                    feature_name="amount",
                    metric="PSI",
                    threshold=0.2,
                )
                .save()
            )
            ```

        Parameters:
            metric: Distribution distance metric. One of PSI, KL_DIVERGENCE, JS_DIVERGENCE,
                WASSERSTEIN, HELLINGER, KOLMOGOROV_SMIRNOV. Default is PSI.
            threshold: Threshold above which the distance triggers a shift alert.
                Defaults to 0.2 when metric is PSI, otherwise required.
            feature_name: Name of the feature to monitor. When None, applies to all
                type-compatible features.
            strict: Whether the threshold comparison is strict (>) or non-strict (>=).
            binning_strategy: How to discretize continuous features. One of EQUI_WIDTH,
                EQUI_FREQUENCY, CUSTOM_EDGES, CATEGORICAL.
                Defaults to EQUI_FREQUENCY for numeric features and CATEGORICAL otherwise.
            bin_count: Number of histogram bins. Defaults to 10.
            smoothing_epsilon: Small additive constant to avoid log(0). Defaults to 1e-6.
            custom_bin_edges: Required when binning_strategy is CUSTOM_EDGES.

        Returns:
            The updated FeatureMonitoringConfig object.
        """
        if self.detection_window_config is None:
            self.with_detection_window()

        if self.reference_window_config is None:
            raise ValueError(
                "Reference window is required for distribution comparisons. "
                "Use with_reference_window() or with_reference_training_dataset() first."
            )

        metric_upper = metric.upper()

        # Apply defaults.
        if threshold is None:
            if metric_upper == "PSI":
                threshold = 0.2
            else:
                raise ValueError(
                    f"threshold is required for metric '{metric}'. "
                    "Only PSI has a built-in default of 0.2."
                )

        if bin_count is None:
            bin_count = 10
        if smoothing_epsilon is None:
            smoothing_epsilon = 1e-6

        if feature_name is None:
            # Fan out to all type-compatible features.
            if self._valid_features is not None:
                feature_names = (
                    self._feature_monitoring_config_engine.resolve_compatible_features(
                        metric=metric_upper, valid_features=self._valid_features
                    )
                )
            else:
                raise ValueError(
                    "Cannot fan out compare_on_distribution — feature type information is required. "
                    "Re-create the monitoring config via fg.create_feature_monitoring(...) "
                    "or fv.create_feature_monitoring(...)."
                )
        else:
            # Single named feature: check WASSERSTEIN/KS type constraint.
            if (
                self._valid_features is not None
                and feature_name in self._valid_features
            ):
                feature_type = self._valid_features[feature_name]
                if not self._feature_monitoring_config_engine.is_type_compatible(
                    metric_upper, feature_type
                ):
                    raise ValueError(
                        f"Metric '{metric_upper}' requires a numeric feature; "
                        f"'{feature_name}' has type '{feature_type}'."
                    )
            feature_names = [feature_name]

        for fname in feature_names:
            # Determine per-feature binning_strategy default if not supplied.
            resolved_binning_strategy = binning_strategy
            if resolved_binning_strategy is None:
                if self._valid_features is not None and fname in self._valid_features:
                    ftype = self._valid_features[fname]
                    ftype_lower = ftype.lower() if ftype else ""
                    from hsfs.core.feature_monitoring_config_engine import (
                        NUMERIC_HIVE_TYPES,
                    )

                    is_numeric = (
                        ftype_lower in NUMERIC_HIVE_TYPES
                        or ftype_lower.startswith("decimal")
                    )
                    resolved_binning_strategy = (
                        "EQUI_FREQUENCY" if is_numeric else "CATEGORICAL"
                    )
                else:
                    resolved_binning_strategy = "EQUI_FREQUENCY"

            sc_config = StatisticsComparisonConfig(
                distribution_metric=metric_upper,
                threshold=threshold,
                strict=strict,
                binning_strategy=resolved_binning_strategy,
                bin_count=bin_count,
                smoothing_epsilon=smoothing_epsilon,
                custom_bin_edges=custom_bin_edges,
            )
            fs_config = self._with_feature_statistics_config(feature_name=fname)
            self._with_statistics_comparison_config(
                feature_statistics_config=fs_config,
                statistics_comparison_config=sc_config,
            )

        # Promote to PDF — never downgrade.
        self._feature_monitoring_type = FeatureMonitoringType.DISTRIBUTION_COMPARISON

        return self

    @public
    def save(self) -> FeatureMonitoringConfig:
        """Saves the feature monitoring configuration.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Setup feature monitoring and a detection window
            my_monitoring_config = fg.create_scheduled_statistics(
                name="my_monitoring_config",
            ).save()
            ```

        Returns:
            The saved FeatureMonitoringConfig object.
        """
        registered_config = self._feature_monitoring_config_engine._save(self)
        self.detection_window_config = registered_config._detection_window_config
        self.job_schedule = registered_config._job_schedule
        self.feature_statistics_configs = registered_config._feature_statistics_configs
        self._job_name = registered_config._job_name
        self._id = registered_config._id

        if (
            self._feature_monitoring_type
            != FeatureMonitoringType.STATISTICS_COMPUTATION
        ):
            self.reference_window_config = registered_config._reference_window_config

        return self

    @public
    def update(self) -> FeatureMonitoringConfig:
        """Updates allowed fields of the saved feature monitoring configuration.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Update the percentage of rows to use when computing the statistics
            my_monitoring_config.detection_window.row_percentage = 10
            my_monitoring_config.update()
            ```

        Returns:
            The updated FeatureMonitoringConfig object.
        """
        return self._feature_monitoring_config_engine._update(self)

    @public
    def run_once(self) -> Job:
        """Trigger the feature monitoring job once which computes and compares statistics on the detection and reference windows.

        Example:
            ```python3
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Trigger the feature monitoring job once
            my_monitoring_config.run_job()
            ```

        Info:
            The feature monitoring job will be triggered asynchronously and the method will return immediately.
            Calling this method does not affect the ongoing schedule.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the feature monitoring config has not been saved.

        Returns:
            A handle for the job computing the statistics.
        """
        if not self._id or not self._job_name:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before computing statistics."
            )

        return self._feature_monitoring_config_engine._trigger_monitoring_job(
            job_name=self.job_name
        )

    @public
    def get_job(self) -> Job:
        """Get the feature monitoring job which computes and compares statistics on the detection and reference windows.

        Example:
            ```python3
            # Fetch registered config by name via feature group or feature view
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Get the job which computes statistics on detection and reference window
            job = my_monitoring_config.get_job()
            # Print job history and ongoing executions
            job.executions
            ```
        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the feature monitoring config has not been saved.

        Returns:
            A handle for the job computing the statistics.
        """
        if not self._id or not self._job_name:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before fetching"
                "the associated job."
            )

        return self._feature_monitoring_config_engine._get_monitoring_job(
            job_name=self.job_name
        )

    @public
    def delete(self):
        """Deletes the feature monitoring configuration.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Delete the feature monitoring config
            my_monitoring_config.delete()
            ```

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the feature monitoring config has not been saved.
        """
        if not self._id:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before deleting."
            )

        self._feature_monitoring_config_engine._delete(config_id=self._id)

    @public
    def disable(self):
        """Disables the schedule of the feature monitoring job.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Disable the feature monitoring config
            my_monitoring_config.disable()
            ```

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the feature monitoring config has not been saved.
        """
        self._update_schedule(enabled=False)

    @public
    def enable(self):
        """Enables the schedule of the feature monitoring job.

        The scheduler can be configured via the `job_schedule` property.

        Example:
            ```python
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Enable the feature monitoring config
            my_monitoring_config.enable()
            ```

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the feature monitoring config has not been saved.
        """
        self._update_schedule(enabled=True)

    @public
    def get_history(
        self,
        start_time: datetime | date | str | int | None = None,
        end_time: datetime | date | str | int | None = None,
        with_statistics: bool = True,
    ) -> list[FeatureMonitoringResult]:
        """Fetch the history of the computed statistics and comparison results for this configuration.

        Example:
            ```python3
            # Fetch your feature group or feature view
            fg = fs.get_feature_group(name="my_feature_group", version=1)
            # Fetch registered config by name
            my_monitoring_config = fg.get_feature_monitoring_configs(name="my_monitoring_config")
            # Fetch the history of the computed statistics for this configuration
            history = my_monitoring_config.get_history(
                start_time="2021-01-01",
                end_time="2021-01-31",
            )
            ```

        Parameters:
            start_time: The start time of the time range to fetch the history for.
            end_time: The end time of the time range to fetch the history for.
            with_statistics: Whether to include the computed statistics in the results.

        Returns:
            A list of FeatureMonitoringResult objects containing the history of the computed statistics and comparison results for this configuration.

        Raises:
            hopsworks.client.exceptions.FeatureStoreException: If the feature monitoring config has not been saved.
        """
        if not self._id:
            raise FeatureStoreException(
                "Feature monitoring config must be registered via `.save()` before fetching"
                "the associated history."
            )
        return self._feature_monitoring_result_engine._fetch_all_feature_monitoring_results_by_config_id(
            config_id=self._id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    @public
    def get_feature_names(self) -> list[str]:
        """Return the names of all features monitored by this configuration.

        Returns:
            The names of all features monitored by this configuration.
        """
        return [
            fs_config.feature_name for fs_config in self._feature_statistics_configs
        ]

    def _with_feature_statistics_config(
        self, feature_name: str
    ) -> FeatureStatisticsConfig:
        if self.feature_statistics_configs is not None:
            for fsc in self.feature_statistics_configs:
                if fsc.feature_name == feature_name:
                    return fsc

        fs_config = FeatureStatisticsConfig(
            feature_name=feature_name,
            statistics_comparison_configs=[],
        )
        if self.feature_statistics_configs is None:
            self.feature_statistics_configs = [fs_config]
        else:
            self.feature_statistics_configs.append(fs_config)

        return fs_config

    def _with_statistics_comparison_config(
        self,
        feature_statistics_config: FeatureStatisticsConfig,
        statistics_comparison_config: StatisticsComparisonConfig,
    ) -> StatisticsComparisonConfig:
        if feature_statistics_config.statistics_comparison_configs is None:
            raise ValueError("Statistics comparison configurations cannot be updated")

        for sc_config in feature_statistics_config.statistics_comparison_configs:
            if sc_config == statistics_comparison_config:
                raise ValueError(
                    "A similar statistics comparison configuration already exists."
                )

        feature_statistics_config.statistics_comparison_configs.append(
            statistics_comparison_config
        )
        return statistics_comparison_config

    def _update_schedule(self, enabled):
        if self._job_schedule is None:
            raise FeatureStoreException("No scheduler found for monitoring job")
        job_schedule = JobSchedule(
            id=self._job_schedule.id,
            start_date_time=self._job_schedule.start_date_time,
            cron_expression=self._job_schedule.cron_expression,
            end_time=self._job_schedule.end_date_time,
            enabled=enabled,
        )
        self.job_schedule = self._job_api.create_or_update_schedule_job(
            self._job_name, job_schedule.to_dict()
        )
        return self._job_schedule

    @public
    @property
    def id(self) -> int | None:
        """Id of the feature monitoring configuration."""
        return self._id

    @public
    @property
    def feature_store_id(self) -> int:
        """Id of the Feature Store."""
        return self._feature_store_id

    @public
    @property
    def feature_group_id(self) -> int | None:
        """Id of the Feature Group to which this feature monitoring configuration is attached."""
        return self._feature_group_id

    @public
    @property
    def feature_view_name(self) -> str | None:
        """Name of the Feature View to which this feature monitoring configuration is attached."""
        return self._feature_view_name

    @public
    @property
    def feature_view_version(self) -> int | None:
        """Version of the Feature View to which this feature monitoring configuration is attached."""
        return self._feature_view_version

    @public
    @property
    def model_name(self) -> str | None:
        """Name of the model whose inference logs are filtered by this configuration.

        Set on configs created via `FeatureView.create_model_monitoring` /
        `Deployment.create_model_monitoring`. Both `model_name` and `model_version` are
        either both null (regular feature monitoring) or both non-null (model monitoring).
        """
        return self._model_name

    @public
    @property
    def model_version(self) -> int | None:
        """Version of the model whose inference logs are filtered by this configuration."""
        return self._model_version

    @public
    @property
    def name(self) -> str:
        """The name of the feature monitoring config.

        A Feature Group or Feature View cannot have multiple feature monitoring configurations with the same name. The name of
        a feature monitoring configuration is limited to 63 characters.

        Info:
            This property is read-only once the feature monitoring configuration has been saved.
        """
        return self._name

    @name.setter
    def name(self, name: str):
        if hasattr(self, "_id") and self._id is not None:
            raise AttributeError("The name of a registered config is read-only.")
        if not isinstance(name, str):
            raise TypeError("name must be of type str")
        if len(name) > FEATURES.MAX_LENGTH_NAME:
            raise ValueError(
                "name must be less than {FEATURES.MAX_LENGTH_NAME} characters."
            )
        self._name = name

    @public
    @property
    def description(self) -> str | None:
        """Description of the feature monitoring configuration."""
        return self._description

    @description.setter
    def description(self, description: str | None):
        if not isinstance(description, str) and description is not None:
            raise TypeError("description must be of type str")
        if isinstance(description, str) and len(description) > MAX_LENGTH_DESCRIPTION:
            raise ValueError(
                "description must be less than {MAX_LENGTH_DESCRIPTION} characters"
            )
        self._description = description

    @public
    @property
    def job_name(self) -> str | None:
        """Name of the feature monitoring job."""
        return self._job_name

    @public
    @property
    def enabled(self) -> bool:
        """Controls whether or not this config is spawning new feature monitoring jobs.

        This field belongs to the scheduler configuration but is made transparent to the user for convenience.
        When the backend returns a top-level enabled flag, that value takes precedence.
        """
        if self._enabled is not None:
            return self._enabled
        return self.job_schedule.enabled

    @enabled.setter
    def enabled(self, enabled: bool):
        self._enabled = enabled
        self.job_schedule.enabled = enabled

    @public
    @property
    def feature_monitoring_type(self) -> FeatureMonitoringType:
        """The type of feature monitoring to perform. Used for internal validation.

        Options are:
            - STATISTICS_COMPUTATION if no reference window (and, therefore, comparison config) is provided
            - STATISTICS_COMPARISON if a reference window (and, therefore, comparison config) is provided.

        Info:
            This property is read-only.
        """
        return self._feature_monitoring_type

    @public
    @property
    def job_schedule(self) -> JobSchedule:
        """Schedule of the feature monitoring job.

        This field belongs to the job configuration but is made transparent to the user for convenience.
        """
        return self._job_schedule

    @job_schedule.setter
    def job_schedule(self, job_schedule: JobSchedule | dict[str, Any] | None):
        if isinstance(job_schedule, JobSchedule):
            self._job_schedule = job_schedule
        elif isinstance(job_schedule, dict):
            self._job_schedule = JobSchedule(**job_schedule)
        elif job_schedule is None:
            self._job_schedule = None
        else:
            raise TypeError("job_schedule must be of type JobSchedule, dict or None")

    @public
    @property
    def detection_window_config(self) -> mwc.MonitoringWindowConfig:
        """Configuration for the detection window."""
        return self._detection_window_config

    @detection_window_config.setter
    def detection_window_config(
        self,
        detection_window_config: mwc.MonitoringWindowConfig | dict[str, Any] | None,
    ):
        if isinstance(detection_window_config, mwc.MonitoringWindowConfig):
            self._detection_window_config = detection_window_config
        elif isinstance(detection_window_config, dict):
            self._detection_window_config = (
                self._monitoring_window_config_engine._build_monitoring_window_config(
                    **detection_window_config
                )
            )
        elif detection_window_config is None:
            self._detection_window_config = detection_window_config
        else:
            raise TypeError(
                "detection_window_config must be of type MonitoringWindowConfig, dict or None"
            )

    @public
    @property
    def reference_window_config(self) -> mwc.MonitoringWindowConfig:
        """Configuration for the reference window."""
        return self._reference_window_config

    @reference_window_config.setter
    def reference_window_config(
        self,
        reference_window_config: mwc.MonitoringWindowConfig
        | dict[str, Any]
        | None = None,
    ):
        """Sets the reference window for monitoring.

        Parameters:
            reference_window_config: The configuration for the reference window.
        """
        # TODO: improve setter documentation
        if (
            self._feature_monitoring_type
            == FeatureMonitoringType.STATISTICS_COMPUTATION
            and reference_window_config is not None
        ):
            raise AttributeError(
                "reference_window_config is only available for feature monitoring"
                " not for scheduled statistics. Use `create_feature_monitoring()` instead."
            )
        if isinstance(reference_window_config, mwc.MonitoringWindowConfig):
            self._reference_window_config = reference_window_config
        elif isinstance(reference_window_config, dict):
            self._reference_window_config = (
                self._monitoring_window_config_engine._build_monitoring_window_config(
                    **reference_window_config
                )
            )
        elif reference_window_config is None:
            self._reference_window_config = None
        else:
            raise TypeError(
                "reference_window_config must be of type MonitoringWindowConfig, dict or None"
            )

    @public
    @property
    def feature_statistics_configs(self) -> list[FeatureStatisticsConfig]:
        """Configurations for the computation (and comparison) of feature statistics."""
        return self._feature_statistics_configs

    @feature_statistics_configs.setter
    def feature_statistics_configs(
        self, feature_statistics_configs: list[FeatureStatisticsConfig]
    ):
        self._feature_monitoring_config_engine._validate_feature_statistics_configs(
            feature_statistics_configs,
            self._feature_monitoring_type
            == FeatureMonitoringType.STATISTICS_COMPUTATION,
        )
        self._feature_statistics_configs = feature_statistics_configs
