from __future__ import annotations

import json
import warnings
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hsfs import feature_group, util
from hsfs.feature import Feature


if TYPE_CHECKING:
    import datetime


# Quartz expressions behind the two supported cadences of the offline
# materialization job. Values are the names users pass, not the cron strings.
LOG_MATERIALIZATION_INTERVALS: dict[str, str] = {
    "hour": "0 0 * * * ? *",
    "day": "0 0 0 * * ? *",
}


# The two ways logged rows reach the logging feature group. A feature view
# logs through exactly one of them: the layout of its logging group differs.
LOG_TRANSPORTS = ("realtime", "job")


def _transport(name: str) -> str:
    key = str(name).strip().lower()
    if key not in LOG_TRANSPORTS:
        raise ValueError(
            f"Unsupported feature logging transport {name!r}; "
            f"expected one of {', '.join(LOG_TRANSPORTS)}."
        )
    return key


def _materialization_cron(interval: str) -> str:
    key = str(interval).strip().lower()
    if key not in LOG_MATERIALIZATION_INTERVALS:
        raise ValueError(
            f"Unsupported log materialization interval {interval!r}; "
            f"expected one of {', '.join(LOG_MATERIALIZATION_INTERVALS)}."
        )
    return LOG_MATERIALIZATION_INTERVALS[key]


def _transport_of_feature_group(fg) -> str:
    """The transport a logging feature group was created for, read off its layout."""
    return "realtime" if getattr(fg, "stream", True) else "job"


def _interval_of_schedule(schedule) -> str | None:
    """The interval name whose cron a job schedule carries, `None` for any other cadence."""
    cron = schedule.cron_expression if schedule is not None else None
    return next(
        (name for name, expr in LOG_MATERIALIZATION_INTERVALS.items() if expr == cron),
        None,
    )


class LoggingMetaData:
    """Class that holds the data for feature logging."""

    def __init__(self):
        self.untransformed_features: list[list[Any]] | None = []
        self.transformed_features: list[list[Any]] = []
        self.serving_keys: list[dict[str, Any]] = []
        self.request_parameters: list[dict[str, Any]] = []
        self.event_time: list[datetime.datetime] = []
        self.inference_helper: list[dict[str, Any]] = []

    def __repr__(self):
        return (
            f"LoggingMetaData(untransformed_features={self.untransformed_features}, \n"
            f"transformed_features={self.transformed_features}, \n"
            f"serving_keys={self.serving_keys}, \n"
            f"request_parameters={self.request_parameters}, \n"
            f"event_time={self.event_time}, \n"
            f"inference_helper={self.inference_helper})"
        )


@public
class FeatureLogging:
    NOT_FOUND_ERROR_CODE = 270248

    def __init__(
        self,
        id: int | None = None,
        transformed_features: feature_group.FeatureGroup | None = None,
        untransformed_features: feature_group.FeatureGroup | None = None,
        extra_logging_columns: list[Feature] | None = None,
        materialization_interval: str | None = None,
        transport: str | None = None,
    ):
        """DTO class for feature logging.

        Parameters:
            id: Id of the feature logging object.
            transformed_features: The feature group containing the transformed features. As of Hopsworks 4.6, transformed and untransformed features are logged in the same feature group. This feature group is maintained for backward compatibility.
            untransformed_features: The feature group containing the untransformed features.
            extra_logging_columns: List of extra logging columns.
            materialization_interval: How often the logs are written to the offline store, `"hour"` or `"day"`; `None` keeps the platform default.
            transport: How logged rows reach the logging feature group, `"realtime"` or `"job"`; `None` keeps the platform default.
        """
        self._id = id
        self._transformed_features = transformed_features
        self._untransformed_features = untransformed_features
        self._extra_logging_columns = extra_logging_columns
        self._materialization_interval = (
            None
            if materialization_interval is None
            else str(materialization_interval).strip().lower()
        )
        if self._materialization_interval is not None:
            _materialization_cron(self._materialization_interval)
        self._transport = None if transport is None else _transport(transport)

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> FeatureLogging:
        from hsfs.feature_group import FeatureGroup  # avoid circular import

        json_decamelized = humps.decamelize(json_dict)
        transformed_features = json_decamelized.get("transformed_log_fg")
        untransformed_features = json_decamelized.get("untransformed_log_fg")
        if transformed_features:
            transformed_features = FeatureGroup.from_response_json(transformed_features)
        if untransformed_features:
            untransformed_features = FeatureGroup.from_response_json(
                untransformed_features
            )
        extra_logging_columns = json_decamelized.get("extra_logging_columns")
        if extra_logging_columns:
            extra_logging_columns = [
                Feature.from_response_json(feature) for feature in extra_logging_columns
            ]
        return cls(
            json_decamelized.get("id"),
            transformed_features,
            untransformed_features,
            extra_logging_columns,
            json_decamelized.get("materialization_interval"),
            json_decamelized.get("transport"),
        )

    def _update(self, others):
        self._transformed_features = others.transformed_features
        self._untransformed_features = others.untransformed_features
        # Both derive from the new group and its schedule.
        self._transport = None
        self._materialization_interval = None
        return self

    @public
    @property
    def transformed_features(self) -> feature_group.FeatureGroup:
        return self._transformed_features

    @public
    @property
    def untransformed_features(self) -> feature_group.FeatureGroup:
        return self._untransformed_features

    @public
    @property
    def extra_logging_columns(self) -> list[Feature] | None:
        return self._extra_logging_columns

    @public
    @property
    def materialization_interval(self) -> str | None:
        """How often the logs are written to the offline store, `"hour"` or `"day"`, or `None` for the platform default.

        The backend keeps the cadence only as the materialization job's schedule, so a value that was not set in this session is read back from that schedule.
        """
        if (
            self._materialization_interval is None
            and self._untransformed_features is not None
        ):
            self._materialization_interval = _interval_of_schedule(self._schedule())
        return self._materialization_interval

    def _schedule(self):
        # The job transport has no materialization job: its cadence is the commit
        # job's schedule, named after the logging group.
        if self.transport == "job":
            from hopsworks_common.core.job_api import JobApi

            job = JobApi().get_job(
                f"{self._untransformed_features.name}_feature_log_commit"
            )
            return job.job_schedule if job is not None else None
        return self._untransformed_features.materialization_job.job_schedule

    @public
    @property
    def transport(self) -> str | None:
        """How logged rows reach the logging feature group, `"realtime"` or `"job"`.

        A feature view logs through one transport, and the layout of its logging feature group tells which: the `realtime` group is a stream group with an online copy, the `job` group is an offline-only Delta group filled by the commit job.
        `None` when logging has no feature group yet.
        """
        if self._transport is None and self._untransformed_features is not None:
            self._transport = _transport_of_feature_group(self._untransformed_features)
        return self._transport

    @public
    def get_feature_group(
        self, transformed: bool | None = None
    ) -> feature_group.FeatureGroup:
        """Get the feature group backing this feature logging.

        Transformed and untransformed features are logged in the same feature
        group, so the same feature group is returned regardless of `transformed`.

        Parameters:
            transformed: Deprecated and ignored; kept for backwards compatibility.

        Returns:
            The feature group used to store logged features.
        """
        if transformed is not None:
            warnings.warn(
                "Providing ´transformed´ while fetching logging feature group is deprecated"
                " and will be dropped in future versions. Transformed and untransformed features are now logged in the same feature group.",
                DeprecationWarning,
                stacklevel=2,
            )
        if transformed:
            if self._transformed_features is None:
                return self._untransformed_features
            return self._transformed_features
        return self._untransformed_features

    @public
    @property
    def id(self) -> str:
        return self._id

    def to_dict(self):
        return {
            "id": self._id,
            "transformedLogFg": self._transformed_features,
            "untransformedLogFg": self._untransformed_features,
            "extraLoggingColumns": self._extra_logging_columns,
            "materializationInterval": self._materialization_interval,
            "transport": self._transport,
        }

    def json(self) -> dict[str, Any]:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self):
        return self.json()
