from __future__ import annotations

import json
import warnings
from typing import TYPE_CHECKING, Any

import humps
from hsfs import feature_group, util
from hsfs.feature import Feature


if TYPE_CHECKING:
    import datetime


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


class FeatureLogging:
    NOT_FOUND_ERROR_CODE = 270248

    def __init__(
        self,
        id: int = None,
        transformed_features: feature_group.FeatureGroup = None,
        untransformed_features: feature_group.FeatureGroup = None,
        extra_logging_columns: list[Feature] | None = None,
    ):
        """DTO class for feature logging.

        Parameters:
            id : `int`. Id of the feature logging object.
            transformed_features : `FeatureGroup`. The feature group containing the transformed features. As of Hopsworks 4.6, transformed and untransformed features are logged in the same feature group. This feature group is maintained for backward compatibility.
            untransformed_features : `FeatureGroup`. The feature group containing the untransformed features.
            extra_logging_columns : `List[Feature]`. List of extra logging columns.
        """
        self._id = id
        self._transformed_features = transformed_features
        self._untransformed_features = untransformed_features
        self._extra_logging_columns = extra_logging_columns

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
        )

    def update(self, others):
        self._transformed_features = others.transformed_features
        self._untransformed_features = others.untransformed_features
        return self

    @property
    def transformed_features(self) -> feature_group.FeatureGroup:
        return self._transformed_features

    @property
    def untransformed_features(self) -> feature_group.FeatureGroup:
        return self._untransformed_features

    @property
    def extra_logging_columns(self) -> list[Feature] | None:
        return self._extra_logging_columns

    def get_feature_group(self, transformed: bool | None = None):
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

    @property
    def id(self) -> str:
        return self._id

    def to_dict(self):
        return {
            "id": self._id,
            "transformedLogFg": self._transformed_features,
            "untransformedLogFg": self._untransformed_features,
            "extraLoggingColumns": self._extra_logging_columns,
        }

    def json(self) -> dict[str, Any]:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self):
        return self.json()
