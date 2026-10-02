from __future__ import annotations

import json
import warnings
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_apigen import public
from hopsworks_common import constants
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


@public
class FeatureLogging:
    NOT_FOUND_ERROR_CODE = 270248

    def __init__(
        self,
        id: int | None = None,
        transformed_features: feature_group.FeatureGroup | None = None,
        untransformed_features: feature_group.FeatureGroup | None = None,
        extra_logging_columns: list[Feature] | None = None,
    ):
        """DTO class for feature logging.

        Parameters:
            id: Id of the feature logging object.
            transformed_features: The feature group containing the transformed features. As of Hopsworks 4.6, transformed and untransformed features are logged in the same feature group. This feature group is maintained for backward compatibility.
            untransformed_features: The feature group containing the untransformed features.
            extra_logging_columns: List of extra logging columns.
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

    def _update(self, others):
        self._transformed_features = others.transformed_features
        self._untransformed_features = others.untransformed_features
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

    @property
    def _is_legacy(self) -> bool:
        """Whether this logging row predates FSTORE-1871, i.e. still carries a separate transformed feature group."""
        return self._transformed_features is not None

    @staticmethod
    def _uses_legacy_model_column(feature_names: set[str] | list[str]) -> bool:
        """Whether a logging feature group stores the model identity in the pre-FSTORE-1871 hsml_model column.

        A feature group that carries both hsml_model and model_name classifies as current schema, so
        corrupted combined feature groups are never written to as if they were legacy.
        """
        return (
            constants.FEATURE_LOGGING.LEGACY_MODEL_COLUMN_NAME in feature_names
            and constants.FEATURE_LOGGING.MODEL_COLUMN_NAME not in feature_names
        )

    @staticmethod
    def _prediction_column_names(
        prediction_feature_names: list[str],
        logging_feature_group_feature_names: list[str],
    ) -> dict[str, str]:
        """Map each label column to the column name it takes in the logging feature group.

        Current logging feature groups store predictions under predicted_<label>; legacy
        (pre-FSTORE-1871) ones store them under the bare label name. Mixed schemas resolve
        to the prefixed name so corrupted feature groups are treated as current schema.
        """
        mapping = {}
        for feature_name in prediction_feature_names:
            prefixed_name = constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + feature_name
            if (
                prefixed_name not in logging_feature_group_feature_names
                and feature_name in logging_feature_group_feature_names
            ):
                mapping[feature_name] = feature_name
            else:
                mapping[feature_name] = prefixed_name
        return mapping

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
        }

    def json(self) -> dict[str, Any]:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self):
        return self.json()
