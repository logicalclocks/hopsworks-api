import json
import warnings
from typing import Any, Dict, List, Optional

import humps
from hsfs import feature_group, util
from hsfs.feature import Feature


class FeatureLogging:
    NOT_FOUND_ERROR_CODE = 270248

    def __init__(
        self,
        id: int = None,
        transformed_features: "feature_group.FeatureGroup" = None,
        untransformed_features: "feature_group.FeatureGroup" = None,
        extra_logging_columns: Optional[List[Feature]] = None,
    ):
        self._id = id
        self._transformed_features = transformed_features
        self._untransformed_features = untransformed_features
        self._extra_logging_columns = extra_logging_columns

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "FeatureLogging":
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
    def transformed_features(self) -> "feature_group.FeatureGroup":
        return self._transformed_features

    @property
    def untransformed_features(self) -> "feature_group.FeatureGroup":
        return self._untransformed_features

    @property
    def extra_logging_columns(self) -> Optional[List[Feature]]:
        return self._extra_logging_columns

    def get_feature_group(self, transformed: Optional[bool] = None):
        if transformed is not None:
            warnings.warn(
                "Providing ´transformed´ while fetching logging feature group is deprecated"
                + " and will be dropped in future versions. Transformed and untransformed features are now logged in the same feature group.",
                DeprecationWarning,
                stacklevel=2,
            )
        if transformed:
            if self._transformed_features is None:
                return self._untransformed_features
            return self._transformed_features
        else:
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

    def json(self) -> Dict[str, Any]:
        return json.dumps(self, cls=util.Encoder)

    def __repr__(self):
        return self.json()
