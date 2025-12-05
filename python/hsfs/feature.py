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
from datetime import datetime
from typing import TYPE_CHECKING, Any

import hsfs
import humps
from hsfs import util
from hsfs.constructor import filter
from hsfs.decorators import typechecked


if TYPE_CHECKING:
    from hsfs.feature_group import FeatureGroup


@typechecked
class Feature:
    """Metadata object representing a feature in a feature group in the Feature Store.

    See Training Dataset Feature for the
    feature representation of training dataset schemas.
    """

    COMPLEX_TYPES = ["MAP", "ARRAY", "STRUCT", "UNIONTYPE"]

    def __init__(
        self,
        name: str,
        type: str | None = None,
        description: str | None = None,
        primary: bool = False,
        foreign: bool = False,
        partition: bool = False,
        hudi_precombine_key: bool = False,
        online_type: str | None = None,
        default_value: str | None = None,
        feature_group_id: int | None = None,
        feature_group: hsfs.feature_group.FeatureGroup
        | hsfs.feature_group.ExternalFeatureGroup
        | hsfs.feature_group.SpineGroup
        | None = None,
        on_demand: bool = False,
        use_fully_qualified_name=False,
        **kwargs,
    ) -> None:
        self._name = util.autofix_feature_name(name, warn=True)
        self._type = type
        self._description = description
        self._primary = primary
        self._foreign = foreign
        self._partition = partition
        self._hudi_precombine_key = hudi_precombine_key
        self._online_type = online_type
        self._default_value = default_value
        self._use_fully_qualified_name = use_fully_qualified_name
        if feature_group is not None:
            self._feature_group_id = feature_group.id
        else:
            self._feature_group_id = feature_group_id
        self._on_demand = on_demand

    def to_dict(self) -> dict[str, Any]:
        """Get structured info about specific Feature in python dictionary format.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            selected_feature = fg.get_feature("min_temp")
            selected_feature.to_dict()
            ```
        """
        return {
            "name": self._name,
            "type": self._type,
            "description": self._description,
            "partition": self._partition,
            "hudiPrecombineKey": self._hudi_precombine_key,
            "primary": self._primary,
            "foreign": self._foreign,
            "onlineType": self._online_type,
            "defaultValue": self._default_value,
            "featureGroupId": self._feature_group_id,
            "onDemand": self.on_demand,
            "useFullyQualifiedName": self._use_fully_qualified_name,
        }

    def _get_fully_qualified_feature_name(
        self, feature_group: FeatureGroup = None, prefix: str = None
    ) -> str:
        """Returns the name of the feature when used to generated dataframes for training/batch data.

        - If the feature is configured to use a fully qualified name, it returns that name.
        - Otherwise, if a prefix is provided, it returns the feature name prefixed accordingly.
        - If neither condition applies, it returns the feature's original name.

        Parameters:
            feature_group (FeatureGroup, optional): The feature group context in which the name is being used.
            prefix (str, optional): A prefix to prepend to the feature name if applicable.

        Returns:
            str: The fully qualified feature name.
        """
        if self.use_fully_qualified_name:
            return util.generate_fully_qualified_feature_name(
                feature_group=feature_group, feature_name=self._name
            )
        if prefix:
            return prefix + self._name
        return self._name

    @property
    def use_fully_qualified_name(self) -> bool:
        """Use fully qualified name for the feature when generating dataframes for training/batch data."""
        return self._use_fully_qualified_name

    @use_fully_qualified_name.setter
    def use_fully_qualified_name(self, use_fully_qualified_name: bool) -> None:
        self._use_fully_qualified_name = use_fully_qualified_name

    def json(self) -> str:
        return json.dumps(self, cls=util.Encoder)

    @classmethod
    def from_response_json(cls, json_dict: dict[str, Any]) -> Feature:
        if json_dict is None:
            return None

        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def is_complex(self) -> bool:
        """Returns true if the feature has a complex type.

        Example:
            ```python
            # connect to the Feature Store
            fs = ...

            # get the Feature Group instance
            fg = fs.get_or_create_feature_group(...)

            selected_feature = fg.get_feature("min_temp")
            selected_feature.is_complex()
            ```
        """
        return any(map(self._type.upper().startswith, self.COMPLEX_TYPES))

    @property
    def name(self) -> str:
        """Name of the feature."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def description(self) -> str | None:
        """Description of the feature."""
        return self._description

    @description.setter
    def description(self, description: str | None) -> None:
        self._description = description

    @property
    def type(self) -> str | None:
        """Data type of the feature in the offline feature store.

        Warning: Not a Python type
            This type property is not to be confused with Python types.
            The type property represents the actual data type of the feature in
            the feature store.
        """
        return self._type

    @type.setter
    def type(self, type: str | None) -> None:
        self._type = type

    @property
    def online_type(self) -> str | None:
        """Data type of the feature in the online feature store."""
        return self._online_type

    @online_type.setter
    def online_type(self, online_type: str | None) -> None:
        self._online_type = online_type

    @property
    def primary(self) -> bool:
        """Whether the feature is part of the primary key of the feature group."""
        return self._primary

    @primary.setter
    def primary(self, primary: bool) -> None:
        self._primary = primary

    @property
    def foreign(self) -> bool:
        """Whether the feature is part of the foreign key of the feature group."""
        return self._foreign

    @foreign.setter
    def foreign(self, foreign: bool) -> None:
        self._foreign = foreign

    @property
    def partition(self) -> bool:
        """Whether the feature is part of the partition key of the feature group."""
        return self._partition

    @partition.setter
    def partition(self, partition: bool) -> None:
        self._partition = partition

    @property
    def hudi_precombine_key(self) -> bool:
        """Whether the feature is part of the hudi precombine key of the feature group."""
        return self._hudi_precombine_key

    @hudi_precombine_key.setter
    def hudi_precombine_key(self, hudi_precombine_key: bool) -> None:
        self._hudi_precombine_key = hudi_precombine_key

    @property
    def default_value(self) -> str | None:
        """Default value of the feature as string, if the feature was appended to the feature group."""
        return self._default_value

    @default_value.setter
    def default_value(self, default_value: str | None) -> None:
        self._default_value = default_value

    @property
    def feature_group_id(self) -> int | None:
        return self._feature_group_id

    @property
    def on_demand(self) -> bool:
        """Whether the feature is a on-demand feature computed using on-demand transformation functions."""
        return self._on_demand

    @on_demand.setter
    def on_demand(self, on_demand) -> None:
        self._on_demand = on_demand

    def _get_filter_value(self, value: Any) -> Any:
        if self.type == "timestamp":
            return datetime.fromtimestamp(
                util.convert_event_time_to_timestamp(value) / 1000
            ).strftime("%Y-%m-%d %H:%M:%S")
        return value

    def __lt__(self, other) -> filter.Filter:
        return filter.Filter(self, filter.Filter.LT, self._get_filter_value(other))

    def __le__(self, other) -> filter.Filter:
        return filter.Filter(self, filter.Filter.LE, self._get_filter_value(other))

    def __eq__(self, other) -> filter.Filter:
        return filter.Filter(self, filter.Filter.EQ, self._get_filter_value(other))

    def __ne__(self, other) -> filter.Filter:
        return filter.Filter(self, filter.Filter.NE, self._get_filter_value(other))

    def __ge__(self, other) -> filter.Filter:
        return filter.Filter(self, filter.Filter.GE, self._get_filter_value(other))

    def __gt__(self, other) -> filter.Filter:
        return filter.Filter(self, filter.Filter.GT, self._get_filter_value(other))

    def contains(self, other: str | list[Any]) -> filter.Filter:
        """Construct a filter similar to SQL's `IN` operator.

        Warning: Deprecated
            `contains` method is deprecated.
            Use [`Feature.isin`][hsfs.feature.Feature.isin] instead.

        Parameters:
            other: A single feature value or a list of feature values.

        Returns:
            A filter that leaves only the feature values also contained in `other`.
        """
        return self.isin(other)

    def isin(self, other: str | list[Any]) -> filter.Filter:
        return filter.Filter(self, filter.Filter.IN, json.dumps(other))

    def like(self, other: Any) -> filter.Filter:
        return filter.Filter(self, filter.Filter.LK, other)

    def __str__(self) -> str:
        return self.json()

    def __repr__(self) -> str:
        return f"Feature({self._name!r}, {self._type!r}, {self._description!r}, {self._primary}, {self._foreign}, {self._partition}, {self._online_type!r}, {self._default_value!r}, {self._feature_group_id!r})"

    def __hash__(self) -> int:
        return hash(f"{self.feature_group_id}_{self.name}")
