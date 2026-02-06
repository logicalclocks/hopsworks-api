#
#  Copyright 2021. Logical Clocks AB
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
from __future__ import annotations

import copy
import json
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

import humps
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.constants import FEATURES
from hsfs import util
from hsfs.core import transformation_function_engine
from hsfs.decorators import typechecked
from hsfs.hopsworks_udf import HopsworksUdf
from packaging.version import Version


if TYPE_CHECKING:
    from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
    from hsfs.transformation_statistics import TransformationStatistics


_logger = logging.getLogger(__name__)


class TransformationType(Enum):
    """Type of transformation function, determining when and how it is executed.

    Hopsworks supports two types of transformation functions that serve different purposes
    in the ML pipeline:

    Attributes:
        MODEL_DEPENDENT: Transformations attached to feature views that are parameterized by training dataset statistics.
            These transformations (e.g., scaling, encoding) use statistics like mean, std_dev, min, max
            computed from the training data. They are applied consistently during:

            - Training dataset creation
            - Batch inference via `get_batch_data()`
            - Online inference via `get_feature_vector()`

            Model-dependent transformations prevent training-serving skew by ensuring the same
            statistical parameters are used in both training and inference.

            Example use cases: min-max scaling, standard scaling, label encoding, one-hot encoding.

        ON_DEMAND: Transformations attached to feature groups that compute features at request time.
            These transformations typically require parameters only available during inference
            (e.g., current timestamp, request metadata). They are:

            - Executed during data insertion to backfill historical values
            - Computed in real-time during online inference using `request_parameters`
            - Cannot access training dataset statistics

            Example use cases: time since last event, geolocation lookups, real-time calculations.

        UNDEFINED: Internal state for transformation functions not yet attached to a feature view or feature group.
            This is a transient state; the type is set to MODEL_DEPENDENT or ON_DEMAND when
            the transformation is attached.

    See Also:
        - [`FeatureView`][hsfs.feature_view.FeatureView]: Attach transformations as MODEL_DEPENDENT.
        - [`FeatureGroup`][hsfs.feature_group.FeatureGroup]: Attach transformations as ON_DEMAND.
        - [`TransformationStatistics`][hsfs.transformation_statistics.TransformationStatistics]: Statistics available to MODEL_DEPENDENT transformations.
    """

    MODEL_DEPENDENT = "model_dependent"
    ON_DEMAND = "on_demand"
    UNDEFINED = "undefined"


@typechecked
class TransformationFunction:
    """A transformation function that can be saved to the feature store and attached to feature views or feature groups.

    Transformation functions wrap a [`HopsworksUdf`][hsfs.hopsworks_udf.HopsworksUdf] (created via the
    [`@udf`][hsfs.hopsworks_udf.udf] decorator) with metadata for persistence and type classification.

    There are two types of transformation functions:

    - **Model-Dependent**: Attached to feature views, these transformations have access to training dataset
      statistics and are applied during training data creation, batch inference, and online inference.
      Use for scaling, encoding, and other transformations that must be consistent between training and serving.

    - **On-Demand**: Attached to feature groups, these transformations compute features that require
      request-time parameters. They are executed during data insertion (to backfill historical values)
      and in real-time during online inference.

    Example: Create and save a transformation function
        ```python
        from hopsworks import udf

        @udf(return_type=float)
        def add_one(feature):
            return feature + 1

        # Create transformation function metadata
        tf = fs.create_transformation_function(
            transformation_function=add_one,
            version=1
        )

        # Save to feature store for reuse
        tf.save()
        ```

    Example: Retrieve and attach to a feature view (model-dependent)
        ```python
        # Retrieve saved transformation function
        add_one_tf = fs.get_transformation_function(name="add_one", version=1)

        # Attach to feature view - becomes model-dependent
        feature_view = fs.create_feature_view(
            name="my_view",
            query=fg.select_all(),
            transformation_functions=[add_one_tf("my_feature")]
        )
        ```

    Example: Attach to a feature group (on-demand)
        ```python
        @udf(return_type=int, drop=["event_time"])
        def days_since_event(event_time, context):
            return (context["current_time"] - event_time).dt.days

        # Attach to feature group - becomes on-demand
        fg = fs.create_feature_group(
            name="my_fg",
            transformation_functions=[days_since_event]
        )
        ```

    Note: Output Column Naming
        By default, output columns are named using the pattern:

        - Model-dependent: `{function_name}_{input_features}` (e.g., `min_max_scaler_age`)
        - On-demand with single output: `{function_name}` (e.g., `days_since_event`)
        - Multiple outputs: `{base_name}_{index}` (e.g., `split_feature_0`, `split_feature_1`)

        Use the [`alias()`][hsfs.transformation_function.TransformationFunction.alias] method to set custom names.

    See Also:
        - [`udf`][hsfs.hopsworks_udf.udf]: Decorator to create transformation functions.
        - [`FeatureStore.create_transformation_function`][hsfs.feature_store.FeatureStore.create_transformation_function]: Create a TransformationFunction.
        - [`FeatureStore.get_transformation_function`][hsfs.feature_store.FeatureStore.get_transformation_function]: Retrieve a saved TransformationFunction.
        - [`TransformationType`][hsfs.transformation_function.TransformationType]: Enum describing transformation types.
    """

    NOT_FOUND_ERROR_CODE = 270160

    def __init__(
        self,
        featurestore_id: int,
        hopsworks_udf: HopsworksUdf,
        version: int | None = None,
        id: int | None = None,
        transformation_type: TransformationType | None = None,
        type=None,
        items=None,
        count=None,
        href=None,
        **kwargs,
    ):
        self._id: int = id
        self._featurestore_id: int = featurestore_id
        self._version: int = version

        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                self._featurestore_id
            )
        )
        if not isinstance(hopsworks_udf, HopsworksUdf):
            raise FeatureStoreException(
                "Please use the hopsworks_udf decorator when defining transformation functions."
            )

        self.__hopsworks_udf: HopsworksUdf = hopsworks_udf
        TransformationFunction._validate_transformation_type(
            transformation_type=transformation_type, hopsworks_udf=hopsworks_udf
        )

        # Setting transformation type as unknown when the transformation function is not attached to a feature view / feature group.
        # This happens for example when the transformation function is fetched from the backend.
        self.transformation_type = (
            transformation_type if transformation_type else TransformationType.UNDEFINED
        )

        if self.__hopsworks_udf._generate_output_col_name:
            # Reset output column names so that they would be regenerated.
            # Handles the use case in which the same UDF is used to define both on-demand and model dependent transformations.
            self.__hopsworks_udf._output_column_names = []

    def save(self) -> None:
        """Save this transformation function to the feature store for later reuse.

        Once saved, the transformation function can be retrieved by name and version
        using [`FeatureStore.get_transformation_function()`][hsfs.feature_store.FeatureStore.get_transformation_function].

        This enables sharing transformation functions across feature views and projects,
        and ensures consistent transformations are applied to the same features.

        Example: Save and reuse a transformation function
            ```python
            from hopsworks import udf

            @udf(return_type=float)
            def normalize(value):
                return value / 100.0

            # Create and save
            tf = fs.create_transformation_function(
                transformation_function=normalize,
                version=1
            )
            tf.save()

            # Later, retrieve and use
            normalize_fn = fs.get_transformation_function(name="normalize", version=1)
            feature_view = fs.create_feature_view(
                name="my_view",
                query=fg.select_all(),
                transformation_functions=[normalize_fn("my_feature")]
            )
            ```

        Raises:
            FeatureStoreException: If a transformation function with the same name and version already exists.

        See Also:
            - [`delete()`][hsfs.transformation_function.TransformationFunction.delete]: Remove from feature store.
            - [`FeatureStore.get_transformation_function()`][hsfs.feature_store.FeatureStore.get_transformation_function]: Retrieve saved functions.
        """
        self._transformation_function_engine.save(self)

    def delete(self) -> None:
        """Delete this transformation function from the feature store.

        Removes the transformation function metadata from the feature store.
        This does not affect feature views or feature groups that already use this transformation.

        Example: Delete a transformation function
            ```python
            # Retrieve the transformation function
            tf = fs.get_transformation_function(name="my_transform", version=1)

            # Delete it
            tf.delete()
            ```

        Raises:
            FeatureStoreException: If the transformation function does not exist or cannot be deleted.

        See Also:
            - [`save()`][hsfs.transformation_function.TransformationFunction.save]: Save to feature store.
        """
        self._transformation_function_engine.delete(self)

    def __call__(self, *features: list[str]) -> TransformationFunction:
        """Update the feature to be using in the transformation function.

        Parameters:
            features: `List[str]`. Name of features to be passed to the User Defined function.

        Returns:
            `HopsworksUdf`: Meta data class for the user defined function.

        Raises:
            `hopsworks.client.exceptions.FeatureStoreException`: If the provided number of features do not match the number of arguments in the defined UDF or if the provided feature names are not strings.
        """
        # Deep copy so that the same transformation function can be used to create multiple new transformation function with different features.
        transformation = copy.deepcopy(self)
        transformation.__hopsworks_udf = transformation.__hopsworks_udf(*features)

        return transformation

    @classmethod
    def from_response_json(
        cls, json_dict: dict[str, Any]
    ) -> TransformationFunction | list[TransformationFunction]:
        """Function that constructs the class object from its json serialization.

        Parameters:
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.

        Returns:
            `TransformationFunction`: Json deserialized class object.
        """
        json_decamelized = humps.decamelize(json_dict)

        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            for tffn_dto in json_decamelized["items"]:
                if tffn_dto.get("hopsworks_udf", False):
                    tffn_dto["hopsworks_udf"] = HopsworksUdf.from_response_json(
                        tffn_dto["hopsworks_udf"]
                    )
            if json_decamelized["count"] == 1:
                return cls(**json_decamelized["items"][0])
            return [cls(**tffn_dto) for tffn_dto in json_decamelized["items"]]
        if json_decamelized.get("hopsworks_udf", False):
            json_decamelized["hopsworks_udf"] = HopsworksUdf.from_response_json(
                json_decamelized["hopsworks_udf"]
            )
        return cls(**json_decamelized)

    def update_from_response_json(
        self, json_dict: dict[str, Any]
    ) -> TransformationFunction:
        """Function that updates the class object from its json serialization.

        Parameters:
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.

        Returns:
            `TransformationFunction`: Json deserialized class object.
        """
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        """Convert class into its json serialized form.

        Returns:
            `str`: Json serialized object.
        """
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> dict[str, Any]:
        """Convert class into a dictionary.

        Returns:
            `Dict`: Dictionary that contains all data required to json serialize the object.
        """
        backend_version = client.get_connection().backend_version

        return {
            "id": self._id,
            "version": self._version,
            "featurestoreId": self._featurestore_id,
            "hopsworksUdf": self.hopsworks_udf.to_dict(),
            **(
                {"transformationType": self.transformation_type.value}
                if Version(backend_version) > Version("4.1.6")
                else {}
            ),  # This check is added for backward compatibility with older versions of Hopsworks. The "transformationType" field was added for equality checking of transformation functions and versions below 4.1.6 do not support unknown fields in the backend.
        }

    def alias(self, *args: str):
        """Set custom names for the output features produced by this transformation.

        By default, Hopsworks generates output feature names using the pattern:

        - Model-dependent: `{function_name}_{input_features}` (e.g., `min_max_scaler_age`)
        - On-demand single output: `{function_name}` (e.g., `days_since_event`)
        - Multiple outputs: `{base_name}_{index}` (e.g., `split_0`, `split_1`)

        Use `alias()` to override these defaults with meaningful names.
        Each name must be unique and at most 63 characters long.

        Example: Set custom output names
            ```python
            from hopsworks import udf

            @udf(return_type=[float, float])
            def normalize_coords(lat, lon):
                return lat / 90.0, lon / 180.0

            # Set custom names for the two output features
            normalize_coords.alias("normalized_latitude", "normalized_longitude")

            feature_view = fs.create_feature_view(
                name="locations",
                query=fg.select_all(),
                transformation_functions=[normalize_coords("latitude", "longitude")]
            )
            ```

        Parameters:
            *args: Output feature names.
                The number of names must match the number of outputs from the transformation.

        Returns:
            This TransformationFunction instance (for method chaining).

        Raises:
            FeatureStoreException: If the number of names doesn't match the number of outputs,
                names are not unique, or names exceed the 63-character limit.
        """
        self.__hopsworks_udf.alias(*args)

        return self

    def _get_output_column_names(self) -> list[str]:
        """Function that generates feature names for the transformed features.

        Returns:
            List of feature names for the transformed columns.
        """
        # If function name matches the name of an input feature and the transformation function only returns one output feature then
        # then the transformed output feature would have the same name as the input feature. i.e the input feature will get overwritten.
        if (
            len(self.__hopsworks_udf.return_types) == 1
            and any(
                self.__hopsworks_udf.function_name
                == transformation_feature.feature_name
                for transformation_feature in self.__hopsworks_udf._transformation_features
            )
            and (
                not self.__hopsworks_udf.dropped_features
                or self.__hopsworks_udf.function_name
                not in self.__hopsworks_udf.dropped_features
            )
        ):
            output_col_names = [self.__hopsworks_udf.function_name]
        else:
            if self.transformation_type == TransformationType.MODEL_DEPENDENT:
                _BASE_COLUMN_NAME = f"{self.__hopsworks_udf.function_name}_{'_'.join(self.__hopsworks_udf.transformation_features)}_"
            elif self.transformation_type == TransformationType.ON_DEMAND:
                _BASE_COLUMN_NAME = (
                    self.__hopsworks_udf.function_name
                    if len(self.__hopsworks_udf.return_types) == 1
                    else f"{self.__hopsworks_udf.function_name}_"
                )

            output_col_names = (
                [
                    f"{_BASE_COLUMN_NAME}{i}"
                    for i in range(len(self.__hopsworks_udf.return_types))
                ]
                if len(self.__hopsworks_udf.return_types) > 1
                else [_BASE_COLUMN_NAME]
            )

        if any(
            len(output_col_name) > FEATURES.MAX_LENGTH_NAME
            for output_col_name in output_col_names
        ):
            _logger.warning(
                f"The default output feature names generated by the transformation function {repr(self.__hopsworks_udf)} exceed the maximum allowed length of {FEATURES.MAX_LENGTH_NAME} characters. Default names have been truncated to fit within the size limit. To avoid this, consider using the alias function to explicitly specify output column names."
            )
            if len(output_col_names) > 1:
                # Slicing the output column names
                for index, output_col_name in enumerate(output_col_names):
                    output_col_names[index] = (
                        f"{output_col_name[: FEATURES.MAX_LENGTH_NAME - (len(output_col_names) + 1)]}_{str(index)}"
                        if len(output_col_name) > FEATURES.MAX_LENGTH_NAME
                        else output_col_name
                    )
            else:
                output_col_names = [output_col_names[0][: FEATURES.MAX_LENGTH_NAME]]

        return output_col_names

    @staticmethod
    def _validate_transformation_type(
        transformation_type: TransformationType, hopsworks_udf: HopsworksUdf
    ):
        """Function that returns validates if the defined transformation function can be used for the specified UDF type.

        Raises:
            `hopsworks.client.exceptions.FeatureStoreException` : If the UDF Type is None or if statistics or multiple columns has been output by a on-demand transformation function
        """
        if (
            transformation_type == TransformationType.ON_DEMAND
            and hopsworks_udf.statistics_required
        ):
            raise FeatureStoreException(
                "On-Demand Transformation functions cannot use statistics, please remove statistics parameters from the functions"
            )

    @property
    def id(self) -> id:
        """Unique identifier of this transformation function in the feature store.

        Assigned by Hopsworks when the transformation function is saved.
        Returns `None` for unsaved transformation functions.
        """
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @property
    def version(self) -> int:
        """Version number of this transformation function.

        Enables versioning of transformation functions with the same name.
        Use different versions to track changes to transformation logic over time.
        """
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def hopsworks_udf(self) -> HopsworksUdf:
        """The underlying UDF metadata containing the transformation logic.

        Provides access to the [`HopsworksUdf`][hsfs.hopsworks_udf.HopsworksUdf] instance
        which contains the function source code, input/output mappings, and execution configuration.
        """
        # Make sure that the output column names for a model-dependent or on-demand transformation function, when accessed externally from the class.
        if (
            self.transformation_type
            and self.transformation_type != TransformationType.UNDEFINED
            and not self.__hopsworks_udf.output_column_names
        ):
            self.__hopsworks_udf.output_column_names = self._get_output_column_names()
        return self.__hopsworks_udf

    @property
    def transformation_type(self) -> TransformationType:
        """Type of this transformation: MODEL_DEPENDENT or ON_DEMAND.

        - `MODEL_DEPENDENT`: Attached to feature views, has access to training dataset statistics.
        - `ON_DEMAND`: Attached to feature groups, computed at request time.
        - `UNDEFINED`: Not yet attached to a feature view or feature group.

        See Also:
            - [`TransformationType`][hsfs.transformation_function.TransformationType]: Full description of each type.
        """
        return self._transformation_type

    @transformation_type.setter
    def transformation_type(self, transformation_type) -> None:
        self._transformation_type = transformation_type

    @property
    def transformation_statistics(
        self,
    ) -> TransformationStatistics | None:
        """Training dataset statistics available to this transformation (model-dependent only).

        For model-dependent transformations, this provides access to statistics like mean, stddev,
        min, max computed from the training dataset. Returns `None` for on-demand transformations.

        Statistics are populated when a training dataset is created or when serving is initialized.

        See Also:
            - [`TransformationStatistics`][hsfs.transformation_statistics.TransformationStatistics]: Container for feature statistics.
        """
        return self.__hopsworks_udf.transformation_statistics

    @transformation_statistics.setter
    def transformation_statistics(
        self, statistics: list[FeatureDescriptiveStatistics]
    ) -> None:
        self.__hopsworks_udf.transformation_statistics = statistics
        # Generate output column names for one-hot encoder after transformation statistics is set.
        # This is done because the number of output columns for one-hot encoding dependents on number of unique values in training dataset statistics.
        if self.__hopsworks_udf.function_name == "one_hot_encoder":
            self.__hopsworks_udf.output_column_names = self._get_output_column_names()

    @property
    def output_column_names(self) -> list[str]:
        """Names of the output features produced by this transformation.

        Output names are either:

        - Custom names set via [`alias()`][hsfs.transformation_function.TransformationFunction.alias]
        - Auto-generated names following the pattern:
            - Model-dependent: `{function_name}_{input_features}` (e.g., `min_max_scaler_age`)
            - On-demand single output: `{function_name}` (e.g., `days_since_event`)
            - Multiple outputs: `{base_name}_{index}` (e.g., `one_hot_encoder_category_0`)

        Example: Access output column names
            ```python
            tf = min_max_scaler("age")
            print(tf.output_column_names)
            # Output: ["min_max_scaler_age"]
            ```
        """
        if (
            self.__hopsworks_udf.function_name == "one_hot_encoder"
            and len(self.__hopsworks_udf.output_column_names)
            != len(self.__hopsworks_udf.return_types)
        ) or not self.__hopsworks_udf.output_column_names:
            self.__hopsworks_udf.output_column_names = self._get_output_column_names()
        return self.__hopsworks_udf.output_column_names

    def __repr__(self):
        if self.transformation_type == TransformationType.MODEL_DEPENDENT:
            return f"Model-Dependent Transformation Function : {repr(self.__hopsworks_udf)}"
        if self.transformation_type == TransformationType.ON_DEMAND:
            return f"On-Demand Transformation Function : {repr(self.__hopsworks_udf)}"
        return f"Transformation Function : {repr(self.__hopsworks_udf)}"

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()
