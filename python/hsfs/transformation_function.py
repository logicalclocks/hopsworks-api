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
from hopsworks_apigen import public
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
    """Class that store the possible types of transformation functions."""

    MODEL_DEPENDENT = "model_dependent"
    ON_DEMAND = "on_demand"
    UNDEFINED = "undefined"  # This type is used when the UDF created is not attached to a feature view / feature group. Hence the transformation function is neither model dependent nor on-demand.


@public
@typechecked
class TransformationFunction:
    NOT_FOUND_ERROR_CODE = 270160
    """
    DTO class for transformation functions.

    Parameters:
        featurestore_id : `int`. Id of the feature store in which the transformation function is saved.
        hopsworks_udf : `HopsworksUDF`. The meta data object for UDF in Hopsworks, which can be created using the `@udf` decorator.
        version : `int`. The version of the transformation function.
        id : `int`. The id of the transformation function in the feature store.
        transformation_type : `UDFType`. The type of the transformation function. Can be "on-demand" or "model-dependent"
    """

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

    @public
    def save(self) -> None:
        """Save a transformation function into the backend.

        Example:
            ```python
            # import hopsworks udf decorator
            from hopworks import udf

            # define function
            @udf(int)
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    version=1
                )

            # persist transformation function in backend
            plus_one_meta.save()
            ```
        """
        self._transformation_function_engine.save(self)

    @public
    def delete(self) -> None:
        """Delete transformation function from backend.

        Example:
            ```python
            # import hopsworks udf decorator
            from hopworks import udf

            # define function
            @udf(int)
            def plus_one(value):
                return value + 1

            # create transformation function
            plus_one_meta = fs.create_transformation_function(
                    transformation_function=plus_one,
                    version=1
                )
            # persist transformation function in backend
            plus_one_meta.save()

            # retrieve transformation function
            plus_one_fn = fs.get_transformation_function(name="plus_one")

            # delete transformation function from backend
            plus_one_fn.delete()
            ```
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

    @public
    def alias(self, *args: str):
        """Set the names of the transformed features output by the transformation function."""
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

    def executor(
        self,
        statistics: TransformationStatistics
        | list[FeatureDescriptiveStatistics]
        | dict[str, dict[str, Any]] = None,
        context: dict[str, Any] = None,
        online: bool = False,
    ) -> Any:
        """Create an executable transformation with optional statistics and context for unit testing.

        This method returns a callable object that can execute the transformation function with
        the specified configuration. It is designed for unit testing transformation functions locally.

        The executor allows you to:
        - Inject mock statistics for testing model-dependent transformations
        - Provide transformation context for testing transformation functions using context variables
        - Switch between online (single-value) and offline (batch) execution modes

        !!! example "Testing transformation with pandas execution mode"
            ```python
            @udf(return_type=float, mode="pandas")
            def add_one(value):
                return value + 1

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=add_one,
                transformation_type=TransformationType.ON_DEMAND
            )

            # Create executor and test
            executor = tf.executor()
            result = executor.execute(pd.Series([1.0, 2.0, 3.0]))
            assert result.tolist() == [2.0, 3.0, 4.0]
            ```

        !!! example "Testing transformation with python execution mode"
            ```python
            @udf(return_type=float, mode="python")
            def add_one(value):
                return value + 1

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=add_one,
                transformation_type=TransformationType.ON_DEMAND
            )

            # Create executor and test
            executor = tf.executor()
            result = executor.execute(1.0)
            assert result == 2.0
            ```

        !!! example "Testing transformation with default execution mode"
            ```python
            # In the default execution mode, Hopsworks executes the transformation function as pandas UDF for batch processing and as python function for online processing to get optimal.
            # Hence, the function should should be able to handle both online and offline execution modes and unit-test musts be written for both these use-cases.
            # In the offline mode, Hopsworks would pass a pandas Series to the function.
            # In the online mode, Hopsworks would pass a single value to the function.

            @udf(return_type=float)
            def double_value(value):
                return value * 2

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=double_value,
                transformation_type=TransformationType.ON_DEMAND
            )

            # Offline mode (batch processing with pandas Series)
            offline_executor = tf.executor(online=False)
            batch_result = offline_executor.execute(pd.Series([1.0, 2.0, 3.0]))

            # Online mode (single value processing)
            online_executor = tf.executor(online=True)
            single_result = online_executor.execute(5.0)
            assert single_result == 10.0
            ```

        !!! example "Unit test with mocked statistics"
            ```python
            from hsfs.transformation_statistics import TransformationStatistics

            @udf(return_type=float)
            def normalize(value, statistics=TransformationStatistics("value")):
                return (value - statistics.value.mean) / statistics.value.std_dev

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=normalize,
                transformation_type=TransformationType.MODEL_DEPENDENT
            )

            # Test with mock statistics
            executor = tf.executor(statistics={"value": {"mean": 100.0, "std_dev": 25.0}})
            result = executor.execute(pd.Series([100.0, 125.0, 150.0]))
            assert result.tolist() == [0.0, 1.0, 2.0]
            ```

        !!! example "Unit test with transformation context"
            ```python
            @udf(return_type=float)
            def apply_discount(price, context):
                return price * (1 - context["discount_rate"])

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=apply_discount,
                transformation_type=TransformationType.ON_DEMAND
            )

            executor = tf.executor(context={"discount_rate": 0.1})
            result = executor.execute(pd.Series([100.0, 200.0]))
            assert result.tolist() == [90.0, 180.0]
            ```

        !!! example "Testing online vs offline execution modes"
            ```python
            # For transformation functions using the default execution mode `default`.
            # The function should should be able to handle both online and offline execution modes.
            # In the offline mode, Hopsworks would pass a pandas Series to the function.
            # In the online mode, Hopsworks would pass a single value to the function.
            @udf(return_type=float, mode="default")
            def double_value(value):
                return value * 2

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=double_value,
                transformation_type=TransformationType.ON_DEMAND
            )

            # Offline mode (batch processing with pandas Series)
            offline_executor = tf.executor(online=False)
            batch_result = offline_executor.execute(pd.Series([1.0, 2.0, 3.0]))

            # Online mode (single value processing)
            online_executor = tf.executor(online=True)
            single_result = online_executor.execute(5.0)
            assert single_result == 10.0
            ```

        Parameters:
            statistics: Statistics for model-dependent transformations.
                Can be provided as:

                - `TransformationStatistics`: Pre-built statistics object
                - `dict[str, dict[str, Any]]`: Dictionary mapping feature names to their statistics (e.g., `{"amount": {"mean": 100.0, "std_dev": 25.0}}`)
                - `list[FeatureDescriptiveStatistics]`: List of statistics objects from Hopsworks
            context: A dictionary mapping variable names to values that provide contextual
                information to the transformation function at runtime.
                The keys must match parameter names defined in the transformation function.
            online: Whether to execute in online mode (single values) or offline mode (batch/vectorized).
                Only applicable when the transformation uses `mode="default"`.

        Returns:
            A callable object with an `execute(*args)` method to run the transformation.
            - pd.Series - Single output Pandas UDFs.
            - pd.DataFrame - Multi-output Pandas UDFs.
            - int | float | str | bool | datetime | time | date - Single output Python UDFs.
            - tuple[int | float | str | bool | datetime | time | date] - Multi-output Python UDFs.
        """
        return self.hopsworks_udf.executor(
            statistics=statistics, context=context, online=online
        )

    def execute(self, *args) -> Any:
        """Execute the transformation function directly with the provided arguments.

        This is a convenience method for quick testing of simple transformations that don't
        require statistics or transformation context. It executes in offline mode (batch processing).

        !!! example "Quick transformation testing"
            ```python
            @udf(return_type=float)
            def add_one(value):
                return value + 1

            tf = TransformationFunction(
                featurestore_id=1,
                hopsworks_udf=add_one,
                transformation_type=TransformationType.ON_DEMAND
            )

            # Direct execution for simple tests
            result = tf.execute(pd.Series([1.0, 2.0, 3.0]))
            assert result.tolist() == [2.0, 3.0, 4.0]
            ```

        !!! note
            For transformations that require statistics or transformation context or need to be executed in online mode, use [`executor()`][hsfs.transformation_function.TransformationFunction.executor] instead:
            ```python
            result = tf.executor(statistics=stats, context=ctx).execute(data)
            ```

        Parameters:
            *args: Input arguments matching the transformation function's parameter signature.
                For batch processing, pass pandas Series or DataFrames.

        Returns:
            The transformed values.
            - pd.Series - Single output Pandas UDFs.
            - pd.DataFrame - Multi-output Pandas UDFs.
            - int | float | str | bool | datetime | time | date - Single output Python UDFs.
            - tuple[int | float | str | bool | datetime | time | date] - Multi-output Python UDFs.
        """
        return self.hopsworks_udf.executor().execute(*args)

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

    @public
    @property
    def id(self) -> id:
        """Transformation function id."""
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @public
    @property
    def version(self) -> int:
        """Version of the transformation function."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @public
    @property
    def hopsworks_udf(self) -> HopsworksUdf:
        """Meta data class for the user defined transformation function."""
        # Make sure that the output column names for a model-dependent or on-demand transformation function, when accessed externally from the class.
        if (
            self.transformation_type
            and self.transformation_type != TransformationType.UNDEFINED
            and not self.__hopsworks_udf.output_column_names
        ):
            self.__hopsworks_udf.output_column_names = self._get_output_column_names()
        return self.__hopsworks_udf

    @public
    @property
    def transformation_type(self) -> TransformationType:
        """Type of the Transformation: can be `model dependent` or `on-demand`."""
        return self._transformation_type

    @transformation_type.setter
    def transformation_type(self, transformation_type) -> None:
        self._transformation_type = transformation_type

    @public
    @property
    def transformation_statistics(
        self,
    ) -> TransformationStatistics | None:
        """Feature statistics required for the defined UDF."""
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

    @public
    @property
    def output_column_names(self) -> list[str]:
        """Names of the output columns generated by the transformation functions."""
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
