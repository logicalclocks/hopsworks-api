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
from typing import Any, Dict, List, Optional, Union

import humps
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.constants import FEATURES
from hsfs import util
from hsfs.core import transformation_function_engine
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.decorators import typechecked
from hsfs.hopsworks_udf import HopsworksUdf
from hsfs.transformation_statistics import TransformationStatistics
from packaging.version import Version


_logger = logging.getLogger(__name__)


class TransformationType(Enum):
    """
    Class that store the possible types of transformation functions.
    """

    MODEL_DEPENDENT = "model_dependent"
    ON_DEMAND = "on_demand"
    UNDEFINED = "undefined"  # This type is used when the UDF created is not attached to a feature view / feature group. Hence the transformation function is neither model dependent nor on-demand.


@typechecked
class TransformationFunction:
    NOT_FOUND_ERROR_CODE = 270160
    """
    DTO class for transformation functions.

    # Arguments
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
        version: Optional[int] = None,
        id: Optional[int] = None,
        transformation_type: Optional[TransformationType] = None,
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
        """Save a transformation function into the backend.

        !!! example
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

    def delete(self) -> None:
        """Delete transformation function from backend.

        !!! example
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

    def __call__(self, *features: List[str]) -> TransformationFunction:
        """
        Update the feature to be using in the transformation function

        # Arguments
            features: `List[str]`. Name of features to be passed to the User Defined function
        # Returns
            `HopsworksUdf`: Meta data class for the user defined function.
        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: If the provided number of features do not match the number of arguments in the defined UDF or if the provided feature names are not strings.
        """
        # Deep copy so that the same transformation function can be used to create multiple new transformation function with different features.
        transformation = copy.deepcopy(self)
        transformation.__hopsworks_udf = transformation.__hopsworks_udf(*features)

        return transformation

    @classmethod
    def from_response_json(
        cls, json_dict: Dict[str, Any]
    ) -> Union[TransformationFunction, List[TransformationFunction]]:
        """
        Function that constructs the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
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
            else:
                return [cls(**tffn_dto) for tffn_dto in json_decamelized["items"]]
        else:
            if json_decamelized.get("hopsworks_udf", False):
                json_decamelized["hopsworks_udf"] = HopsworksUdf.from_response_json(
                    json_decamelized["hopsworks_udf"]
                )
            return cls(**json_decamelized)

    def update_from_response_json(
        self, json_dict: Dict[str, Any]
    ) -> TransformationFunction:
        """
        Function that updates the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `TransformationFunction`: Json deserialized class object.
        """
        json_decamelized = humps.decamelize(json_dict)
        self.__init__(**json_decamelized)
        return self

    def json(self) -> str:
        """
        Convert class into its json serialized form.

        # Returns
            `str`: Json serialized object.
        """
        return json.dumps(self, cls=util.Encoder)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert class into a dictionary.

        # Returns
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
        """
        Set the names of the transformed features output by the transformation function.
        """
        self.__hopsworks_udf.alias(*args)

        return self

    def _get_output_column_names(self) -> str:
        """
        Function that generates feature names for the transformed features

        # Returns
            `List[str]`: List of feature names for the transformed columns
        """
        # If function name matches the name of an input feature and the transformation function only returns one output feature then
        # then the transformed output feature would have the same name as the input feature. i.e the input feature will get overwritten.
        if (
            len(self.__hopsworks_udf.return_types) == 1
            and any(
                [
                    self.__hopsworks_udf.function_name
                    == transformation_feature.feature_name
                    for transformation_feature in self.__hopsworks_udf._transformation_features
                ]
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
                _BASE_COLUMN_NAME = f'{self.__hopsworks_udf.function_name}_{"_".join(self.__hopsworks_udf.transformation_features)}_'
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
                        f"{output_col_name[:FEATURES.MAX_LENGTH_NAME-(len(output_col_names) + 1)]}_{str(index)}"
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
        """
        Function that returns validates if the defined transformation function can be used for the specified UDF type.

        # Raises
            `hopsworks.client.exceptions.FeatureStoreException` : If the UDF Type is None or if statistics or multiple columns has been output by a on-demand transformation function
        """

        if transformation_type == TransformationType.ON_DEMAND:
            if hopsworks_udf.statistics_required:
                raise FeatureStoreException(
                    "On-Demand Transformation functions cannot use statistics, please remove statistics parameters from the functions"
                )

    @property
    def id(self) -> id:
        """Transformation function id."""
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @property
    def version(self) -> int:
        """Version of the transformation function."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

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

    @property
    def transformation_type(self) -> TransformationType:
        """Type of the Transformation : Can be \"model dependent\" or \"on-demand\" """
        return self._transformation_type

    @transformation_type.setter
    def transformation_type(self, transformation_type) -> None:
        self._transformation_type = transformation_type

    @property
    def transformation_statistics(
        self,
    ) -> Optional[TransformationStatistics]:
        """Feature statistics required for the defined UDF"""
        return self.__hopsworks_udf.transformation_statistics

    @transformation_statistics.setter
    def transformation_statistics(
        self, statistics: List[FeatureDescriptiveStatistics]
    ) -> None:
        self.__hopsworks_udf.transformation_statistics = statistics
        # Generate output column names for one-hot encoder after transformation statistics is set.
        # This is done because the number of output columns for one-hot encoding dependents on number of unique values in training dataset statistics.
        if self.__hopsworks_udf.function_name == "one_hot_encoder":
            self.__hopsworks_udf.output_column_names = self._get_output_column_names()

    @property
    def output_column_names(self) -> List[str]:
        """Names of the output columns generated by the transformation functions"""
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
        elif self.transformation_type == TransformationType.ON_DEMAND:
            return f"On-Demand Transformation Function : {repr(self.__hopsworks_udf)}"
        else:
            return f"Transformation Function : {repr(self.__hopsworks_udf)}"

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()
