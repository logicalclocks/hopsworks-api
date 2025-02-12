#
#   Copyright 2022 Hopsworks AB
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


import logging

import pandas as pd
import pytest
from hopsworks_common import version
from hsfs.client.exceptions import FeatureStoreException
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationFunction, TransformationType
from packaging.version import Version


class TestTransformationFunction:
    def test_from_response_json_one_argument_no_statistics(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_one_argument_no_statistics_function"
        ]["response"]
        json["transformation_type"] = TransformationType.MODEL_DEPENDENT
        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_one_fs"
        assert tf.hopsworks_udf.return_types == ["double"]
        assert not tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["col1"]
        assert tf.hopsworks_udf.statistics_features == []
        assert tf.hopsworks_udf._statistics_argument_names == []
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )

    def test_from_response_json_one_argument_with_statistics(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_one_argument_with_statistics_function"
        ]["response"]
        json["transformation_type"] = TransformationType.MODEL_DEPENDENT

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_mean_fs"
        assert tf.hopsworks_udf.return_types == ["double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["data"]
        assert tf.hopsworks_udf.statistics_features == ["data"]
        assert tf.hopsworks_udf._statistics_argument_names == ["data1"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics=stats):\n    return data1 + statistics.data1.mean\n"
        )

    def test_from_response_json_multiple_argument_with_statistics(
        self, backend_fixtures
    ):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_multiple_argument_with_statistics_function"
        ]["response"]
        json["transformation_type"] = TransformationType.MODEL_DEPENDENT

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "test_func"
        assert tf.hopsworks_udf.return_types == ["string"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == [
            "feature1",
            "feature2",
            "feature3",
        ]
        assert tf.hopsworks_udf.statistics_features == ["feature1", "feature2"]
        assert tf.hopsworks_udf._statistics_argument_names == ["data1", "data2"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(str)\ndef test_func(data1 : pd.Series, data2, data3, statistics=stats):\n    return data1 + statistics.data1.mean\n"
        )

    def test_from_response_json_multiple_return_type_functions(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"][
            "get_multiple_return_type_functions"
        ]["response"]
        json["transformation_type"] = TransformationType.MODEL_DEPENDENT

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "test_func"
        assert tf.hopsworks_udf.return_types == ["string", "double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == [
            "feature1",
            "feature2",
            "feature3",
        ]
        assert tf.hopsworks_udf.statistics_features == ["feature1", "feature2"]
        assert tf.hopsworks_udf._statistics_argument_names == ["data1", "data2"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(str, float)\ndef test_func(data1 : pd.Series, data2, data3, statistics=stats):\n    return pd.DataFrame('col1': ['a', 'b'], 'col2':[1,2])\n"
        )

    def test_from_response_json_list_empty(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list_empty"]["response"]

        # Act
        tf_list = TransformationFunction.from_response_json(json)

        # Assert
        assert len(tf_list) == 0

    def test_from_response_json_list(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list"]["response"]
        for response_json in json["items"]:
            response_json["transformation_type"] = TransformationType.MODEL_DEPENDENT

        # Act
        tf_list = TransformationFunction.from_response_json(json)

        # Assert
        assert len(tf_list) == 2
        tf = tf_list[0]
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_mean_fs"
        assert tf.hopsworks_udf.return_types == ["double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["data"]
        assert tf.hopsworks_udf.statistics_features == ["data"]
        assert tf.hopsworks_udf._statistics_argument_names == ["data1"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics=stats):\n    return data1 + statistics.data1.mean\n"
        )

        tf = tf_list[1]
        assert tf.id == 2
        assert tf._featurestore_id == 11
        assert tf.version == 1
        assert tf.hopsworks_udf.function_name == "add_one_fs"
        assert tf.hopsworks_udf.return_types == ["double"]
        assert not tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["col1"]
        assert tf.hopsworks_udf.statistics_features == []
        assert tf.hopsworks_udf._statistics_argument_names == []
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )

    def test_from_response_json_list_one_argument(self, backend_fixtures):
        # Arrange
        json = backend_fixtures["transformation_function"]["get_list_one_argument"][
            "response"
        ]
        for response_json in json["items"]:
            response_json["transformation_type"] = TransformationType.MODEL_DEPENDENT

        # Act
        tf = TransformationFunction.from_response_json(json)

        # Assert
        assert not isinstance(tf, list)
        assert tf.id == 1
        assert tf._featurestore_id == 11
        assert tf.version == 2
        assert tf.hopsworks_udf.function_name == "add_mean_fs"
        assert tf.hopsworks_udf.return_types == ["double"]
        assert tf.hopsworks_udf.statistics_required
        assert tf.hopsworks_udf.transformation_features == ["data"]
        assert tf.hopsworks_udf.statistics_features == ["data"]
        assert tf.hopsworks_udf._statistics_argument_names == ["data1"]
        assert (
            tf.hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics=stats):\n    return data1 + statistics.data1.mean\n"
        )

    def test_transformation_function_definition_no_hopworks_udf(self):
        def test(col1):
            return col1 + 1

        with pytest.raises(FeatureStoreException) as exception:
            TransformationFunction(
                featurestore_id=10,
                hopsworks_udf=test,
                transformation_type=TransformationType.MODEL_DEPENDENT,
            )

        assert (
            str(exception.value)
            == "Please use the hopsworks_udf decorator when defining transformation functions."
        )

    def test_transformation_function_definition_with_hopworks_udf(self, mocker):
        mocked_connection = mocker.MagicMock()
        mocked_connection.backend_version = (
            version.__version__
        )  # Mocking backend version to be the same as the current version
        mocked_connection = mocker.patch(
            "hopsworks_common.client.get_connection", return_value=mocked_connection
        )

        @udf(int)
        def test2(col1):
            return col1 + 1

        tf = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test2,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        # Creating dict representation of udf.
        udf_json = test2.to_dict()
        # Adding output column names to dict for testing since it would be generated when UDF is accessed out the transformation function.
        udf_json["outputColumnNames"] = ["test2_col1_"]

        assert tf.hopsworks_udf.to_dict() == udf_json

    def test_generate_output_column_names_one_argument_one_output_type_mdt(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == ["test_func_col1_"]

    def test_generate_output_column_names_one_argument_one_output_type_odt(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]

    def test_generate_output_column_names_one_argument_one_output_type_prefix_mdt(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func._feature_name_prefix = "prefix_"

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == ["test_func_prefix_col1_"]

    def test_generate_output_column_names_one_argument_one_output_type_prefix_odt(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func._feature_name_prefix = "prefix_"

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]
        assert odt.output_column_names == ["prefix_test_func"]

    def test_generate_output_column_names_multiple_argument_one_output_type_mdt(self):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == ["test_func_col1_col2_col3_"]

    def test_generate_output_column_names_multiple_argument_one_output_type_odt(self):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]

    def test_generate_output_column_names_multiple_argument_one_output_type_prefix_mdt(
        self,
    ):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        test_func._feature_name_prefix = "prefix_"

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == [
            "test_func_prefix_col1_prefix_col2_prefix_col3_"
        ]
        assert mdt.output_column_names == [
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_"
        ]

    def test_generate_output_column_names_multiple_argument_one_output_type_prefix_odt(
        self,
    ):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        test_func._feature_name_prefix = "prefix_"

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]
        assert odt.output_column_names == ["prefix_test_func"]

    def test_generate_output_column_names_single_argument_multiple_output_type_mdt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col1 + 1], "col3": [col1 + 1]}
            )

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == [
            "test_func_col1_0",
            "test_func_col1_1",
            "test_func_col1_2",
        ]

    def test_generate_output_column_names_single_argument_multiple_output_type_odt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col1 + 1], "col3": [col1 + 1]}
            )

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == [
            "test_func_0",
            "test_func_1",
            "test_func_2",
        ]
        assert odt.output_column_names == ["test_func_0", "test_func_1", "test_func_2"]

    def test_generate_output_column_names_single_argument_multiple_output_type_prefix_mdt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col1 + 1], "col3": [col1 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == [
            "test_func_prefix_col1_0",
            "test_func_prefix_col1_1",
            "test_func_prefix_col1_2",
        ]
        assert mdt.output_column_names == [
            "prefix_test_func_prefix_col1_0",
            "prefix_test_func_prefix_col1_1",
            "prefix_test_func_prefix_col1_2",
        ]

    def test_generate_output_column_names_single_argument_multiple_output_type_prefix_odt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col1 + 1], "col3": [col1 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )

        assert odt._get_output_column_names() == [
            "test_func_0",
            "test_func_1",
            "test_func_2",
        ]
        assert odt.output_column_names == [
            "prefix_test_func_0",
            "prefix_test_func_1",
            "prefix_test_func_2",
        ]

    def test_generate_output_column_names_multiple_argument_multiple_output_type_mdt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == [
            "test_func_col1_col2_col3_0",
            "test_func_col1_col2_col3_1",
            "test_func_col1_col2_col3_2",
        ]
        assert mdt.output_column_names == [
            "test_func_col1_col2_col3_0",
            "test_func_col1_col2_col3_1",
            "test_func_col1_col2_col3_2",
        ]

    def test_generate_output_column_names_multiple_argument_multiple_output_type_odt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )

        assert odt._get_output_column_names() == [
            "test_func_0",
            "test_func_1",
            "test_func_2",
        ]
        assert odt.output_column_names == ["test_func_0", "test_func_1", "test_func_2"]

    def test_generate_output_column_names_multiple_argument_multiple_output_type_prefix_mdt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == [
            "test_func_prefix_col1_prefix_col2_prefix_col3_0",
            "test_func_prefix_col1_prefix_col2_prefix_col3_1",
            "test_func_prefix_col1_prefix_col2_prefix_col3_2",
        ]
        assert mdt.output_column_names == [
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_0",
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_1",
            "prefix_test_func_prefix_col1_prefix_col2_prefix_col3_2",
        ]

    def test_generate_output_column_names_multiple_argument_multiple_output_type_prefix_odt(
        self,
    ):
        @udf([int, float, int])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )

        assert odt._get_output_column_names() == [
            "test_func_0",
            "test_func_1",
            "test_func_2",
        ]
        assert odt.output_column_names == [
            "prefix_test_func_0",
            "prefix_test_func_1",
            "prefix_test_func_2",
        ]

    def test_validate_udf_type_on_demand_statistics(self):
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1")

        @udf(int)
        def test_func(col1, statistics=stats):
            return col1 + statistics.col1.mean

        with pytest.raises(FeatureStoreException) as exe:
            TransformationFunction(
                featurestore_id=10,
                hopsworks_udf=test_func,
                transformation_type=TransformationType.ON_DEMAND,
            )

        assert (
            str(exe.value)
            == "On-Demand Transformation functions cannot use statistics, please remove statistics parameters from the functions"
        )

    def test_alias_one_output_mdt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mdt = mdt.alias("feature_plus_one_mdt")

        assert mdt.output_column_names == ["feature_plus_one_mdt"]

    def test_alias_one_output_odt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        odt = odt.alias("feature_plus_one_odt")

        assert odt.output_column_names == ["feature_plus_one_odt"]

    def test_alias_one_output_list_mdt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mdt = mdt.alias(["feature_plus_one_mdt"])

        assert mdt.output_column_names == ["feature_plus_one_mdt"]

    def test_alias_one_output_list_odt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        odt = odt.alias(["feature_plus_one_odt"])

        assert odt.output_column_names == ["feature_plus_one_odt"]

    def test_alias_multiple_output_mdt(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_and_sub,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mdt = mdt.alias("feature_plus_one_mdt", "feature_minus_one_mdt")

        assert mdt.output_column_names == [
            "feature_plus_one_mdt",
            "feature_minus_one_mdt",
        ]

    def test_alias_multiple_output_list_mdt(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_and_sub,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mdt = mdt.alias(["feature_plus_one_mdt", "feature_minus_one_mdt"])

        assert mdt.output_column_names == [
            "feature_plus_one_mdt",
            "feature_minus_one_mdt",
        ]

    def test_alias_invalid_number_column_names_mdt(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_and_sub,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with pytest.raises(FeatureStoreException) as exp:
            mdt.alias(["feature_plus_one", "feature_minus_one", "invalid_col"])

        assert (
            str(exp.value)
            == "The number of output feature names provided does not match the number of features returned by the transformation function 'add_and_sub(feature)'. Pease provide exactly 2 feature name(s) to match the output."
        )

    def test_alias_invalid_number_column_names_odt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        with pytest.raises(FeatureStoreException) as exp:
            odt.alias(["feature_plus_one", "feature_minus_one", "invalid_col"])

        assert (
            str(exp.value)
            == "The number of output feature names provided does not match the number of features returned by the transformation function 'add_one(feature)'. Pease provide exactly 1 feature name(s) to match the output."
        )

    def test_alias_invalid_type_mdt(self):
        @udf([int])
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with pytest.raises(FeatureStoreException) as exp:
            mdt.alias({"name": "col1"})

        assert (
            str(exp.value)
            == "Invalid output feature names provided for the transformation function 'add_one(feature)'. Please ensure all arguments are strings."
        )

    def test_alias_invalid_type_odt(self):
        @udf([int])
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        with pytest.raises(FeatureStoreException) as exp:
            odt.alias({"name": "col1"})

        assert (
            str(exp.value)
            == "Invalid output feature names provided for the transformation function 'add_one(feature)'. Please ensure all arguments are strings."
        )

    def test_alias_duplicates_mdt(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_and_sub,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with pytest.raises(FeatureStoreException) as exp:
            mdt.alias("feature_plus_one", "feature_plus_one")

        assert (
            str(exp.value)
            == "Duplicate output feature names provided for the transformation function 'add_and_sub(feature)'. Please ensure all arguments names are unique."
        )

    def test_call_and_alias_mdt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mdt = mdt("feature2_mdt").alias(["feature_plus_one_mdt"])

        assert mdt.output_column_names == ["feature_plus_one_mdt"]
        assert mdt.hopsworks_udf.transformation_features == ["feature2_mdt"]

    def test_call_and_alias_odt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        odt = odt("feature2_odt").alias(["feature_plus_one_odt"])

        assert odt.output_column_names == ["feature_plus_one_odt"]
        assert odt.hopsworks_udf.transformation_features == ["feature2_odt"]

    def test_alias_invalid_length_mdt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with pytest.raises(FeatureStoreException) as exp:
            mdt.alias(["invalid" * 10])

        assert (
            str(exp.value)
            == "Invalid output feature names specified for the transformation function 'add_one(feature)'. Please provide names shorter than 63 characters."
        )

    def test_alias_invalid_length_odt(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        with pytest.raises(FeatureStoreException) as exp:
            odt.alias(["invalid" * 10])

        assert (
            str(exp.value)
            == "Invalid output feature names specified for the transformation function 'add_one(feature)'. Please provide names shorter than 63 characters."
        )

    def test_generated_output_col_name_invalid_mdt(self, caplog):
        @udf(int)
        def test(
            long_feature_name1,
            long_feature_name2,
            long_feature_name3,
            long_feature_name4,
            long_feature_name5,
            long_feature_name6,
            long_feature_name7,
        ):
            return long_feature_name1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with caplog.at_level(logging.WARNING):
            mdt._get_output_column_names()

        assert (
            "The default output feature names generated by the transformation function test(long_feature_name1, long_feature_name2, long_feature_name3, long_feature_name4, long_feature_name5, long_feature_name6, long_feature_name7) exceed the maximum allowed length of 63 characters. Default names have been truncated to fit within the size limit. To avoid this, consider using the alias function to explicitly specify output column names."
            in caplog.text
        )

        assert mdt.output_column_names == [
            "test_long_feature_name1_long_feature_name2_long_feature_name3_l"
        ]

    def test_generated_output_col_name_invalid_mdt_multiple_returns(self, caplog):
        @udf([int, int, int])
        def test(
            long_feature_name1,
            long_feature_name2,
            long_feature_name3,
            long_feature_name4,
            long_feature_name5,
            long_feature_name6,
            long_feature_name7,
        ):
            return long_feature_name1, long_feature_name2, long_feature_name3

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with caplog.at_level(logging.WARNING):
            mdt._get_output_column_names()

        assert (
            "The default output feature names generated by the transformation function test(long_feature_name1, long_feature_name2, long_feature_name3, long_feature_name4, long_feature_name5, long_feature_name6, long_feature_name7) exceed the maximum allowed length of 63 characters. Default names have been truncated to fit within the size limit. To avoid this, consider using the alias function to explicitly specify output column names."
            in caplog.text
        )

        assert mdt.output_column_names == [
            "test_long_feature_name1_long_feature_name2_long_feature_nam_0",
            "test_long_feature_name1_long_feature_name2_long_feature_nam_1",
            "test_long_feature_name1_long_feature_name2_long_feature_nam_2",
        ]

    def test_generate_output_col_name_invalid_odt(self, caplog):
        @udf(int)
        def really_long_function_name_that_exceed_63_characters_causing_invalid_name_for_on_demand_features(
            features,
        ):
            return features

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=really_long_function_name_that_exceed_63_characters_causing_invalid_name_for_on_demand_features,
            transformation_type=TransformationType.ON_DEMAND,
        )

        with caplog.at_level(logging.WARNING):
            odt._get_output_column_names()

        assert (
            "The default output feature names generated by the transformation function really_long_function_name_that_exceed_63_characters_causing_invalid_name_for_on_demand_features(features) exceed the maximum allowed length of 63 characters. Default names have been truncated to fit within the size limit. To avoid this, consider using the alias function to explicitly specify output column names."
            in caplog.text
        )

        assert odt.hopsworks_udf.output_column_names == [
            "really_long_function_name_that_exceed_63_characters_causing_inv"
        ]

    def test_generate_output_col_name_invalid_mdt(self, caplog):
        @udf(int)
        def really_long_function_name_that_exceed_63_characters_causing_invalid_name_for_on_demand_features(
            features,
        ):
            return features

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=really_long_function_name_that_exceed_63_characters_causing_invalid_name_for_on_demand_features,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with caplog.at_level(logging.WARNING):
            mdt._get_output_column_names()

        assert (
            "The default output feature names generated by the transformation function really_long_function_name_that_exceed_63_characters_causing_invalid_name_for_on_demand_features(features) exceed the maximum allowed length of 63 characters. Default names have been truncated to fit within the size limit. To avoid this, consider using the alias function to explicitly specify output column names."
            in caplog.text
        )

        # Asserting that the output column names generate are stripped to the required length
        assert mdt.hopsworks_udf.output_column_names == [
            "really_long_function_name_that_exceed_63_characters_causing_inv"
        ]

    @pytest.mark.skipif(
        Version("4.1.6") >= Version(version.__version__),
        reason="Requires Hopsworks 4.1.7 or higher to be working.",
    )
    def test_equality_mdt(self, mocker):
        mocked_connection = mocker.MagicMock()
        mocked_connection.backend_version = (
            version.__version__
        )  # Mocking backend version to be the same as the current version
        mocked_connection = mocker.patch(
            "hopsworks_common.client.get_connection", return_value=mocked_connection
        )

        @udf([int])
        def add_one(feature):
            return feature + 1

        mdt1 = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mdt2 = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        assert mdt1 == mdt2

    @pytest.mark.skipif(
        Version("4.1.6") >= Version(version.__version__),
        reason="Requires Hopsworks 4.1.7 or higher to be working.",
    )
    def test_equality_odt(self, mocker):
        mocked_connection = mocker.MagicMock()
        mocked_connection.backend_version = (
            version.__version__
        )  # Mocking backend version to be the same as the current version
        mocked_connection = mocker.patch(
            "hopsworks_common.client.get_connection", return_value=mocked_connection
        )

        @udf([int])
        def add_one(feature):
            return feature + 1

        odt1 = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        odt2 = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        assert odt1 == odt2

    @pytest.mark.skipif(
        Version("4.1.6") >= Version(version.__version__),
        reason="Requires Hopsworks 4.1.7 or higher to be working.",
    )
    def test_inequality(self, mocker):
        mocked_connection = mocker.MagicMock()
        mocked_connection.backend_version = (
            version.__version__
        )  # Mocking backend version to be the same as the current version
        mocked_connection = mocker.patch(
            "hopsworks_common.client.get_connection", return_value=mocked_connection
        )

        @udf([int])
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )

        assert mdt != odt
