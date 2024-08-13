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


import pandas as pd
import pytest
from hsfs.client.exceptions import FeatureStoreException
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationFunction, TransformationType


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

    def test_transformation_function_definition_with_hopworks_udf(self):
        @udf(int)
        def test2(col1):
            return col1 + 1

        tf = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test2,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        assert tf.hopsworks_udf == test2

    def test_generate_output_column_names_one_argument_one_output_type(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == ["test_func_col1_"]

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]

    def test_generate_output_column_names_one_argument_one_output_type_prefix(self):
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
        assert mdt.output_column_names == ["prefix_test_func_prefix_col1_"]

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]
        assert odt.output_column_names == ["prefix_test_func"]

    def test_generate_output_column_names_multiple_argument_one_output_type(self):
        @udf(int)
        def test_func(col1, col2, col3):
            return col1 + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        assert mdt._get_output_column_names() == ["test_func_col1_col2_col3_"]
        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]

    def test_generate_output_column_names_multiple_argument_one_output_type_prefix(
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
        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=test_func,
            transformation_type=TransformationType.ON_DEMAND,
        )
        assert odt._get_output_column_names() == ["test_func"]
        assert odt.output_column_names == ["prefix_test_func"]

    def test_generate_output_column_names_single_argument_multiple_output_type(self):
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

    def test_generate_output_column_names_single_argument_multiple_output_type_prefix(
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

    def test_generate_output_column_names_multiple_argument_multiple_output_type(self):
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

    def test_generate_output_column_names_multiple_argument_multiple_output_type_prefix(
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

    def test_validate_udf_type_on_demand_multiple_output(self):
        @udf([int, float])
        def test_func(col1, col2):
            return pd.DataFrame({"out1": col1 + 1, "out2": col2 + 2})

        with pytest.raises(FeatureStoreException) as exe:
            TransformationFunction(
                featurestore_id=10,
                hopsworks_udf=test_func,
                transformation_type=TransformationType.ON_DEMAND,
            )

        assert (
            str(exe.value)
            == "On-Demand Transformation functions can only return one column as output"
        )

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
