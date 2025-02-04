#
#   Copyright 2024 Hopsworks AB
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

from datetime import date, datetime, time

import pandas as pd
import pytest
from hsfs.client.exceptions import FeatureStoreException
from hsfs.hopsworks_udf import (
    HopsworksUdf,
    TransformationFeature,
    UDFExecutionMode,
    udf,
)


class TestHopsworksUdf:
    def test_udf_creation_invalid_execution_mode(self):
        with pytest.raises(FeatureStoreException) as exception:

            @udf(return_type=int, mode="invalid")
            def test(feature):
                pass

        assert (
            str(exception.value)
            == "Ivalid execution mode `invalid` for UDF. Please use `default`, `python` or `pandas` instead."
        )

    def test_udf_creation_default_execution_mode(self):
        @udf(return_type=int)
        def test(feature):
            pass

        assert test.execution_mode == UDFExecutionMode.DEFAULT

        @udf(return_type=int, mode="default")
        def test1(feature):
            pass

        assert test1.execution_mode == UDFExecutionMode.DEFAULT

    def test_udf_creation_python_execution_mode(self):
        @udf(return_type=int, mode="python")
        def test(feature):
            pass

        assert test.execution_mode == UDFExecutionMode.PYTHON

    def test_udf_creation_pandas_execution_mode(self):
        @udf(return_type=int, mode="pandas")
        def test(feature):
            pass

        assert test.execution_mode == UDFExecutionMode.PANDAS

    def test_validate_and_convert_output_types_one_elements(self):
        assert HopsworksUdf._validate_and_convert_output_types([int]) == ["bigint"]

        assert HopsworksUdf._validate_and_convert_output_types([float]) == ["double"]

        assert HopsworksUdf._validate_and_convert_output_types([str]) == ["string"]

        assert HopsworksUdf._validate_and_convert_output_types([bool]) == ["boolean"]

        assert HopsworksUdf._validate_and_convert_output_types([datetime]) == [
            "timestamp"
        ]

        assert HopsworksUdf._validate_and_convert_output_types([time]) == ["timestamp"]

        assert HopsworksUdf._validate_and_convert_output_types([date]) == ["date"]

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

    def test_validate_and_convert_output_types_multiple_types(self):
        assert HopsworksUdf._validate_and_convert_output_types(
            [int, float, str, bool, datetime, date, time]
        ) == ["bigint", "double", "string", "boolean", "timestamp", "date", "timestamp"]

        assert HopsworksUdf._validate_and_convert_output_types(
            ["bigint", "double", "string", "boolean", "timestamp", "date"]
        ) == ["bigint", "double", "string", "boolean", "timestamp", "date"]

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

    def test_validate_and_convert_output_types_invalid_types(self):
        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([int, pd.DatetimeTZDtype])

        assert (
            str(exception.value)
            == f"Output type {pd.DatetimeTZDtype} is not supported. Please refer to the documentation to get more information on the supported types."
        )

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._validate_and_convert_output_types([int, "pd.DatetimeTZDtype"])

        assert (
            str(exception.value)
            == "Output type pd.DatetimeTZDtype is not supported. Please refer to the documentation to get more information on the supported types."
        )

    def test_get_module_imports(self):
        assert HopsworksUdf._get_module_imports(
            "python/tests/test_helpers/transformation_test_helper.py"
        ) == [
            "import pandas as pd",
            "from hsfs.transformation_statistics import TransformationStatistics",
        ]

    def test_extract_source_code(self):
        from .test_helpers.transformation_test_helper import test_function

        assert """import pandas as pd
from hsfs.transformation_statistics import TransformationStatistics
def test_function():
    return True""" == HopsworksUdf._extract_source_code(test_function).strip()

    def test_extract_function_arguments_no_arguments(self):
        from .test_helpers.transformation_test_helper import test_function

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._extract_function_arguments(test_function)

        assert (
            str(exception.value)
            == "No arguments present in the provided user defined function. Please provide at least one argument in the defined user defined function."
        )

    def test_extract_function_arguments_one_argument(self):
        from .test_helpers.transformation_test_helper import test_function_one_argument

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None)
        ]

    def test_extract_function_arguments_one_argument_with_statistics(self):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument_with_statistics,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument_with_statistics
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1")
        ]

    def test_extract_function_arguments_one_argument_with_typehint(self):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument_with_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument_with_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None)
        ]

    def test_extract_function_arguments_one_argument_with_statistics_and_typehints(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument_with_statistics_and_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_one_argument_with_statistics_and_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1")
        ]

    def test_extract_function_arguments_multiple_argument(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
        ]

    def test_extract_function_arguments_multiple_argument_with_statistics(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_statistics,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_statistics
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_multiple_argument_with_typehints(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
        ]

    def test_extract_function_arguments_multiple_argument_with_statistics_and_typehints(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_statistics_and_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_statistics_and_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name="arg2"),
        ]

    def test_extract_function_arguments_multiple_argument_with_mixed_statistics_and_typehints(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_mixed_statistics_and_typehints,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_with_mixed_statistics_and_typehints
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_multiple_argument_all_parameter_with_spaces(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_with_spaces,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_all_parameter_with_spaces
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name="arg2"),
        ]

    def test_extract_function_arguments_multiple_argument_all_parameter_multiline(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_all_parameter_multiline
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_multiple_argumen_all_parameter_multiline_with_comments(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline_with_comments,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_multiple_argument_all_parameter_multiline_with_comments
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name="arg1"),
            TransformationFeature(feature_name="arg2", statistic_argument_name=None),
            TransformationFeature(feature_name="arg3", statistic_argument_name="arg3"),
        ]

    def test_extract_function_arguments_context_variable(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_context_variables,
        )

        function_argument = HopsworksUdf._extract_function_arguments(
            test_function_context_variables
        )

        assert function_argument == [
            TransformationFeature(feature_name="arg1", statistic_argument_name=None),
        ]

    def test_extract_function_arguments_statistics_invalid(self):
        from .test_helpers.transformation_test_helper import (
            test_function_statistics_invalid,
        )

        with pytest.raises(FeatureStoreException) as exception:
            HopsworksUdf._extract_function_arguments(test_function_statistics_invalid)

        assert (
            str(exception.value)
            == "No argument corresponding to statistics parameter 'arg3' present in function definition."
        )

    def test_format_source_code_one_argument(self):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument,
        )

        function_source = HopsworksUdf._extract_source_code(test_function_one_argument)

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_one_argument(arg1):
\t    pass"""
        )

    def test_format_source_code_one_argument_with_statistics(self):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument_with_statistics,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_one_argument_with_statistics
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_one_argument_with_statistics(arg1):
\t    pass"""
        )

    def test_format_source_code_one_argument_with_typehints(self):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument_with_typehints,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_one_argument_with_typehints
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_one_argument_with_typehints(arg1):
\t    pass"""
        )

    def test_format_source_code_one_argument_with_statistics_and_typehints(self):
        from .test_helpers.transformation_test_helper import (
            test_function_one_argument_with_statistics_and_typehints,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_one_argument_with_statistics_and_typehints
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_one_argument_with_statistics_and_typehints(arg1):
\t    pass"""
        )

    def test_format_source_code_multiple_argument(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_multiple_argument_with_statistics(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_statistics,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_with_statistics
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_with_statistics(arg1, arg2, arg3):
\t    pass"""
        )

    def test_format_source_code_multiple_argument_with_typehints(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_typehints,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_with_typehints
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_with_typehints(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_multiple_argument_with_statistics_and_typehints(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_statistics_and_typehints,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_with_statistics_and_typehints
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_with_statistics_and_typehints(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_multiple_argument_with_mixed_statistics_and_typehints(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_with_mixed_statistics_and_typehints,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_with_mixed_statistics_and_typehints
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_with_mixed_statistics_and_typehints(arg1, arg2, arg3):
\t    pass"""
        )

    def test_format_source_code_multiple_argument_all_parameter_with_spaces(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_with_spaces,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_all_parameter_with_spaces
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_all_parameter_with_spaces(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_multiple_argument_all_parameter_multiline(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_all_parameter_multiline
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_all_parameter_multiline(arg1, arg2, arg3):
\t    pass"""
        )

    def test_format_source_code_multiline_with_comments(self):
        from .test_helpers.transformation_test_helper import (
            test_function_multiple_argument_all_parameter_multiline_with_comments,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_multiple_argument_all_parameter_multiline_with_comments
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_multiple_argument_all_parameter_multiline_with_comments(arg1, arg2, arg3):
\t    pass"""
        )

    def test_format_source_code_transformation_statistics_as_default_one_line(self):
        from .test_helpers.transformation_test_helper import (
            test_function_transformation_statistics_as_default_one_line,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_transformation_statistics_as_default_one_line
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_transformation_statistics_as_default_one_line(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_transformation_statistics_as_default_one_line_return_type(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_transformation_statistics_as_default_one_line_return_type,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_transformation_statistics_as_default_one_line_return_type
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_transformation_statistics_as_default_one_line_return_type(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_transformation_statistics_as_default_multiple_line(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_transformation_statistics_as_default_multiple_line,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_transformation_statistics_as_default_multiple_line
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_transformation_statistics_as_default_multiple_line(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_transformation_statistics_as_default_multiple_line_return_type_spaces(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_transformation_statistics_as_default_multiple_line_return_type_spaces,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_transformation_statistics_as_default_multiple_line_return_type_spaces
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_transformation_statistics_as_default_multiple_line_return_type_spaces(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_transformation_statistics_as_default_multiple_line_return_type_no_spaces(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_transformation_statistics_as_default_multiple_line_return_type_no_spaces,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_transformation_statistics_as_default_multiple_line_return_type_no_spaces
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_transformation_statistics_as_default_multiple_line_return_type_no_spaces(arg1, arg2):
\t    pass"""
        )

    def test_format_source_code_context_variable(
        self,
    ):
        from .test_helpers.transformation_test_helper import (
            test_function_context_variables,
        )

        function_source = HopsworksUdf._extract_source_code(
            test_function_context_variables
        )

        formated_source, module_imports = HopsworksUdf._format_source_code(
            function_source
        )

        assert (
            formated_source.strip()
            == """def test_function_context_variables(arg1):
\t    pass"""
        )

    def test_drop_features_one_element(self):
        @udf([int, float, int], drop="col1")
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        assert test_func.dropped_features == ["col1"]

    def test_drop_features_one_element_prefix(self):
        @udf([int, float, int], drop="col1")
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        assert test_func._dropped_features == ["col1"]
        assert test_func.dropped_features == ["prefix_col1"]

    def test_drop_features_multiple_element(self):
        @udf([int, float, int], drop=["col1", "col2"])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        assert test_func.dropped_features == ["col1", "col2"]

    def test_drop_features_multiple_element_prefix(self):
        @udf([int, float, int], drop=["col1", "col2"])
        def test_func(col1, col2, col3):
            return pd.DataFrame(
                {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
            )

        test_func._feature_name_prefix = "prefix_"

        assert test_func._dropped_features == ["col1", "col2"]
        assert test_func.dropped_features == ["prefix_col1", "prefix_col2"]

    def test_drop_features_invalid(self):
        with pytest.raises(FeatureStoreException) as exp:

            @udf([int, float, int], drop=["col1", "invalid_col"])
            def test_func(col1, col2, col3):
                return pd.DataFrame(
                    {"col1": [col1 + 1], "col2": [col2 + 1], "col3": [col3 + 1]}
                )

        assert (
            str(exp.value)
            == "Cannot drop features 'invalid_col' as they are not features given as arguments in the defined UDF."
        )

    def test_create_pandas_udf_return_schema_from_list_one_output_type(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        assert test_func._create_pandas_udf_return_schema_from_list() == "bigint"

    def test_create_pandas_udf_return_schema_from_list_one_argument_multiple_output_type(
        self,
    ):
        @udf([int, float, str, date, datetime, time, bool])
        def test_func(col1):
            return pd.DataFrame(
                {
                    "col1": [col1 + 1],
                    "col2": [col1 + 1],
                    "col3": [col1 + 1],
                    "col4": [col1 + 1],
                    "col5": [col1 + 1],
                    "col6": [True],
                }
            )

        test_func.output_column_names = [
            "test_func_col1_0",
            "test_func_col1_1",
            "test_func_col1_2",
            "test_func_col1_3",
            "test_func_col1_4",
            "test_func_col1_5",
            "test_func_col1_6",
        ]

        assert (
            test_func._create_pandas_udf_return_schema_from_list()
            == "`test_func_col1_0` bigint, `test_func_col1_1` double, `test_func_col1_2` string, `test_func_col1_3` date, `test_func_col1_4` timestamp, `test_func_col1_5` timestamp, `test_func_col1_6` boolean"
        )

    def test_pandas_udf_wrapper_single_output(self):
        test_dataframe = pd.DataFrame({"col1": [1, 2, 3, 4]})

        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func.output_column_names = ["test_func_col1_"]
        renaming_wrapper_function = test_func.pandas_udf_wrapper()

        result = renaming_wrapper_function(test_dataframe["col1"])

        assert result.name == "test_func_col1_"
        assert result.values.tolist() == [2, 3, 4, 5]

    def test_pandas_udf_wrapper_context_variables(self):
        test_dataframe = pd.DataFrame({"col1": [1, 2, 3, 4]})

        @udf(int)
        def test_func(col1, context):
            return col1 + context["test_value"]

        test_func.output_column_names = ["test_func_col1_"]
        test_func.transformation_context = {"test_value": 200}
        renaming_wrapper_function = test_func.pandas_udf_wrapper()

        result = renaming_wrapper_function(test_dataframe["col1"])

        assert result.name == "test_func_col1_"
        assert result.values.tolist() == [201, 202, 203, 204]

    def test_python_udf_wrapper_single_output(self):
        test_dataframe = pd.DataFrame({"col1": [1, 2, 3, 4]})

        @udf(int)
        def test_func(col1):
            return col1 + 1

        test_func.output_column_names = ["test_func_col1_"]
        wrapper_function = test_func.python_udf_wrapper(rename_outputs=False)

        result = test_dataframe.apply(
            lambda x: wrapper_function(x["col1"]), axis=1, result_type="expand"
        )

        assert result.values.tolist() == [2, 3, 4, 5]

    def test_python_udf_wrapper_context_variables(self):
        test_dataframe = pd.DataFrame({"col1": [1, 2, 3, 4]})

        @udf(int)
        def test_func(col1, context):
            return col1 + context["test_value"]

        test_func.transformation_context = {"test_value": 100}
        test_func.output_column_names = ["test_func_col1_"]
        wrapper_function = test_func.python_udf_wrapper(rename_outputs=False)

        result = test_dataframe.apply(
            lambda x: wrapper_function(x["col1"]), axis=1, result_type="expand"
        )

        assert result.values.tolist() == [101, 102, 103, 104]

    def test_pandas_udf_wrapper_multiple_output(self):
        @udf([int, float])
        def test_func(col1, col2):
            return pd.DataFrame({"out1": col1 + 1, "out2": col2 + 2})

        test_func.output_column_names = [
            "test_func_col1_col2_0",
            "test_func_col1_col2_1",
        ]
        renaming_wrapper_function = test_func.pandas_udf_wrapper()

        test_dataframe = pd.DataFrame(
            {"column1": [1, 2, 3, 4], "column2": [10, 20, 30, 40]}
        )

        result = renaming_wrapper_function(
            test_dataframe["column1"], test_dataframe["column2"]
        )

        assert all(result.columns == ["test_func_col1_col2_0", "test_func_col1_col2_1"])
        assert result.values.tolist() == [[2, 12], [3, 22], [4, 32], [5, 42]]

    def test_python_udf_wrapper_multiple_output(self):
        @udf([int, float], mode="python")
        def test_func(col1, col2):
            return col1 + 1, col2 + 2

        test_func.output_column_names = [
            "test_func_col1_col2_0",
            "test_func_col1_col2_1",
        ]
        wrapper_function = test_func.python_udf_wrapper(rename_outputs=False)

        test_dataframe = pd.DataFrame(
            {"column1": [1, 2, 3, 4], "column2": [10, 20, 30, 40]}
        )

        result = test_dataframe.apply(
            lambda x: wrapper_function(x["column1"], x["column2"]),
            axis=1,
            result_type="expand",
        )

        assert result.values.tolist() == [[2, 12], [3, 22], [4, 32], [5, 42]]

        renaming_wrapper_function = test_func.python_udf_wrapper(rename_outputs=True)

        result = test_dataframe.apply(
            lambda x: renaming_wrapper_function(x["column1"], x["column2"]),
            axis=1,
            result_type="expand",
        )

        assert all(result.columns == ["test_func_col1_col2_0", "test_func_col1_col2_1"])
        assert result.values.tolist() == [[2, 12], [3, 22], [4, 32], [5, 42]]

    def test_get_udf_spark_engine_default_mode_training(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        @udf(return_type=int)
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=False)

        assert pandas_udf_mocker.call_count == 1
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 1
        assert python_wrapper_mocker.call_count == 0

    def test_get_udf_spark_engine_default_mode_inference(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        @udf(return_type=int)
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=True)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 0
        assert python_wrapper_mocker.call_count == 1

    def test_get_udf_spark_engine_python_mode_training(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        @udf(return_type=int, mode="python")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=False)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 1
        assert pandas_wrapper_mocker.call_count == 0
        assert python_wrapper_mocker.call_count == 1

    def test_get_udf_spark_engine_python_mode_inference(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        @udf(return_type=int, mode="python")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=True)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 0
        assert python_wrapper_mocker.call_count == 1

    def test_get_udf_spark_engine_pandas_mode_training(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        @udf(return_type=int, mode="pandas")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=False)

        assert pandas_udf_mocker.call_count == 1
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 1
        assert python_wrapper_mocker.call_count == 0

    def test_get_udf_spark_engine_pandas_mode_inference(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="spark")

        @udf(return_type=int, mode="pandas")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=True)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 1
        assert python_wrapper_mocker.call_count == 0

    def test_get_udf_python_engine_default_mode_training(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(return_type=int)
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=False)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 1
        assert python_wrapper_mocker.call_count == 0

    def test_get_udf_python_engine_default_mode_inference(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(return_type=int)
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=True)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 0
        assert python_wrapper_mocker.call_count == 1

    def test_get_udf_python_engine_python_mode_training(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(return_type=int, mode="python")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=False)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 0
        assert python_wrapper_mocker.call_count == 1

    def test_get_udf_python_engine_python_mode_inference(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(return_type=int, mode="python")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=True)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 0
        assert python_wrapper_mocker.call_count == 1

    def test_get_udf_python_engine_pandas_mode_training(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(return_type=int, mode="pandas")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=False)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 1
        assert python_wrapper_mocker.call_count == 0

    def test_get_udf_python_engine_pandas_mode_inference(self, mocker):
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(return_type=int, mode="pandas")
        def test(feature):
            pass

        pandas_udf_mocker = mocker.patch("pyspark.sql.functions.pandas_udf")
        python_udf_mocker = mocker.patch("pyspark.sql.functions.udf")
        pandas_wrapper_mocker = mocker.patch.object(test, "pandas_udf_wrapper")
        python_wrapper_mocker = mocker.patch.object(test, "python_udf_wrapper")

        _ = test.get_udf(online=True)

        assert pandas_udf_mocker.call_count == 0
        assert python_udf_mocker.call_count == 0
        assert pandas_wrapper_mocker.call_count == 1
        assert python_wrapper_mocker.call_count == 0

    def test_HopsworkUDf_call_one_argument(self):
        @udf(int)
        def test_func(col1):
            return col1 + 1

        assert test_func.transformation_features == ["col1"]
        assert test_func.statistics_features == []

        assert test_func("new_feature").transformation_features == ["new_feature"]
        assert test_func("new_feature").statistics_features == []

        # Test with prefix
        test_func._feature_name_prefix = "prefix_"
        assert test_func.transformation_features == ["prefix_col1"]
        assert test_func.statistics_features == []

        assert test_func("new_feature").transformation_features == [
            "prefix_new_feature"
        ]
        assert test_func("new_feature").statistics_features == []

    def test_HopsworkUDf_call_one_argument_statistics(self):
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1")

        @udf(int)
        def test_func(col1, statistics=stats):
            return col1 + statistics.col1.mean

        assert test_func.transformation_features == ["col1"]
        assert test_func.statistics_features == ["col1"]
        assert test_func._statistics_argument_names == ["col1"]

        assert test_func("new_feature").transformation_features == ["new_feature"]
        assert test_func("new_feature").statistics_features == ["new_feature"]
        assert test_func("new_feature")._statistics_argument_names == ["col1"]

        # Test with prefix
        test_func._feature_name_prefix = "prefix_"
        assert test_func.transformation_features == ["prefix_col1"]
        assert test_func.statistics_features == ["col1"]
        assert test_func._statistics_argument_names == ["col1"]

        assert test_func("new_feature").transformation_features == [
            "prefix_new_feature"
        ]
        assert test_func("new_feature").statistics_features == ["new_feature"]
        assert test_func("new_feature")._statistics_argument_names == ["col1"]

    def test_HopsworkUDf_call_multiple_argument_statistics(self):
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1", "col3")

        @udf(int)
        def test_func(col1, col2, col3, statistics=stats):
            return col1 + statistics.col1.mean + statistics.col3.mean

        assert test_func.transformation_features == ["col1", "col2", "col3"]
        assert test_func.statistics_features == ["col1", "col3"]

        assert test_func("f1", "f2", "f3").transformation_features == ["f1", "f2", "f3"]
        assert test_func("f1", "f2", "f3").statistics_features == ["f1", "f3"]
        assert test_func("f1", "f2", "f3")._statistics_argument_names == [
            "col1",
            "col3",
        ]

    def test_validate_and_convert_drop_features(self):
        dropped_features = "feature1"
        transformation_feature = ["feature1", "feature2"]
        feature_name_prefix = None

        dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            dropped_features, transformation_feature, feature_name_prefix
        )

        assert dropped_features == ["feature1"]

    def test_validate_and_convert_drop_features_dropped_list(self):
        dropped_features = ["feature1", "feature2"]
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = None

        dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            dropped_features, transformation_feature, feature_name_prefix
        )

        assert dropped_features == ["feature1", "feature2"]

    def test_validate_and_convert_drop_features_dropped_invalid(self):
        dropped_features = "feature4"
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = None

        with pytest.raises(FeatureStoreException) as exp:
            HopsworksUdf._validate_and_convert_drop_features(
                dropped_features, transformation_feature, feature_name_prefix
            )

        assert (
            str(exp.value)
            == "Cannot drop features 'feature4' as they are not features given as arguments in the defined UDF."
        )

    def test_validate_and_convert_drop_features_dropped_invalid_list(self):
        dropped_features = ["feature4", "feature5"]
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = None

        with pytest.raises(FeatureStoreException) as exp:
            HopsworksUdf._validate_and_convert_drop_features(
                dropped_features, transformation_feature, feature_name_prefix
            )

        assert (
            str(exp.value)
            == "Cannot drop features 'feature4', 'feature5' as they are not features given as arguments in the defined UDF."
        )

    def test_validate_and_convert_drop_features_dropped_list_prefix(self):
        dropped_features = ["feature1", "feature2"]
        transformation_feature = ["test_feature1", "test_feature2", "test_feature3"]
        feature_name_prefix = "test_"

        dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            dropped_features, transformation_feature, feature_name_prefix
        )

        assert dropped_features == ["feature1", "feature2"]

    def test_validate_and_convert_drop_features_dropped_prefix_invalid(self):
        dropped_features = ["feature1", "feature2"]
        transformation_feature = ["feature1", "feature2", "feature3"]
        feature_name_prefix = "test_"

        with pytest.raises(FeatureStoreException) as exp:
            HopsworksUdf._validate_and_convert_drop_features(
                dropped_features, transformation_feature, feature_name_prefix
            )

        assert (
            str(exp.value)
            == "Cannot drop features 'test_feature1', 'test_feature2' as they are not features given as arguments in the defined UDF."
        )

    def test_alias_one_output(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        add_one = add_one.alias("feature_plus_one")

        assert add_one.output_column_names == ["feature_plus_one"]

    def test_alias_one_output_list(self):
        @udf(int)
        def add_one(feature):
            return feature + 1

        add_one = add_one.alias(["feature_plus_one"])

        assert add_one.output_column_names == ["feature_plus_one"]

    def test_alias_multiple_output(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        add_one = add_and_sub.alias("feature_plus_one", "feature_minus_one")

        assert add_one.output_column_names == ["feature_plus_one", "feature_minus_one"]

    def test_alias_multiple_output_list(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        add_one = add_and_sub.alias(["feature_plus_one", "feature_minus_one"])

        assert add_one.output_column_names == ["feature_plus_one", "feature_minus_one"]

    def test_alias_invalid_number_column_names(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        with pytest.raises(FeatureStoreException) as exp:
            add_and_sub.alias(["feature_plus_one", "feature_minus_one", "invalid_col"])

        assert (
            str(exp.value)
            == "The number of output feature names provided does not match the number of features returned by the transformation function 'add_and_sub(feature)'. Pease provide exactly 2 feature name(s) to match the output."
        )

    def test_alias_invalid_type(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        with pytest.raises(FeatureStoreException) as exp:
            add_and_sub.alias("feature_plus_one", {"name": "col1"})

        assert (
            str(exp.value)
            == "Invalid output feature names provided for the transformation function 'add_and_sub(feature)'. Please ensure all arguments are strings."
        )

    def test_alias_duplicates(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        with pytest.raises(FeatureStoreException) as exp:
            add_and_sub.alias("feature_plus_one", "feature_plus_one")

        assert (
            str(exp.value)
            == "Duplicate output feature names provided for the transformation function 'add_and_sub(feature)'. Please ensure all arguments names are unique."
        )

    def test_call_and_alias(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        add_one = add_and_sub("feature2").alias(
            ["feature_plus_one", "feature_minus_one"]
        )

        assert add_one.output_column_names == ["feature_plus_one", "feature_minus_one"]
        assert add_one.transformation_features == ["feature2"]

    def test_alias_invalid_length(self):
        @udf([int, int])
        def add_and_sub(feature):
            return feature + 1, feature - 1

        with pytest.raises(FeatureStoreException) as exp:
            add_and_sub.alias(["invalid" * 10, "feature_minus_one"])

        assert (
            str(exp.value)
            == "Invalid output feature names specified for the transformation function 'add_and_sub(feature)'. Please provide names shorter than 63 characters."
        )

    def test_invalid_transformation_context(self):
        @udf(int)
        def test_func(feature, context):
            return feature + context["test_value"]

        with pytest.raises(FeatureStoreException) as exp:
            test_func.transformation_context = "invalid_context"

        exp.match("Transformation context variable must be passed as dictionary.")

    def test_prepare_transformation_function_scope_no_kwargs_no_statistics_no_context(
        self,
    ):
        @udf(int)
        def test_func(feature):
            return feature + 1

        # Setting the output column names to test the scope as they would always be set before scope is prepared.
        test_func._output_column_names = ["test_func_feature_"]

        scope = test_func._prepare_transformation_function_scope()

        assert scope["_output_col_names"] == ["test_func_feature_"]
        assert "_output_col_names" in scope.keys()

    def test_prepare_transformation_function_scope_no_kwargs_no_statistics_context(
        self,
    ):
        @udf(int)
        def test_func(feature, context):
            return feature + 1

        # Setting the output column names to test the scope, transformation context as they would always be set before scope is prepared.
        test_func._output_column_names = ["test_func_feature_"]
        test_func.transformation_context = {"test_value": 100}

        scope = test_func._prepare_transformation_function_scope()

        print(scope)

        assert scope["_output_col_names"] == ["test_func_feature_"]
        assert scope["context"] == {"test_value": 100}
        assert all(
            value in scope.keys()
            for value in {
                "_output_col_names",
                "context",
            }
        )

    def test_prepare_transformation_function_scope_no_kwargs_statistics_context(self):
        @udf(int)
        def test_func(feature, context):
            return feature + 1

        # Setting the output column names to test the scope, transformation context and statistics as they would always be set before scope is prepared.
        test_func._output_column_names = ["test_func_feature_"]
        test_func.transformation_context = {"test_value": 100}
        test_func._statistics = 10

        scope = test_func._prepare_transformation_function_scope()

        assert scope["_output_col_names"] == ["test_func_feature_"]
        assert scope["context"] == {"test_value": 100}
        assert scope["statistics"] == 10
        assert all(
            value in scope.keys()
            for value in {"_output_col_names", "context", "statistics"}
        )

    def test_prepare_transformation_function_scope_kwargs_statistics_context(self):
        @udf(int)
        def test_func(feature, context):
            return feature + 1

        # Setting the output column names to test the scope, transformation context and statistics as they would always be set before scope is prepared.
        test_func._output_column_names = ["test_func_feature_"]
        test_func.transformation_context = {"test_value": 100}
        test_func._statistics = 10

        scope = test_func._prepare_transformation_function_scope(test="values")

        assert scope["_output_col_names"] == ["test_func_feature_"]
        assert scope["context"] == {"test_value": 100}
        assert scope["statistics"] == 10
        assert scope["test"] == "values"
        assert all(
            value in scope.keys()
            for value in {"_output_col_names", "context", "statistics", "test"}
        )
