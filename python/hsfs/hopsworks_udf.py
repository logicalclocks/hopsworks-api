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

import ast
import copy
import inspect
import json
import re
import warnings
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import humps
from hopsworks_common import client
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.constants import FEATURES
from hsfs import engine, util
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.decorators import typechecked
from hsfs.transformation_statistics import TransformationStatistics
from packaging.version import Version


class UDFExecutionMode(Enum):
    """
    Class that store the possible execution types of UDF's.
    """

    DEFAULT = "default"
    PYTHON = "python"
    PANDAS = "pandas"

    def get_current_execution_mode(self, online):
        if self == UDFExecutionMode.DEFAULT and online:
            return UDFExecutionMode.PYTHON
        elif self == UDFExecutionMode.DEFAULT and not online:
            return UDFExecutionMode.PANDAS
        else:
            return self

    @staticmethod
    def from_string(execution_mode: str):
        try:
            return UDFExecutionMode[execution_mode.upper()]
        except KeyError as e:
            raise FeatureStoreException(
                f"Ivalid execution mode `{execution_mode}` for UDF. Please use `default`, `python` or `pandas` instead."
            ) from e


class UDFKeyWords(Enum):
    """
    Class that stores the keywords used as arguments in a UDFs.
    """

    STATISTICS = "statistics"
    CONTEXT = "context"


def udf(
    return_type: Union[List[type], type],
    drop: Optional[Union[str, List[str]]] = None,
    mode: Literal["default", "python", "pandas"] = "default",
) -> "HopsworksUdf":
    """
    Create an User Defined Function that can be and used within the Hopsworks Feature Store to create transformation functions.

    Hopsworks UDF's are user defined functions that executes as 'pandas_udf' when executing
    in spark engine and as pandas functions in the python engine. The pandas udf/pandas functions
    gets as inputs pandas Series's and can provide as output a pandas Series or a pandas DataFrame.
    A Hopsworks udf is defined using the `hopsworks_udf` decorator. The outputs of the defined UDF
    must be mentioned in the decorator as a list of python types.


    !!! example
        ```python
        from hopsworks import udf

        @udf(float)
        def add_one(data1):
            return data1 + 1
        ```

    # Arguments
        return_type: `Union[List[type], type]`. The output types of the defined UDF
        drop: `Optional[Union[str, List[str]]]`. The features to be dropped after application of transformation functions. Default's to None.
        mode: `Literal["default", "python", "pandas"]`. The exection mode of the UDF. Default's to 'default'

    # Returns
        `HopsworksUdf`: The metadata object for hopsworks UDF's.

    # Raises
        `hopsworks.client.exceptions.FeatureStoreException` : If unable to create UDF.
    """

    def wrapper(func: Callable) -> HopsworksUdf:
        udf = HopsworksUdf(
            func=func,
            return_types=return_type,
            dropped_argument_names=drop,
            execution_mode=UDFExecutionMode.from_string(mode),
        )
        return udf

    return wrapper


@dataclass
class TransformationFeature:
    """
    Mapping of feature names to their corresponding statistics argument names in the code.

    The statistic_argument_name for a feature name would be None if the feature does not need statistics.

    # Arguments
        feature_name : `str`. Name of the feature.
        statistic_argument_name : `str`. Name of the statistics argument in the code for the feature specified in the feature name.
    """

    feature_name: str
    statistic_argument_name: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "feature_name": self.feature_name,
            "statistic_argument_name": self.statistic_argument_name,
        }


@typechecked
class HopsworksUdf:
    """
    Meta data for user defined functions.

    Stores meta data required to execute the user defined function in both spark and python engine.
    The class generates uses the metadata to dynamically generate user defined functions based on the
    engine it is executed in.

    # Arguments
        func : `Union[Callable, str]`. The transformation function object or the source code of the transformation function.
        return_types : `Union[List[type], type, List[str], str]`. A python type or a list of python types that denotes the data types of the columns output from the transformation functions.
        name : `Optional[str]`. Name of the transformation function.
        transformation_features : `Optional[List[TransformationFeature]]`. A list of objects of `TransformationFeature` that maps the feature used for transformation to their corresponding statistics argument names if any
        transformation_function_argument_names : `Optional[List[str]]`. The argument names of the transformation function.
        dropped_argument_names : `Optional[List[str]]`. The arguments to be dropped from the finial DataFrame after the transformation functions are applied.
        dropped_feature_names : `Optional[List[str]]`. The feature name corresponding to the arguments names that are dropped
        feature_name_prefix: `Optional[str]`. Prefixes if any used in the feature view.
        output_column_names: `Optional[List[str]]`. The names of the output columns returned from the transformation function.
        generate_output_col_names: `bool`. Generate default output column names for the transformation function. Default's to True.
    """

    # Mapping for converting python types to spark types - required for creating pandas UDF's.
    PYTHON_SPARK_TYPE_MAPPING = {
        str: "string",
        int: "bigint",
        float: "double",
        bool: "boolean",
        datetime: "timestamp",
        time: "timestamp",
        date: "date",
    }

    def __init__(
        self,
        func: Union[Callable, str],
        return_types: Union[List[type], type, List[str], str],
        execution_mode: UDFExecutionMode,
        name: Optional[str] = None,
        transformation_features: Optional[List[TransformationFeature]] = None,
        transformation_function_argument_names: Optional[List[str]] = None,
        dropped_argument_names: Optional[List[str]] = None,
        dropped_feature_names: Optional[List[str]] = None,
        feature_name_prefix: Optional[str] = None,
        output_column_names: Optional[str] = None,
        generate_output_col_names: bool = True,
    ):
        self._return_types: List[str] = HopsworksUdf._validate_and_convert_output_types(
            return_types
        )

        self._execution_mode: UDFExecutionMode = execution_mode

        self._feature_name_prefix: Optional[str] = (
            feature_name_prefix  # Prefix to be added to feature names
        )

        self._function_name: str = func.__name__ if name is None else name

        self._function_source: str = (
            HopsworksUdf._extract_source_code(func)
            if isinstance(func, Callable)
            else func
        )

        # The parameter `output_column_names` is initialized lazily.
        # It is only initialized if the output column names are retrieved from the backend or explicitly specified using the `alias` function or is initialized with default column names if the UDF is accessed from a transformation function.
        # Output column names are only stored in the backend when a model dependent or on demand transformation function is created using the defined UDF.
        self._output_column_names: List[str] = []

        if not transformation_features:
            # New transformation function being declared so extract source code from function
            self._transformation_features: List[TransformationFeature] = (
                HopsworksUdf._extract_function_arguments(func)
                if not transformation_features
                else transformation_features
            )

            self._transformation_function_argument_names = [
                feature.feature_name for feature in self._transformation_features
            ]

            self._dropped_argument_names: List[str] = (
                HopsworksUdf._validate_and_convert_drop_features(
                    dropped_argument_names,
                    self.transformation_features,
                    feature_name_prefix,
                )
            )
            self._dropped_features = self._dropped_argument_names

        else:
            self._transformation_features = transformation_features
            self._transformation_function_argument_names = (
                transformation_function_argument_names
            )
            self._dropped_argument_names = dropped_argument_names
            self._dropped_features = (
                dropped_feature_names
                if dropped_feature_names
                else dropped_argument_names
            )
            self._output_column_names = (
                output_column_names if output_column_names else []
            )

        self._formatted_function_source, self._module_imports = (
            HopsworksUdf._format_source_code(self._function_source)
        )

        self._statistics: Optional[TransformationStatistics] = None

        self._transformation_context: Dict[str, Any] = {}

        # Denote if the output feature names have to be generated.
        # Set to `False` if the output column names are saved in the backend and the udf is constructed from it using `from_response_json` function or if user has specified the output feature names using the `alias`` function.
        self._generate_output_col_name: bool = generate_output_col_names

    @staticmethod
    def _validate_and_convert_drop_features(
        dropped_features: Union[str, List[str]],
        transformation_feature: List[str],
        feature_name_prefix: str,
    ) -> List[str]:
        """
        Function that converts dropped features to a list and validates if the dropped feature is present in the transformation function
        # Arguments
            dropped_features: `Union[str, List[str]]`. Features of be dropped.
            transformation_feature: `List[str]`. Features to be transformed in the UDF
        # Returns
            `List[str]`: A list of features to be dropped.
        """
        if not dropped_features:
            return None

        dropped_features = (
            [dropped_features]
            if not isinstance(dropped_features, list)
            else dropped_features
        )

        feature_name_prefix = feature_name_prefix if feature_name_prefix else ""

        missing_drop_features = []
        for dropped_feature in dropped_features:
            dropped_feature = feature_name_prefix + dropped_feature
            if dropped_feature not in transformation_feature:
                missing_drop_features.append(dropped_feature)

        if missing_drop_features:
            missing_drop_features = "', '".join(missing_drop_features)
            raise FeatureStoreException(
                f"Cannot drop features '{missing_drop_features}' as they are not features given as arguments in the defined UDF."
            )

        return dropped_features

    @staticmethod
    def _validate_and_convert_output_types(
        output_types: Union[List[type], List[str]],
    ) -> List[str]:
        """
        Function that takes in a type or list of types validates if it is supported and return a list of strings

        # Arguments
            output_types: `list`. List of python types.

        # Raises
            `hopsworks.client.exceptions.FeatureStoreException` : If any of the output type is invalid
        """
        convert_output_types = []
        output_types = (
            output_types if isinstance(output_types, List) else [output_types]
        )
        for output_type in output_types:
            if (
                output_type not in HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING.keys()
                and output_type not in HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING.values()
            ):
                raise FeatureStoreException(
                    f"Output type {output_type} is not supported. Please refer to the documentation to get more information on the supported types."
                )
            convert_output_types.append(
                output_type
                if isinstance(output_type, str)
                else HopsworksUdf.PYTHON_SPARK_TYPE_MAPPING[output_type]
            )
        return convert_output_types

    @staticmethod
    def _get_module_imports(path: str) -> List[str]:
        """Function that extracts the imports used in the python file specified in the path.

        # Arguments
            path: `str`. Path to python file from which imports are to be extracted.

        # Returns
            `List[str]`: A list of string that contains the import statement using in the file.
        """
        imports = []
        with open(path) as fh:
            root = ast.parse(fh.read(), path)
        for node in ast.iter_child_nodes(root):
            if isinstance(node, ast.Import):
                imported_module = False
            elif isinstance(node, ast.ImportFrom):
                imported_module = node.module
            else:
                continue
            for n in node.names:
                if imported_module:
                    import_line = "from " + imported_module + " import " + n.name
                elif n.asname:
                    import_line = "import " + n.name + " as " + n.asname
                else:
                    import_line = "import " + n.name
                imports.append(import_line)
        return imports

    @staticmethod
    def _extract_source_code(udf_function: Callable) -> str:
        """
        Function to extract the source code of the function along with the imports used in the file.

        The module imports cannot be extracted if the function is defined in a jupyter notebook.

        # Arguments
            udf_function: `Callable`. Function for which the source code must be extracted.
        # Returns
            `str`: a string that contains the source code of function along with the extracted module imports.
        """
        try:
            module_imports = HopsworksUdf._get_module_imports(
                inspect.getfile(udf_function)
            )
        except FileNotFoundError:
            module_imports = [""]
            warnings.warn(
                "Cannot extract imported dependencies for the UDF from the module in which it is defined. Please make sure to import all dependencies for the UDF inside the function.",
                stacklevel=2,
            )

        function_code = inspect.getsource(udf_function)
        source_code = "\n".join(module_imports) + "\n" + function_code

        return source_code

    @staticmethod
    def _parse_function_signature(source_code: str) -> Tuple[List[str], str, int, int]:
        """
        Function to parse the source code to extract the argument along with the start and end line of the function signature

        # Arguments
            source_code: `str`. Source code of a function.
        # Returns
            `List[str]`: List of function arguments
            `str`: function signature
            `int`: starting line number of function signature
            `int`: ending line number of function signature

        """
        source_code = source_code.split("\n")

        signature_start_line = None
        signature_end_line = None
        # Find the line where the function signature is defined
        for i, line in enumerate(source_code):
            if line.strip().startswith("def "):
                signature_start_line = i
            if signature_start_line is not None and re.search(
                r"\)\s*(->.*)?\s*:$", line.split("#")[0].strip()
            ):
                signature_end_line = i
                break

        # Parse the function signature to remove the specified argument
        signature = "".join(
            [
                code.split("#")[0]
                for code in source_code[signature_start_line : signature_end_line + 1]
            ]
        )
        arg_list = signature.split("(")[1].split(")")[0].split(",")
        arg_list = [
            arg.split(":")[0].split("=")[0].strip()
            for arg in arg_list
            if not arg.strip() == ""
        ]

        # Extracting keywords like `context`and `statistics` from the function signature.
        keyword_to_remove = [keyword.value for keyword in UDFKeyWords]
        arg_list = [arg for arg in arg_list if arg not in keyword_to_remove]

        return arg_list, signature, signature_start_line, signature_end_line

    @staticmethod
    def _extract_function_arguments(function: Callable) -> List[TransformationFeature]:
        """
        Function to extract the argument names from a provided function source code.

        # Arguments
            source_code: `Callable`. The function for which the value are to be extracted.
        # Returns
            `List[TransformationFeature]`: List of TransformationFeature that provide a mapping from feature names to corresponding statistics parameters if any is present.
        """
        arg_list = []
        statistics = None
        signature = inspect.signature(function).parameters
        if not signature:
            raise FeatureStoreException(
                "No arguments present in the provided user defined function. Please provide at least one argument in the defined user defined function."
            )

        # Extracting keywords like `context`and `statistics` from the function signature.
        for arg in inspect.signature(function).parameters.values():
            # Fetching default value for statistics parameter to find argument names that require statistics.
            if arg.name == UDFKeyWords.STATISTICS.value:
                statistics = arg.default
            elif arg.name != UDFKeyWords.CONTEXT.value:
                arg_list.append(arg.name)

        if statistics:
            missing_statistic_features = [
                statistic_feature
                for statistic_feature in statistics._features
                if statistic_feature not in arg_list
            ]
            if missing_statistic_features:
                missing_statistic_features = "', '".join(missing_statistic_features)
                raise FeatureStoreException(
                    f"No argument corresponding to statistics parameter '{missing_statistic_features}' present in function definition."
                )
            return [
                TransformationFeature(arg, arg if arg in statistics._features else None)
                for arg in arg_list
            ]
        else:
            return [TransformationFeature(arg, None) for arg in arg_list]

    @staticmethod
    def _format_source_code(source_code: str) -> Tuple[str, str]:
        """
        Function that parses the existing source code to remove statistics parameter and remove all decorators and type hints from the function source code.

        # Arguments
            source_code: `str`. Source code of a function.
        # Returns
            `Tuple[str, str]`: Tuple that contains Source code that does not contain any decorators, type hints or statistics parameters and the module imports
        """

        arg_list, signature, _, signature_end_line = (
            HopsworksUdf._parse_function_signature(source_code)
        )
        module_imports = source_code.split("@")[0]

        # Reconstruct the function signature
        new_signature = (
            signature.split("(")[0].strip() + "(" + ", ".join(arg_list) + "):"
        )
        source_code = source_code.split("\n")
        # Reconstruct the modified function as a string
        modified_source = (
            new_signature + "\n\t" + "\n\t".join(source_code[signature_end_line + 1 :])
        )

        return modified_source, module_imports

    def _create_pandas_udf_return_schema_from_list(self) -> str:
        """
        Function that creates the return schema required for executing the defined UDF's as pandas UDF's in Spark.

        # Returns
            `str`: DDL-formatted type string that denotes the return types of the user defined function.
        """
        if len(self.return_types) > 1:
            return ", ".join(
                [
                    f"`{self.output_column_names[i]}` {self.return_types[i]}"
                    for i in range(len(self.return_types))
                ]
            )
        else:
            return self.return_types[0]

    def _prepare_transformation_function_scope(self, **kwargs) -> Dict[str, Any]:
        """
        Function that prepares the scope for the transformation function to be executed. This scope would include any variable that are required to be injected into the transformation function.

        By default the output column names, transformation statistics and transformation context are injected into the scope if they are required by the transformation function. Any additional arguments can be passed as kwargs.
        """
        # Shallow copy of scope performed because updating statistics argument of scope must not affect other instances.
        scope = __import__("__main__").__dict__.copy()

        # Adding variables required to be injected into the scope.
        vaariable_to_inject = {
            UDFKeyWords.STATISTICS.value: self.transformation_statistics,
            UDFKeyWords.CONTEXT.value: self.transformation_context,
            "_output_col_names": self.output_column_names,
        }
        vaariable_to_inject.update(**kwargs)

        # Injecting variables that have a value into scope.
        scope.update({k: v for k, v in vaariable_to_inject.items() if v is not None})

        return scope

    def python_udf_wrapper(self, rename_outputs) -> Callable:
        """
        Function that creates a dynamic wrapper function for the defined udf. The wrapper function would be used to specify column names, in spark engines and to localize timezones.

        The renames is done so that the column names match the schema expected by spark when multiple columns are returned in a spark udf.
        The wrapper function would be available in the main scope of the program.

        # Returns
            `Callable`: A wrapper function that renames outputs of the User defined function into specified output column names.
        """
        # Check if any output is of date time type.
        date_time_output_index = [
            ind for ind, ele in enumerate(self.return_types) if ele == "timestamp"
        ]

        # Function that converts the timestamp to localized timezone
        convert_timstamp_function = (
            "def convert_timezone(date_time_obj : datetime):\n"
            + "   from datetime import datetime, timezone\n"
            + "   import tzlocal\n"
            + "   current_timezone = tzlocal.get_localzone()\n"
            + "   if date_time_obj and isinstance(date_time_obj, datetime):\n"
            + "      if date_time_obj.tzinfo is None:\n"
            + "      # if timestamp is timezone unaware, make sure it's localized to the system's timezone.\n"
            + "      # otherwise, spark will implicitly convert it to the system's timezone.\n"
            + "         return date_time_obj.replace(tzinfo=current_timezone)\n"
            + "      else:\n"
            + "         return date_time_obj.astimezone(timezone.utc).replace(tzinfo=current_timezone)\n"
            + "   else:\n"
            + "      return None\n"
        )

        # Start wrapper function generation
        code = (
            self._module_imports
            + "\n"
            + (convert_timstamp_function + "\n" if date_time_output_index else "\n")
            + "def wrapper(*args):\n"
            + f"   {self._formatted_function_source}\n"
            + f"   transformed_features = {self.function_name}(*args)\n"
        )
        if len(self.return_types) > 1:
            # If date time columns are there convert make sure that they are localized.
            if date_time_output_index:
                code += (
                    "   transformed_features = list(transformed_features)\n"
                    "   for index in _date_time_output_index:\n"
                    + "      transformed_features[index] = convert_timezone(transformed_features[index])\n"
                )
            if rename_outputs:
                # Use a dictionary to rename output to correct column names. This must be for the udf's to be executable in spark.
                code += "   return dict(zip(_output_col_names, transformed_features))"
            else:
                code += "   return transformed_features"
        else:
            if date_time_output_index:
                code += (
                    "   transformed_features = convert_timezone(transformed_features)\n"
                )
            code += "   return transformed_features"

        # Inject required parameter to scope
        scope = self._prepare_transformation_function_scope(
            _date_time_output_index=date_time_output_index
        )

        # executing code
        exec(code, scope)

        # returning executed function object
        return eval("wrapper", scope)

    def pandas_udf_wrapper(self) -> Callable:
        """
        Function that creates a dynamic wrapper function for the defined udf that renames the columns output by the UDF into specified column names.

        The renames is done so that the column names match the schema expected by spark when multiple columns are returned in a pandas udf.
        The wrapper function would be available in the main scope of the program.

        # Returns
            `Callable`: A wrapper function that renames outputs of the User defined function into specified output column names.
        """

        date_time_output_columns = [
            self.output_column_names[ind]
            for ind, ele in enumerate(self.return_types)
            if ele == "timestamp"
        ]

        # Function to make transformation function time safe. Defined as a string because it has to be dynamically injected into scope to be executed by spark
        convert_timstamp_function = """def convert_timezone(date_time_col : pd.Series):
        import tzlocal
        current_timezone = tzlocal.get_localzone()
        if date_time_col.dt.tz is None:
            # if timestamp is timezone unaware, make sure it's localized to the system's timezone.
            # otherwise, spark will implicitly convert it to the system's timezone.
            return date_time_col.dt.tz_localize(str(current_timezone))
        else:
            # convert to utc, then localize to system's timezone
            return date_time_col.dt.tz_convert('UTC').dt.tz_localize(None).dt.tz_localize(str(current_timezone))"""

        # Defining wrapper function that renames the column names to specific names
        if len(self.return_types) > 1:
            code = (
                self._module_imports
                + "\n"
                + f"""import pandas as pd
{convert_timstamp_function}
def renaming_wrapper(*args):
    {self._formatted_function_source}
    df = {self.function_name}(*args)
    if isinstance(df, tuple):
        df = pd.concat(df, axis=1)
    df.columns = _output_col_names
    for col in _date_time_output_columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = convert_timezone(df[col])
    return df"""
            )
        else:
            code = (
                self._module_imports
                + "\n"
                + f"""import pandas as pd
{convert_timstamp_function}
def renaming_wrapper(*args):
    {self._formatted_function_source}
    df = {self.function_name}(*args)
    df = df.rename(_output_col_names[0])
    if _date_time_output_columns:
        # Set correct type is column is not of datetime type
        if pd.api.types.is_datetime64_any_dtype(df):
            df = convert_timezone(df)
    return df"""
            )

        # Inject required parameter to scope
        scope = self._prepare_transformation_function_scope(
            _date_time_output_columns=date_time_output_columns
        )

        # executing code
        exec(code, scope)

        # returning executed function object
        return eval("renaming_wrapper", scope)

    def __call__(self, *features: List[str]) -> "HopsworksUdf":
        """
        Set features to be passed as arguments to the user defined functions

        # Arguments
            features: Name of features to be passed to the User Defined function
        # Returns
            `HopsworksUdf`: Meta data class for the user defined function.
        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: If the provided number of features do not match the number of arguments in the defined UDF or if the provided feature names are not strings.
        """

        if len(features) != len(self.transformation_features):
            raise FeatureStoreException(
                "Number of features provided does not match the number of features provided in the UDF definition"
            )

        for arg in features:
            if not isinstance(arg, str):
                raise FeatureStoreException(
                    f'Feature names provided must be string "{arg}" is not string'
                )
        transformation_feature_name = self.transformation_features
        if self.dropped_features:
            index_dropped_features = [
                transformation_feature_name.index(dropped_feature)
                for dropped_feature in self.dropped_features
            ]
            updated_dropped_features = [
                features[index] for index in index_dropped_features
            ]
        else:
            updated_dropped_features = None

        # Create a copy of the UDF to associate it with new feature names.
        udf = copy.deepcopy(self)

        udf._transformation_features = [
            TransformationFeature(
                new_feature_name, transformation_feature.statistic_argument_name
            )
            for transformation_feature, new_feature_name in zip(
                self._transformation_features, features
            )
        ]
        udf.dropped_features = updated_dropped_features
        return udf

    def alias(self, *args: str):
        """
        Set the names of the transformed features output by the UDF.
        """
        if len(args) == 1 and isinstance(args[0], list):
            # If a single list is passed, use it directly
            output_col_names = args[0]
        else:
            # Otherwise, use the individual arguments as a list
            output_col_names = list(args)
        if any(
            not isinstance(output_col_name, str) for output_col_name in output_col_names
        ):
            raise FeatureStoreException(
                f"Invalid output feature names provided for the transformation function '{repr(self)}'. Please ensure all arguments are strings."
            )

        self._generate_output_col_name = False
        self.output_column_names = output_col_names

        return self

    def _validate_output_col_name(self, output_col_names):
        if any(
            len(output_col_name) > FEATURES.MAX_LENGTH_NAME
            for output_col_name in output_col_names
        ):
            raise FeatureStoreException(
                f"Invalid output feature names specified for the transformation function '{repr(self)}'. Please provide names shorter than {FEATURES.MAX_LENGTH_NAME} characters."
            )

        if len(output_col_names) != len(set(output_col_names)):
            raise FeatureStoreException(
                f"Duplicate output feature names provided for the transformation function '{repr(self)}'. Please ensure all arguments names are unique."
            )

        if output_col_names and len(output_col_names) != len(self.return_types):
            raise FeatureStoreException(
                f"The number of output feature names provided does not match the number of features returned by the transformation function '{repr(self)}'. Pease provide exactly {len(self.return_types)} feature name(s) to match the output."
            )

    def _validate_transformation_context(self, transformation_context: Dict[str, Any]):
        """
        Function that checks if the context variables provided to the transformation function is valid.

        it checks if context variables are defined as a dictionary.
        """
        if not isinstance(transformation_context, dict):
            raise FeatureStoreException(
                "Transformation context variable must be passed as dictionary."
            )

        return transformation_context

    def update_return_type_one_hot(self):
        self._return_types = [
            self._return_types[0]
            for _ in range(len(self.transformation_statistics.feature.unique_values))
        ]

    def get_udf(self, online: bool = False) -> Callable:
        """
        Function that checks the current engine type, execution type and returns the appropriate UDF.

        If the execution mode is : "default":
            - In the `spark` engine : During inference a spark udf is returned otherwise a spark pandas_udf is returned.
            - In the `python` engine : During inference a python udf is returned otherwise a pandas udf is returned.
        If the execution mode is : "pandas":
            - In the `spark` engine : Always returns a spark pandas udf.
            - In the `python` engine : Always returns a pandas udf.
        If the execution mode is : "python":
            - In the `spark` engine : Always returns a spark udf.
            - In the `python` engine : Always returns a python udf.

        # Arguments
            inference: `bool`. Specify if udf required for online inference.

        # Returns
            `Callable`: Pandas UDF in the spark engine otherwise returns a python function for the UDF.
        """

        if (
            self.execution_mode.get_current_execution_mode(online)
            == UDFExecutionMode.PANDAS
        ):
            if engine.get_type() in ["python", "training"] or online:
                return self.pandas_udf_wrapper()
            else:
                from pyspark.sql.functions import pandas_udf

                return pandas_udf(
                    f=self.pandas_udf_wrapper(),
                    returnType=self._create_pandas_udf_return_schema_from_list(),
                )
        elif (
            self.execution_mode.get_current_execution_mode(online)
            == UDFExecutionMode.PYTHON
        ):
            if engine.get_type() in ["python", "training"] or online:
                # Renaming into correct column names done within Python engine since a wrapper does not work for polars dataFrames.
                return self.python_udf_wrapper(rename_outputs=False)
            else:
                from pyspark.sql.functions import udf as pyspark_udf

                return pyspark_udf(
                    f=self.python_udf_wrapper(rename_outputs=True),
                    returnType=self._create_pandas_udf_return_schema_from_list(),
                )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert class into a dictionary.

        # Returns
            `Dict`: Dictionary that contains all data required to json serialize the object.
        """
        backend_version = client.get_connection().backend_version

        return {
            "sourceCode": self._function_source,
            "outputTypes": self.return_types,
            "transformationFeatures": self.transformation_features,
            "transformationFunctionArgumentNames": self._transformation_function_argument_names,
            "droppedArgumentNames": self._dropped_argument_names,
            "statisticsArgumentNames": self._statistics_argument_names
            if self.statistics_required
            else None,
            "name": self.function_name,
            "featureNamePrefix": self._feature_name_prefix,
            "executionMode": self.execution_mode.value.upper(),
            **(
                {"outputColumnNames": self.output_column_names}
                if Version(backend_version) >= Version("4.1.6")
                else {}
            ),  # This check is added for backward compatibility with older versions of Hopsworks. The "outputColumnNames" field was added in Hopsworks 4.1.6 and versions below do not support unknown fields in the backend.
        }

    def json(self) -> str:
        """
        Convert class into its json serialized form.

        # Returns
            `str`: Json serialized object.
        """
        return json.dumps(self, cls=util.Encoder)

    @classmethod
    def from_response_json(
        cls: "HopsworksUdf", json_dict: Dict[str, Any]
    ) -> "HopsworksUdf":
        """
        Function that constructs the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `HopsworksUdf`: Json deserialized class object.
        """

        json_decamelized = humps.decamelize(json_dict)
        function_source_code = json_decamelized["source_code"]
        function_name = json_decamelized["name"]
        feature_name_prefix = json_decamelized.get("feature_name_prefix", None)
        output_types = [
            output_type.strip() for output_type in json_decamelized["output_types"]
        ]
        transformation_features = [
            feature.strip() for feature in json_decamelized["transformation_features"]
        ]
        dropped_argument_names = (
            [
                dropped_feature.strip()
                for dropped_feature in json_decamelized["dropped_argument_names"]
            ]
            if json_decamelized.get("dropped_argument_names", None)
            else None
        )
        transformation_function_argument_names = (
            [
                arg_name.strip()
                for arg_name in json_decamelized[
                    "transformation_function_argument_names"
                ]
            ]
            if json_decamelized.get("transformation_function_argument_names", None)
            else None
        )
        statistics_features = (
            [
                feature.strip()
                for feature in json_decamelized["statistics_argument_names"]
            ]
            if json_decamelized.get("statistics_argument_names", None)
            else None
        )

        output_column_names = (
            [feature.strip() for feature in json_decamelized["output_column_names"]]
            if json_decamelized.get("output_column_names", None)
            else None
        )

        # Reconstructing statistics arguments.
        arg_list, _, _, _ = HopsworksUdf._parse_function_signature(function_source_code)

        transformation_features = (
            arg_list if not transformation_features else transformation_features
        )

        dropped_feature_names = (
            [
                transformation_features[arg_list.index(dropped_argument_name)]
                for dropped_argument_name in dropped_argument_names
            ]
            if dropped_argument_names
            else None
        )

        if statistics_features:
            transformation_features = [
                TransformationFeature(
                    transformation_features[arg_index],
                    arg_list[arg_index]
                    if arg_list[arg_index] in statistics_features
                    else None,
                )
                for arg_index in range(len(arg_list))
            ]
        else:
            transformation_features = [
                TransformationFeature(transformation_features[arg_index], None)
                for arg_index in range(len(arg_list))
            ]

        hopsworks_udf: HopsworksUdf = cls(
            func=function_source_code,
            return_types=output_types,
            name=function_name,
            transformation_features=transformation_features,
            dropped_argument_names=dropped_argument_names,
            dropped_feature_names=dropped_feature_names,
            feature_name_prefix=feature_name_prefix,
            transformation_function_argument_names=transformation_function_argument_names,
            execution_mode=UDFExecutionMode.from_string(
                json_decamelized["execution_mode"]
            ),
            output_column_names=output_column_names,
            generate_output_col_names=not output_column_names,  # Do not generate output column names if they are retrieved from the back
        )

        # Set transformation features if already set.
        return hopsworks_udf

    @property
    def return_types(self) -> List[str]:
        """Get the output types of the UDF"""
        # Update the number of outputs for one hot encoder to match the number of unique values for the feature
        if self.function_name == "one_hot_encoder" and self.transformation_statistics:
            self.update_return_type_one_hot()
        return self._return_types

    @property
    def function_name(self) -> str:
        """Get the function name of the UDF"""
        return self._function_name

    @property
    def statistics_required(self) -> bool:
        """Get if statistics for any feature is required by the UDF"""
        return bool(self.statistics_features)

    @property
    def transformation_statistics(
        self,
    ) -> Optional[TransformationStatistics]:
        """Feature statistics required for the defined UDF"""
        return self._statistics

    @property
    def output_column_names(self) -> List[str]:
        """Output columns names of the transformation function"""
        if self._feature_name_prefix:
            return [
                self._feature_name_prefix + output_col_name
                for output_col_name in self._output_column_names
            ]
        else:
            return self._output_column_names

    @property
    def transformation_features(self) -> List[str]:
        """
        List of feature names to be used in the User Defined Function.
        """
        if self._feature_name_prefix:
            return [
                self._feature_name_prefix + transformation_feature.feature_name
                for transformation_feature in self._transformation_features
            ]

        else:
            return [
                transformation_feature.feature_name
                for transformation_feature in self._transformation_features
            ]

    @property
    def unprefixed_transformation_features(self) -> List[str]:
        """
        List of feature name used in the transformation function without the feature name prefix.
        """
        return [
            transformation_feature.feature_name
            for transformation_feature in self._transformation_features
        ]

    @property
    def feature_name_prefix(self) -> Optional[str]:
        """The feature name prefix that needs to be added to the feature names"""
        return self._feature_name_prefix

    @property
    def statistics_features(self) -> List[str]:
        """
        List of feature names that require statistics
        """
        return [
            transformation_feature.feature_name
            for transformation_feature in self._transformation_features
            if transformation_feature.statistic_argument_name is not None
        ]

    @property
    def _statistics_argument_mapping(self) -> Dict[str, str]:
        """
        Dictionary that maps feature names to the statistics arguments names in the User defined function.
        """
        return {
            transformation_feature.feature_name: transformation_feature.statistic_argument_name
            for transformation_feature in self._transformation_features
        }

    @property
    def _statistics_argument_names(self) -> List[str]:
        """
        List of argument names required for statistics
        """
        return [
            transformation_feature.statistic_argument_name
            for transformation_feature in self._transformation_features
            if transformation_feature.statistic_argument_name is not None
        ]

    @property
    def dropped_features(self) -> List[str]:
        """
        List of features that will be dropped after the UDF is applied.
        """
        if self._feature_name_prefix and self._dropped_features:
            return [
                self._feature_name_prefix + dropped_feature
                for dropped_feature in self._dropped_features
            ]
        else:
            return self._dropped_features

    @property
    def execution_mode(self) -> UDFExecutionMode:
        return self._execution_mode

    @property
    def transformation_context(self) -> Dict[str, Any]:
        """
        Dictionary that contains the context variables required for the UDF.
        These context variables passed to the UDF during execution.
        """
        return self._transformation_context if self._transformation_context else {}

    @transformation_context.setter
    def transformation_context(self, context_variables: Dict[str, Any]) -> None:
        self._transformation_context = (
            self._validate_transformation_context(context_variables)
            if context_variables
            else {}
        )

    @dropped_features.setter
    def dropped_features(self, features: List[str]) -> None:
        self._dropped_features = HopsworksUdf._validate_and_convert_drop_features(
            features, self.transformation_features, self._feature_name_prefix
        )

    @transformation_statistics.setter
    def transformation_statistics(
        self, statistics: List[FeatureDescriptiveStatistics]
    ) -> None:
        self._statistics = TransformationStatistics(*self._statistics_argument_names)
        for stat in statistics:
            if stat.feature_name in self._statistics_argument_mapping.keys():
                self._statistics.set_statistics(
                    self._statistics_argument_mapping[stat.feature_name], stat.to_dict()
                )

    @output_column_names.setter
    def output_column_names(self, output_col_names: Union[str, List[str]]) -> None:
        if not isinstance(output_col_names, List):
            output_col_names = [output_col_names]
        self._validate_output_col_name(output_col_names)
        self._output_column_names = output_col_names

    def __repr__(self):
        return f'{self.function_name}({", ".join(self.transformation_features)})'
