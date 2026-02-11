#
#   Copyright 2021 Logical Clocks AB
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

import atexit
import multiprocessing
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, TypeVar

import networkx as nx
import pandas as pd
from hopsworks_common.client import exceptions
from hopsworks_common.core.constants import HAS_POLARS
from hsfs import (
    engine,
    feature_view,
    statistics,
    training_dataset,
    transformation_function,
)
from hsfs.core import transformation_function_api
from hsfs.hopsworks_udf import HopsworksUdf, UDFExecutionMode


if HAS_POLARS:
    import polars as pl


if TYPE_CHECKING:
    import pyspark.sql as spark_sql
    from hsfs import feature_view, statistics, training_dataset, transformation_function


class TransformationFunctionEngine:
    BUILTIN_FN_NAMES = [
        "min_max_scaler",
        "standard_scaler",
        "robust_scaler",
        "label_encoder",
    ]
    AMBIGUOUS_FEATURE_ERROR = (
        "Provided feature '{}' in transformation functions is ambiguous and exists in more than one feature groups."
        "You can provide the feature with the prefix that was specified in the join."
    )
    FEATURE_NOT_EXIST_ERROR = "Provided feature '{}' in transformation functions do not exist in any of the feature groups."

    __process_pool = None

    def __init__(self, feature_store_id: int):
        self._feature_store_id = feature_store_id
        self._transformation_function_api: transformation_function_api.TransformationFunctionApi = transformation_function_api.TransformationFunctionApi(
            feature_store_id
        )
        atexit.register(TransformationFunctionEngine.shutdown_process_pool)

    def save(
        self, transformation_fn_instance: transformation_function.TransformationFunction
    ) -> transformation_function.TransformationFunction:
        """Save a transformation function into the feature store.

        # Argument
            transformation_fn_instance `transformation_function.TransformationFunction`: The transformation function to be saved into the feature store.
        """
        self._transformation_function_api.register_transformation_fn(
            transformation_fn_instance
        )

    def get_transformation_fn(
        self, name: str, version: int | None = None
    ) -> (
        transformation_function.TransformationFunction
        | list[transformation_function.TransformationFunction]
    ):
        """Retrieve a transformation function from the feature store.

        If only the name of the transformation function is provided then all the versions of the transformation functions are returned as a list.
        If both name and version are not provided then all transformation functions saved in the feature view is returned.

        # Argument
            name ` Optional[str]`: The name of the transformation function to be retrieved.
            version `Optional[int]`: The version of the transformation function to be retrieved.

        Returns:
            `Union[transformation_function.TransformationFunction, list[transformation_function.TransformationFunction]]` : A transformation function if name and version is provided. A list of transformation functions if only name is provided.
        """
        return self._transformation_function_api.get_transformation_fn(name, version)

    def get_transformation_fns(
        self,
    ) -> list[transformation_function.TransformationFunction]:
        """Get all the transformation functions in the feature store.

        Returns:
            A list of transformation functions.
        """
        transformation_fn_instances = (
            self._transformation_function_api.get_transformation_fn(
                name=None, version=None
            )
        )
        transformation_fns = []
        for (
            transformation_fn_instance
        ) in transformation_fn_instances:  # todo what is the point of this?
            transformation_fns.append(transformation_fn_instance)
        return transformation_fns

    @staticmethod
    def shutdown_process_pool():
        """Shut down the process pool used for parallel execution of transformation functions.

        Waits for all running tasks to complete before shutting down.
        Safe to call even if no process pool has been created.
        """
        if TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.__process_pool.shutdown(wait=True)
            TransformationFunctionEngine.__process_pool = None

    @staticmethod
    def create_process_pool(n_processes: int = None):
        """Create a process pool for parallel execution of transformation functions.

        If a process pool already exists, it is shut down before creating a new one.
        Uses `fork` on Unix-based systems and `spawn` on Windows.

        Parameters:
            n_processes: Number of worker processes.
                If not provided, defaults to the number of available CPU cores.
        """
        if TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.shutdown_process_pool()
        mp_context = multiprocessing.get_context(
            "fork" if sys.platform != "win32" else "spawn"
        )
        TransformationFunctionEngine.__process_pool = ProcessPoolExecutor(
            max_workers=n_processes, mp_context=mp_context
        )

    @staticmethod
    def _validate_transformation_function_arguments(
        execution_graph: list[list[transformation_function.TransformationFunction]],
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        request_parameters: dict[str, Any] = None,
    ) -> None:
        """Validate that all arguments required to execute the transformation functions are present in the passed data or request parameters.

        Parameters:
            execution_graph: The transformation DAG containing the transformation functions to validate, organized by dependency level.
            data: The dataframe or dictionary to validate the transformation functions against.
            request_parameters: Request parameters to validate the transformation functions against.

        Raises:
            exceptions.TransformationFunctionException: If the arguments required to execute the transformation functions are not present in the passed data or request parameters.
        """
        transformation_function_output_feature = set()
        if isinstance(request_parameters, list) and len(request_parameters) != len(
            data
        ):
            raise exceptions.TransformationFunctionException(
                "Request Parameters should be a Dictionary, None, empty or be a list having the same length as the number of rows in the data provided."
            )
        for transformation_functions in execution_graph:
            for tf in transformation_functions:
                if engine.get_instance().check_supported_dataframe(data):
                    missing_features = set(
                        tf.hopsworks_udf.transformation_features
                    ) - set(data.columns)
                elif isinstance(data, dict):
                    missing_features = set(
                        tf.hopsworks_udf.transformation_features
                    ) - set(data.keys())
                else:
                    raise exceptions.FeatureStoreException(
                        f"Dataframe type {type(data)} not supported in the engine."
                    )

                if request_parameters:
                    missing_features = missing_features - set(request_parameters.keys())
                    if tf.hopsworks_udf.feature_name_prefix:
                        missing_features = missing_features - {
                            tf.hopsworks_udf.feature_name_prefix + feature
                            for feature in request_parameters
                        }
                missing_features = (
                    missing_features - transformation_function_output_feature
                )

                if missing_features:
                    raise exceptions.TransformationFunctionException(
                        message=f"The following feature(s): `{', '.join(missing_features)}`, required for the transformation function '{tf.hopsworks_udf.function_name}' are not available.",
                        missing_features=missing_features,
                        transformation_function_name=tf.hopsworks_udf.function_name,
                        transformation_type=tf.transformation_type.value,
                    )
            transformation_function_output_feature.update(
                {
                    output_column_name
                    for tf in transformation_functions
                    for output_column_name in tf.hopsworks_udf.output_column_names
                }
            )

    @staticmethod
    def apply_transformation_functions(
        execution_graph: list[list[transformation_function.TransformationFunction]],
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        online: bool = False,
        transformation_context: dict[str, Any] | list[dict[str, Any]] = None,
        request_parameters: dict[str, Any] = None,
        expected_features: set[str] = None,
        n_processes: int = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Apply the transformation functions from the DAG to the passed data.

        Iterates through each level of the DAG, applying transformations at each level.
        Transformations within the same level are executed in parallel using a process pool.
        For the Spark engine, the transformation functions are pushed down to Spark.

        Parameters:
            execution_graph: The transformation DAG containing transformation functions organized by dependency level.
            data: The dataframe, dictionary, or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase.
                This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            expected_features: Expected features to be present in the data.
                This is required to avoid dropping features with same names that are available from other feature groups in a feature view.
            n_processes: Number of processes to use for parallel execution of transformation functions.
                If not provided, the number of processes will be set to the number of available CPU cores.
                This parameter is only applicable when the engine is `python`.
                With Spark, the transformations are pushed down to the Spark engine.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if not execution_graph or data is None:
            return data

        TransformationFunctionEngine._validate_transformation_function_arguments(
            execution_graph=execution_graph,
            data=data,
            request_parameters=request_parameters,
        )

        dropped_features: set[str] = set()
        for transformation_functions in execution_graph:
            if isinstance(data, (dict, list)) or engine.get_type() != "spark":
                # If the data is a dictionary or if the engine is not spark, we execute the transformation functions using.
                data = TransformationFunctionEngine._apply_transformation_functions(
                    transformation_functions=transformation_functions,
                    data=data,
                    online=online,
                    transformation_context=transformation_context,
                    request_parameters=request_parameters,
                    expected_features=expected_features,
                    n_processes=n_processes,
                    dropped_features=dropped_features,
                )
            else:
                # In the case of spark, we execute the transformation functions using the spark engine since the transformations are pushed down to Spark and are not executed in Python.
                data = engine.get_instance()._apply_transformation_function(
                    transformation_functions=transformation_functions,
                    dataset=data,
                    transformation_context=transformation_context,
                    expected_features=expected_features,
                    request_parameters=request_parameters,
                    dropped_features=dropped_features,
                )
        return data

    @staticmethod
    def _update_request_parameter_data(transformed_data, request_parameters):
        """Merge request parameters into the transformed data.

        Handles both single-record and batch request parameters by inserting them as additional columns or keys into the transformed data.

        Parameters:
            transformed_data: The data to update with request parameters.
            request_parameters: Request parameters as a dictionary for a single record or a list of dictionaries for batch operations.

        Returns:
            The updated data with request parameters merged in.
        """
        if isinstance(request_parameters, list):
            if isinstance(transformed_data, pd.DataFrame):
                request_parameters = pd.DataFrame(request_parameters)
                for col in request_parameters:
                    transformed_data[col] = request_parameters[col]
            elif HAS_POLARS and isinstance(transformed_data, pl.DataFrame):
                request_parameters = pl.DataFrame(request_parameters)
                for col in request_parameters:
                    transformed_data.with_columns(col, request_parameters[col])
            else:
                for data, rq in zip(transformed_data, request_parameters):
                    for col in rq:
                        data[col] = rq[col]
        else:
            if isinstance(transformed_data, list):
                for row in transformed_data:
                    for key in request_parameters:
                        row[key] = request_parameters[key]
            for key in request_parameters:
                if isinstance(transformed_data, (pd.DataFrame, dict)):
                    transformed_data[key] = request_parameters[key]
                else:
                    transformed_data.withColumn(col, request_parameters[key])
        return transformed_data

    @staticmethod
    def _apply_transformation_functions(
        transformation_functions: list[transformation_function.TransformationFunction],
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        online: bool = False,
        transformation_context: dict[str, Any] | list[dict[str, Any]] = None,
        request_parameters: dict[str, Any] = None,
        expected_features: set[str] = None,
        n_processes: int = None,
        dropped_features: set[str] = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Apply a single level of transformation functions in parallel using a process pool.

        This function handles one level of the DAG, submitting each transformation function to a process pool for concurrent execution.
        It is only used when the engine is Python or if the passed data is a dictionary.

        Parameters:
            transformation_functions: List of transformation functions at the same execution level to apply in parallel.
            data: The dataframe, dictionary, or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase.
                This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            expected_features: Expected features to be present in the data.
                This is required to avoid dropping features with same names that are available from other feature groups in a feature view.
            n_processes: Number of processes to use for parallel execution of transformation functions.
                If not provided, the number of processes will be set to the number of available CPU cores.
            dropped_features: Accumulator set of features to be dropped from the data.
                Contains features that were marked for removal in previous DAG levels.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        dropped_features = dropped_features if dropped_features is not None else set()

        if not TransformationFunctionEngine.__process_pool or (
            n_processes
            and TransformationFunctionEngine.__process_pool._max_workers != n_processes
        ):
            TransformationFunctionEngine.create_process_pool(n_processes)

        if isinstance(data, (list, dict)):
            transformed_data = data.copy()
        else:
            transformed_data = engine.get_instance().shallow_copy_dataframe(data)
            transformed_data_columns = list(transformed_data.columns)

        if request_parameters:
            transformed_data = (
                TransformationFunctionEngine._update_request_parameter_data(
                    transformed_data, request_parameters
                )
            )

        futures = []
        execution_engine = engine.get_instance()
        for tf in transformation_functions:
            udf = tf.hopsworks_udf
            udf.transformation_context = transformation_context
            if not isinstance(transformed_data, dict):
                for col in udf.output_column_names:
                    if col in transformed_data_columns:
                        transformed_data_columns.remove(col)
                    transformed_data_columns.append(col)

            if udf.dropped_features:
                dropped_features.update(
                    {f for f in udf.dropped_features if f not in expected_features}
                    if expected_features
                    else udf.dropped_features
                )  # Drop features that are not expected, this is required to avoid dropping features having same name that are available from other feature groups.

            futures.append(
                TransformationFunctionEngine.__process_pool.submit(
                    TransformationFunctionEngine.execute_udf,
                    udf=udf,
                    data=transformed_data,
                    online=online,
                    engine_type=engine.get_type(),
                )
            )
        for future in as_completed(futures):
            result = future.result()
            if isinstance(transformed_data, dict):
                transformed_data.update(result)
            else:
                overwritten_columns = set(result.columns) & set(
                    transformed_data.columns
                )
                if overwritten_columns:
                    transformed_data = engine.get_instance().drop_columns(
                        transformed_data, overwritten_columns
                    )
                transformed_data = execution_engine.concat_dataframes(
                    [transformed_data, result]
                )

        if isinstance(transformed_data, dict):
            transformed_data = {
                k: v for k, v in transformed_data.items() if k not in dropped_features
            }
        else:
            transformed_data = transformed_data[transformed_data_columns]
            transformed_data = engine.get_instance().drop_columns(
                transformed_data, dropped_features
            )

        return transformed_data

    @staticmethod
    def execute_udf(
        udf: HopsworksUdf,
        data: spark_sql.DataFrame
        | pl.DataFrame
        | pd.DataFrame
        | dict[str, Any]
        | list[dict[str, Any]],
        engine_type: str | None = None,
        online: bool = False,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Function to execute the UDF used to defined a transformation function on passed data.

        The functions pushes the execution of Pandas and Python Dataframes to the Python Engine and handles the execution dictionaries.

        Parameters:
            udf: The transformation function to execute.
            data: The dataframe, dictionary, or list of dictionaries to execute the transformation function on.
            engine_type: The engine type to use for execution.
                When set to `"spark"`, offline dictionary execution is not supported.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        execution_engine = engine.get_instance()
        if execution_engine.check_supported_dataframe(data):
            return execution_engine.apply_udf_on_dataframe(
                udf=udf, dataframe=data, online=online, engine_type=engine_type
            )
        if isinstance(data, dict):
            return TransformationFunctionEngine.apply_udf_on_dict(
                udf=udf, data=data, online=online, engine_type=engine_type
            )
        if isinstance(data, list):
            transformed_results = []
            for row in data:
                transformed_results.append(
                    TransformationFunctionEngine.apply_udf_on_dict(
                        udf=udf, data=row, online=online, engine_type=engine_type
                    )
                )
            return transformed_results
        raise exceptions.FeatureStoreException(
            f"Dataframe type {type(data)} not supported in the engine."
        )

    @staticmethod
    def apply_udf_on_dict(
        udf: HopsworksUdf,
        data: dict[str, Any],
        online: bool | None = True,
        engine_type: str | None = None,
    ) -> dict[str, Any]:
        """Apply the UDF of a transformation function on a single dictionary record.

        This function is not pushed to the Python Engine since it needs to be executable in both the Python and Spark kernels.

        Parameters:
            udf: The transformation function to execute.
            data: The dictionary to execute the transformation function on.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.
            engine_type: The engine type to use for execution.
                When set to `"spark"`, offline dictionary execution raises an error because Spark requires DataFrames.

        Returns:
            A dictionary containing only the transformed output columns.
        """
        features = []

        if not online and engine_type == "spark":
            raise exceptions.FeatureStoreException(
                "Cannot apply transformation functions on a dictionary in offline mode when the engine is spark. Please use the python engine or use the online mode."
            )

        for unprefixed_feature in udf.unprefixed_transformation_features:
            prefixed_feature = (
                udf.feature_name_prefix + unprefixed_feature
                if udf.feature_name_prefix
                else unprefixed_feature
            )
            feature_value = data.get(prefixed_feature, data.get(unprefixed_feature))

            if (
                udf.execution_mode.get_current_execution_mode(online=online)
                == UDFExecutionMode.PANDAS
            ):
                features.append(pd.Series(feature_value))
            else:
                features.append(feature_value)

        transformed_result = udf.get_udf(online=online, engine_type=engine_type)(
            *features
        )

        transformed_dict = {}

        if (
            udf.execution_mode.get_current_execution_mode(online=online)
            == UDFExecutionMode.PANDAS
        ):
            # Pandas UDF return can return a pandas series or a pandas dataframe, so we need to cast it back to a dictionary.
            if isinstance(transformed_result, pd.Series):
                transformed_dict[transformed_result.name] = transformed_result.values[0]
            else:
                for col in transformed_result:
                    transformed_dict[col] = transformed_result[col].values[0]
        else:
            # Python UDF return can return a tuple or a list, so we need to cast it back to a dictionary.
            if isinstance(transformed_result, (tuple, list)):
                for index, result in enumerate(transformed_result):
                    transformed_dict[udf.output_column_names[index]] = result
            else:
                transformed_dict[udf.output_column_names[0]] = transformed_result

        return transformed_dict

    def delete(
        self,
        transformation_function_instance: transformation_function.TransformationFunction,
    ) -> None:
        """Delete a transformation function from the feature store.

        Parameters:
            transformation_function_instance `transformation_function.TransformationFunction`: The transformation function to be removed from the feature store.
        """
        self._transformation_function_api.delete(transformation_function_instance)

    @staticmethod
    def compute_transformation_fn_statistics(
        training_dataset_obj: training_dataset.TrainingDataset,
        statistics_features: list[str],
        label_encoder_features: list[str],
        feature_dataframe: pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
        feature_view_obj: feature_view.FeatureView,
    ) -> statistics.Statistics:
        """Compute the statistics required for a training dataset object.

        Parameters:
            training_dataset_obj : The training dataset for which the statistics is to be computed.
            statistics_features : The list of features for which the statistics should be computed.
            label_encoder_features : Features used for label encoding.
            feature_dataframe : The dataframe that contains the data for which the statistics must be computed.
            feature_view_obj : The feature view in which the training data is being created.

        Returns:
            The statistics object that contains the statistics for each features.
        """
        return training_dataset_obj._statistics_engine.compute_transformation_fn_statistics(
            td_metadata_instance=training_dataset_obj,
            columns=statistics_features,
            label_encoder_features=label_encoder_features,  # label encoded features only
            feature_dataframe=feature_dataframe,
            feature_view_obj=feature_view_obj,
        )

    @staticmethod
    def get_ready_to_use_transformation_fns(
        feature_view: feature_view.FeatureView,
        training_dataset_version: int | None = None,
    ) -> list[transformation_function.TransformationFunction]:
        """Function that updates statistics required for all transformation functions in the feature view based on training dataset version.

        Parameters:
            feature_view `FeatureView`: The feature view in which the training data is being created.
            training_dataset_version `TrainingDataset`: The training version used to update the statistics used in the transformation functions.

        Returns:
            `list[transformation_function.TransformationFunction]` : List of transformation functions.
        """
        # check if transformation functions require statistics
        is_stat_required = any(
            tf.hopsworks_udf.statistics_required
            for tf in feature_view.transformation_functions
        )
        if not is_stat_required:
            td_tffn_stats = None
        else:
            # if there are any transformation functions that require statistics get related statistics and
            # populate with relevant arguments
            # there should be only one statistics object with before_transformation=true
            if training_dataset_version is None:
                raise ValueError(
                    "Training data version is required for transformation. Call `feature_view.init_serving(version)` "
                    "or `feature_view.init_batch_scoring(version)` to pass the training dataset version."
                    "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
                )
            td_tffn_stats = feature_view._statistics_engine.get(
                feature_view,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

        if is_stat_required and td_tffn_stats is None:
            raise ValueError(
                "No statistics available for initializing transformation functions."
                "Training data can be created by `feature_view.create_training_data` or `feature_view.training_data`."
            )

        if is_stat_required:
            for transformation_function in feature_view.transformation_functions:
                transformation_function.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )
        return feature_view.transformation_functions

    @staticmethod
    def compute_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        dataset: dict[
            str, pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame")
        ]
        | pd.DataFrame
        | pl.DataFrame
        | TypeVar("pyspark.sql.DataFrame"),
    ) -> None:
        """Function that computes and sets the statistics required for the UDF used for transformation.

        The function assigns the statistics computed to hopsworks UDF object so that the statistics can be used when UDF is executed.

        Parameters:
            training_dataset : The training dataset for which the statistics is to be computed.
            feature_view_obj : The feature view in which the training data is being created.
            dataset : A dataframe that contains the training data or a dictionary that contains both the training and test data.
        """
        statistics_features: set[str] = set()
        label_encoder_features: set[str] = set()

        # Finding the features for which statistics is required
        for tf in feature_view_obj.transformation_functions:
            statistics_features.update(tf.hopsworks_udf.statistics_features)
            if (
                tf.hopsworks_udf.function_name == "label_encoder"
                or tf.hopsworks_udf.function_name == "one_hot_encoder"
            ):
                label_encoder_features.update(tf.hopsworks_udf.statistics_features)
        if statistics_features:
            # compute statistics on training data
            if training_dataset.splits:
                # compute statistics before transformations are applied
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        list(label_encoder_features),
                        dataset.get(training_dataset.train_split),
                        feature_view_obj,
                    )
                )
            else:
                stats = (
                    TransformationFunctionEngine.compute_transformation_fn_statistics(
                        training_dataset,
                        list(statistics_features),
                        list(label_encoder_features),
                        dataset,
                        feature_view_obj,
                    )
                )

            # Set statistics computed in the hopsworks UDF
            for tf in feature_view_obj.transformation_functions:
                tf.transformation_statistics = stats.feature_descriptive_statistics

    @staticmethod
    def build_transformation_function_execution_graph(transformation_functions):
        """Build a DAG (Directed Acyclic Graph) to determine the execution order of transformation functions.

        Analyzes the dependencies between transformation functions by inspecting their input and output features.
        The resulting DAG is a list of levels, where each level contains transformation functions that are independent of each other and can be executed in parallel.
        Transformation functions at level `n` depend on outputs from level `n-1`.

        Parameters:
            transformation_functions: Flat list of transformation functions to organize into a DAG.

        Returns:
            A list of lists of [`TransformationFunction`][hsfs.transformation_function.TransformationFunction] instances, where each inner list represents a level of independent transformations.

        Raises:
            TransformationFunctionException: If a cyclic dependency is detected among the transformation functions.
        """
        funcs = {
            output_column_name: tf
            for tf in transformation_functions
            for output_column_name in tf.hopsworks_udf.output_column_names
        }
        order_index = {
            str(func): index for index, func in enumerate(transformation_functions)
        }
        G = nx.DiGraph()

        # Add nodes
        G.add_nodes_from(funcs)

        # Add edges: dependency -> dependent
        for name, tf in funcs.items():
            transformation_features = tf.hopsworks_udf.transformation_features
            # If input feature name is equal to the function name, then the feature in the dataframe will be overwritten with the transformed data.
            # So, we don't need to add an edge for the feature, as it would be detected as a cyclic dependency.
            if not any(
                transformation_feature == tf.hopsworks_udf.function_name
                for transformation_feature in transformation_features
            ):
                for transformation_feature in transformation_features:
                    if transformation_feature in funcs:
                        G.add_edge(transformation_feature, name)

        levels = []

        while G.nodes:
            ready = {n for n, d in G.in_degree() if d == 0}
            if not ready:
                raise exceptions.TransformationFunctionException(
                    "Cyclic dependency detected in transformation functions."
                )
            level = []
            for n in ready:
                tf = funcs.get(n)
                if tf not in level:
                    level.append(tf)
            levels.append(sorted(level, key=lambda x: order_index[str(x)]))
            G.remove_nodes_from(ready)

        return levels

    @staticmethod
    def print_transformation_function_execution_graph(execution_graph):
        """Print a visual representation of the transformation function DAG.

        Displays the DAG as a series of levels separated by arrows, showing the order in which transformation functions will be executed.
        Transformation functions on the same level are independent and can run in parallel.

        Parameters:
            execution_graph: The DAG to print, as returned by [`build_transformation_function_execution_graph`][hsfs.core.transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph].
        """
        if not execution_graph:
            return
        udfs = [[tf.hopsworks_udf for tf in tfs] for tfs in execution_graph]
        max_len = max([len(str(udf)) for udf in udfs])
        tf_strings = [str(udf).center(max_len) for udf in udfs]
        print(f"\n{'↓'.center(max_len)}\n".join(tf_strings))

    @staticmethod
    def get_and_set_feature_statistics(
        training_dataset: training_dataset.TrainingDataset,
        feature_view_obj: feature_view.FeatureView,
        training_dataset_version: int = None,
    ) -> None:
        """Function that gets the transformation statistics computed while creating the training dataset from the backend and assigns it to the hopsworks UDF object.

        The function assigns the statistics computed to hopsworks UDF object so that the statistics can be used when UDF is executed.

        # Argument
            training_dataset_obj `TrainingDataset`: The training dataset for which the statistics is to be computed.
            feature_view `FeatureView`: The feature view in which the training data is being created.
            training_dataset_version `int`: The version of the training dataset for which the statistics is to be retrieved.

        Raises:
            ValueError : If the statistics are not present in the backend.
        """
        is_stat_required = any(
            tf.hopsworks_udf.statistics_required
            for tf in feature_view_obj.transformation_functions
        )

        if is_stat_required:
            td_tffn_stats = training_dataset._statistics_engine.get(
                feature_view_obj,
                before_transformation=True,
                training_dataset_version=training_dataset_version,
            )

            if td_tffn_stats is None:
                raise ValueError(
                    "No statistics available for initializing transformation functions."
                )

            for tf in feature_view_obj.transformation_functions:
                tf.transformation_statistics = (
                    td_tffn_stats.feature_descriptive_statistics
                )
