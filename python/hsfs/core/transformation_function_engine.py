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
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, TypeVar

import networkx as nx
import pandas as pd
from hopsworks_common.client import exceptions
from hsfs import (
    engine,
    feature_view,
    statistics,
    training_dataset,
    transformation_function,
)
from hsfs.core import transformation_function_api
from hsfs.hopsworks_udf import HopsworksUdf, UDFExecutionMode


if TYPE_CHECKING:
    import polars as pl
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
        if TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.__process_pool.shutdown(wait=True)
            TransformationFunctionEngine.__process_pool = None

    @staticmethod
    def create_process_pool(n_processes: int = None):
        if TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.shutdown_process_pool()
        TransformationFunctionEngine.__process_pool = ProcessPoolExecutor(
            max_workers=n_processes
        )

    @staticmethod
    def _validate_transformation_function_arguments(
        execution_graph: list[list[transformation_function.TransformationFunction]],
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        request_parameters: dict[str, Any] = None,
    ) -> None:
        """Function to validate if all arguments required to execute the transformation functions are present are present in the passed data or request parameters.

        Parameters:
            transformation_functions: List of transformation functions to validate.
            data: The dataframe or list of dictionaries to validate the transformation functions against.
            request_parameters: Request parameters to validate the transformation functions against.

        Raises:
            exceptions.TransformationFunctionException: If the arguments required to execute the transformation functions are not present in the passed data or request parameters.
        """
        transformation_function_output_feature = set()
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
        """Function to apply the transformation functions to the passed data.

        This function validates the arguments and calls the required function to apply the transformation functions to the passed data.
        For spark engine, the transformation functions are pushed down to Spark and is completely handled by the Spark engine.

        Parameters:
            transformation_functions: List of transformation functions to apply.
            data: The dataframe or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            expected_features: Expected features to be present in the data, this is required to avoid dropping features with same names that are available from other feature groups in a feature view.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if not execution_graph or data is None:
            return data

        # TODO : Handle the case where one a transformation function is passed as a parameter to the other transformation function.
        TransformationFunctionEngine._validate_transformation_function_arguments(
            execution_graph=execution_graph,
            data=data,
            request_parameters=request_parameters,
        )

        for transformation_functions in execution_graph:
            if isinstance(data, dict) or engine.get_type() != "spark":
                # If the data is a dictionary or if the engine is not spark, we execute the transformation functions using.
                data = TransformationFunctionEngine._apply_transformation_functions(
                    transformation_functions=transformation_functions,
                    data=data,
                    online=online,
                    transformation_context=transformation_context,
                    request_parameters=request_parameters,
                    expected_features=expected_features,
                )
            else:
                # In the case of spark, we execute the transformation functions using the spark engine since the transformations are pushed down to Spark and are not executed in Python.
                data = engine.get_instance()._apply_transformation_function(
                    transformation_functions=transformation_functions,
                    dataset=data,
                    transformation_context=transformation_context,
                    expected_features=expected_features,
                )
        return data

    @staticmethod
    def _apply_transformation_functions(
        transformation_functions: list[transformation_function.TransformationFunction],
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        online: bool = False,
        transformation_context: dict[str, Any] | list[dict[str, Any]] = None,
        request_parameters: dict[str, Any] = None,
        expected_features: set[str] = None,
        n_processes: int = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Function to apply the transformation functions to the passed dataframe or list of dictionaries.

        This function is only used when the engine is python or if the passed data is a dictionary.

        Parameters:
            transformation_functions: List of transformation functions to apply.
            data: The dataframe or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            expected_features: Expected features to be present in the data, this is required to avoid dropping features with same names that are available from other feature groups in a feature view.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if not TransformationFunctionEngine.__process_pool:
            TransformationFunctionEngine.create_process_pool(n_processes)

        dropped_features: set[str] = set()

        if isinstance(data, dict):
            transformed_data = data.copy()
        else:
            transformed_data = engine.get_instance().shallow_copy_dataframe(data)

        if request_parameters:
            for key in request_parameters:
                transformed_data[key] = request_parameters[key]

        futures = []
        execution_engine = engine.get_instance()
        for tf in transformation_functions:
            udf = tf.hopsworks_udf
            udf.transformation_context = transformation_context

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
                    execution_engine=execution_engine,
                    engine_type=engine.get_type(),
                )
            )

        for future in as_completed(futures):
            # TODO: This code is utter garbage. Fix this.
            result = future.result()
            if isinstance(result, dict):
                for col in result:
                    if col not in transformed_data:
                        transformed_data[col] = result[col]
            else:
                for col in result.columns:
                    if col not in transformed_data.columns:
                        import polars as pl

                        if isinstance(transformed_data, pl.DataFrame):
                            if isinstance(result[col], pd.Series):
                                transformed_data = transformed_data.with_columns(
                                    pl.from_pandas(result[col])
                                )
                            else:
                                transformed_data = transformed_data.with_columns(
                                    result[col]
                                )
                        else:
                            transformed_data[col] = result[col]

        if isinstance(transformed_data, dict):
            transformed_data = {
                k: v for k, v in transformed_data.items() if k not in dropped_features
            }
        else:
            transformed_data = engine.get_instance().drop_columns(
                transformed_data, dropped_features
            )

        return transformed_data

    @staticmethod
    def execute_udf(
        udf: HopsworksUdf,
        data: spark_sql.DataFrame | pl.DataFrame | pd.DataFrame | dict[str, Any],
        execution_engine,
        engine_type: str | None = None,
        online: bool = False,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Function to execute the UDF used to defined a transformation function on passed data.

        The functions pushes the execution of Pandas and Python Dataframes to the Python Engine and handles the execution dictionaries.

        Parameters:
            udf: The transformation function to execute.
            data: The dataframe or list of dictionaries to execute the transformation function on.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        if execution_engine.check_supported_dataframe(data):
            return execution_engine.apply_udf_on_dataframe(
                udf=udf, dataframe=data, online=online, engine_type=engine_type
            )
        if isinstance(data, dict):
            return TransformationFunctionEngine.apply_udf_on_dict(
                udf=udf, data=data, online=online, engine_type=engine_type
            )
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
        """Function to apply the UDF used to defined a transformation function on a dictionary.

        The function is not pushed to the Python Engine since it should be executed this function in both the Python and Spark Kernel.

        Parameters:
            udf: The transformation function to execute.
            data: The dictionary to execute the transformation function on.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.

        Returns:
            The updated dictionary with the transformations applied.
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
        funcs = {
            output_column_name: tf
            for tf in transformation_functions
            for output_column_name in tf.hopsworks_udf.output_column_names
        }
        G = nx.DiGraph()

        # Add nodes
        G.add_nodes_from(funcs)

        # Add edges: dependency -> dependent
        for name, tf in funcs.items():
            transformation_features = tf.hopsworks_udf.transformation_features
            for transformation_feature in transformation_features:
                if transformation_feature in funcs:
                    G.add_edge(transformation_feature, name)

        levels = []

        while G.nodes:
            ready = {n for n, d in G.in_degree() if d == 0}
            if not ready:
                raise ValueError("Dependency cycle detected")
            level = []
            for n in ready:
                tf = funcs.get(n)
                if tf not in level:
                    level.append(tf)
            levels.append(level)
            G.remove_nodes_from(ready)

        return levels

    @staticmethod
    def print_transformation_function_execution_graph(execution_graph):
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
