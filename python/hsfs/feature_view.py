#
#   Copyright 2022 Logical Clocks AB
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
import logging
import warnings
from datetime import date, datetime
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import humps
import pandas as pd
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core import alerts_api
from hopsworks_common.core.constants import HAS_NUMPY, HAS_POLARS
from hsfs import (
    feature_group,
    storage_connector,
    tag,
    training_dataset,
    training_dataset_feature,
    usage,
    util,
)
from hsfs import serving_key as skm
from hsfs.constructor import filter, query
from hsfs.constructor.filter import Filter, Logic
from hsfs.core import (
    explicit_provenance,
    feature_monitoring_config_engine,
    feature_monitoring_result_engine,
    feature_view_engine,
    job,
    statistics_engine,
    transformation_function_engine,
    vector_server,
)
from hsfs.core import feature_monitoring_config as fmc
from hsfs.core import feature_monitoring_result as fmr
from hsfs.core.feature_logging import FeatureLogging
from hsfs.core.feature_view_api import FeatureViewApi
from hsfs.core.job import Job
from hsfs.core.vector_db_client import VectorDbClient
from hsfs.decorators import typechecked
from hsfs.feature import Feature
from hsfs.feature_logger import FeatureLogger
from hsfs.hopsworks_udf import HopsworksUdf
from hsfs.statistics import Statistics
from hsfs.statistics_config import StatisticsConfig
from hsfs.training_dataset_split import TrainingDatasetSplit
from hsfs.transformation_function import TransformationFunction, TransformationType
from hsml.model import Model


if HAS_NUMPY:
    import numpy as np


_logger = logging.getLogger(__name__)

TrainingDatasetDataFrameTypes = Union[
    pd.DataFrame,
    TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
    TypeVar("pyspark.RDD"),  # noqa: F821
    np.ndarray,
    List[List[Any]],
]

if HAS_POLARS:
    import polars as pl

    TrainingDatasetDataFrameTypes = Union[
        TrainingDatasetDataFrameTypes,
        pl.DataFrame,
    ]


SplineDataFrameTypes = Union[
    pd.DataFrame,
    TypeVar("pyspark.sql.DataFrame"),  # noqa: F821
    TypeVar("pyspark.RDD"),  # noqa: F821
    np.ndarray,
    List[List[Any]],
    TypeVar("SplineGroup"),  # noqa: F821
]


_logger = logging.getLogger(__name__)


@typechecked
class FeatureView:
    ENTITY_TYPE = "featureview"
    NOT_FOUND_ERROR_CODE = 270181

    def __init__(
        self,
        name: str,
        query: query.Query,
        featurestore_id: int,
        id: Optional[int] = None,
        version: Optional[int] = None,
        description: Optional[str] = "",
        labels: Optional[List[str]] = None,
        inference_helper_columns: Optional[List[str]] = None,
        training_helper_columns: Optional[List[str]] = None,
        transformation_functions: Optional[
            List[Union[TransformationFunction, HopsworksUdf]]
        ] = None,
        featurestore_name: Optional[str] = None,
        serving_keys: Optional[List[skm.ServingKey]] = None,
        logging_enabled: Optional[bool] = False,
        **kwargs,
    ) -> None:
        self._name = name
        self._id = id
        self._query = query
        # Check if query has any ambiguous columns and print warning in these cases:
        query.check_and_warn_ambiguous_features()
        self._featurestore_id = featurestore_id
        self._feature_store_id = featurestore_id  # for consistency with feature group
        self._feature_store_name = featurestore_name
        self._version = version
        self._description = description
        self._labels = labels if labels else []
        self._inference_helper_columns = (
            inference_helper_columns if inference_helper_columns else []
        )
        self._training_helper_columns = (
            training_helper_columns if training_helper_columns else []
        )

        self._transformation_functions: List[TransformationFunction] = []

        if transformation_functions:
            for transformation_function in transformation_functions:
                if not isinstance(transformation_function, TransformationFunction):
                    self._transformation_functions.append(
                        TransformationFunction(
                            self.featurestore_id,
                            hopsworks_udf=transformation_function,
                            version=1,
                            transformation_type=TransformationType.MODEL_DEPENDENT,
                        )
                    )
                else:
                    if (
                        not transformation_function.transformation_type
                        or transformation_function.transformation_type
                        == TransformationType.UNDEFINED
                    ):
                        transformation_function.transformation_type = (
                            TransformationType.MODEL_DEPENDENT
                        )
                    self._transformation_functions.append(transformation_function)

        if self._transformation_functions:
            self._transformation_functions = FeatureView._sort_transformation_functions(
                self._transformation_functions
            )

        self._features = []
        self._feature_view_engine: feature_view_engine.FeatureViewEngine = (
            feature_view_engine.FeatureViewEngine(featurestore_id)
        )
        self._transformation_function_engine: transformation_function_engine.TransformationFunctionEngine = transformation_function_engine.TransformationFunctionEngine(
            featurestore_id
        )
        self.__vector_server: Optional[vector_server.VectorServer] = None
        self.__batch_scoring_server: Optional[vector_server.VectorServer] = None
        self.__feature_groups: List[feature_group.FeatureGroup] = None
        self._serving_keys = serving_keys if serving_keys else []
        self._prefix_serving_key_map = {}
        self._primary_keys: Set[str] = set()  # Lazy initialized via serving keys

        self._vector_db_client = None
        self._statistics_engine = statistics_engine.StatisticsEngine(
            featurestore_id, self.ENTITY_TYPE
        )
        self._logging_enabled = logging_enabled
        self._feature_logging = None
        self._alert_api = alerts_api.AlertsApi()

        if self._id:
            self._init_feature_monitoring_engine()

        # last_accessed_training_dataset is only from the perspective of the client itself, and not the backend.
        # if multiple clients do training datasets operations, each will have their own view of the last accessed.
        # last accessed (read/write) training dataset is not necessarily the newest (highest version).
        self._last_accessed_training_dataset = None

        self._feature_logger = None
        self._serving_training_dataset_version = None

    def get_last_accessed_training_dataset(self):
        return self._last_accessed_training_dataset

    def delete(self) -> None:
        """Delete current feature view, all associated metadata and training data.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # delete a feature view
            feature_view.delete()
            ```

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature view **and** related training dataset **and** materialized data in HopsFS.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        warnings.warn(
            "All jobs associated to feature view `{}`, version `{}` will be removed.".format(
                self._name, self._version
            ),
            util.JobWarning,
            stacklevel=2,
        )
        self._feature_view_engine.delete(self.name, self.version)

    @staticmethod
    def clean(
        feature_store_id: int, feature_view_name: str, feature_view_version: str
    ) -> None:
        """
        Delete the feature view and all associated metadata and training data.
        This can delete corrupted feature view which cannot be retrieved due to a corrupted query for example.

        !!! example
            ```python
            # delete a feature view and all associated metadata
            from hsfs.feature_view import FeatureView

            FeatureView.clean(
                feature_store_id=1,
                feature_view_name='feature_view_name',
                feature_view_version=1
            )
            ```

        !!! danger "Potentially dangerous operation"
            This operation drops all metadata associated with **this version** of the
            feature view **and** related training dataset **and** materialized data in HopsFS.

        # Arguments
            feature_store_id: int. Id of feature store.
            feature_view_name: str. Name of feature view.
            feature_view_version: str. Version of feature view.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if not isinstance(feature_store_id, int):
            raise ValueError("`feature_store_id` should be an integer.")
        FeatureViewApi(feature_store_id).delete_by_name_version(
            feature_view_name, feature_view_version
        )

    def update(self) -> FeatureView:
        """Update the description of the feature view.

        !!! example "Update the feature view with a new description."
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            feature_view.description = "new description"
            feature_view.update()

            # Description is updated in the metadata. Below should return "new description".
            fs.get_feature_view("feature_view_name", 1).description
            ```

        # Returns
            `FeatureView` Updated feature view.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.update(self)

    @usage.method_logger
    def init_serving(
        self,
        training_dataset_version: Optional[int] = None,
        external: Optional[bool] = None,
        options: Optional[Dict[str, Any]] = None,
        init_sql_client: Optional[bool] = None,
        init_rest_client: bool = False,
        reset_rest_client: bool = False,
        config_rest_client: Optional[Dict[str, Any]] = None,
        default_client: Optional[Literal["sql", "rest"]] = None,
        feature_logger: Optional[FeatureLogger] = None,
        **kwargs,
    ) -> None:
        """Initialise feature view to retrieve feature vector from online and offline feature store.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # initialise feature view to retrieve a feature vector
            feature_view.init_serving(training_dataset_version=1)
            ```

        # Arguments
            training_dataset_version: int, optional. Default to be 1 for online feature store.
                Transformation statistics are fetched from training dataset and applied to the feature vector.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used which relies on the private IP.
                Defaults to True if connection to Hopsworks is established from external environment (e.g AWS
                Sagemaker or Google Colab), otherwise to False.
            init_sql_client: boolean, optional. By default the sql client is initialised if no
                client is specified to match legacy behaviour. If set to True, this ensure the online store
                sql client is initialised, otherwise if init_rest_client is set to true it will
                skip initialising the sql client.
            init_rest_client: boolean, defaults to False. By default the rest client is not initialised.
                If set to True, this ensure the online store rest client is initialised. Pass additional configuration
                options via the rest_config parameter. Set reset_rest_client to True to reset the rest client.
            default_client: string, optional. Which client to default to if both are initialised. Defaults to None.
            options: Additional options as key/value pairs for configuring online serving engine.
                * key: kwargs of SqlAlchemy engine creation (See: https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine).
                  For example: `{"pool_size": 10}`
            reset_rest_client: boolean, defaults to False. If set to True, the rest client will be reset and reinitialised with provided configuration.
            config_rest_client: dictionary, optional. Additional configuration options for the rest client. If the client is already initialised,
                this will be ignored. Options include:
                * `host`: string, optional. The host of the online store. Dynamically set if not provided.
                * `port`: int, optional. The port of the online store. Defaults to 4406.
                * `verify_certs`: boolean, optional. Verify the certificates of the online store server. Defaults to True.
                * `api_key`: string, optional. The API key to authenticate with the online store. The api key must be
                    provided if initialising the rest client in an internal environment.
                * `timeout`: int, optional. The timeout for the rest client in seconds. Defaults to 2.
                * `use_ssl`: boolean, optional. Use SSL to connect to the online store. Defaults to True.
            feature_logger: Custom feature logger which [`feature_view.log()`](#log) uses to log feature vectors. If provided,
                feature vectors will not be inserted to logging feature group automatically when `feature_view.log()` is called.

        """
        # initiate batch scoring server
        # `training_dataset_version` should not be set if `None` otherwise backend will look up the td.
        try:
            self.init_batch_scoring(training_dataset_version)
        except ValueError as e:
            # In 3.3 or before, td version is set to 1 by default.
            # For backward compatibility, if a td version is required, set it to 1.
            if "Training data version is required for transformation" in str(e):
                self.init_batch_scoring(1)
            else:
                raise e
        self._serving_training_dataset_version = training_dataset_version
        # Compatibility with 3.7
        if init_sql_client is None:
            init_sql_client = kwargs.get("init_online_store_sql_client", None)
        if init_rest_client is False:
            init_rest_client = kwargs.get("init_online_store_rest_client", False)

        if training_dataset_version is None:
            training_dataset_version = 1
            warnings.warn(
                "No training dataset version was provided to initialise serving. Defaulting to version 1.",
                util.VersionWarning,
                stacklevel=1,
            )

        # initiate single vector server
        self._vector_server.init_serving(
            entity=self,
            external=external,
            inference_helper_columns=True,
            options=options,
            init_sql_client=init_sql_client,
            init_rest_client=init_rest_client,
            reset_rest_client=reset_rest_client,
            config_rest_client=config_rest_client,
            default_client=default_client,
            training_dataset_version=training_dataset_version,
        )

        self._prefix_serving_key_map = dict(
            [
                (f"{sk.prefix}{sk.feature_name}", sk)
                for sk in self._vector_server.serving_keys
            ]
        )
        if len(self._get_embedding_fgs()) > 0:
            self._vector_db_client = VectorDbClient(
                self.query, serving_keys=self._serving_keys
            )

        if feature_logger:
            self._feature_logger = feature_logger
            self._feature_logger.init(self)
        else:
            # reset feature logger in case init_serving is called again without feature logger
            self._feature_logger = None

    @staticmethod
    def _sort_transformation_functions(
        transformation_functions: List[TransformationFunction],
    ) -> List[TransformationFunction]:
        """
        Function that sorts transformation functions in the order of the output column names.

        The list of transformation functions are sorted based on the output columns names to maintain consistent ordering.

        # Arguments
            transformation_functions:  `List[TransformationFunction]`. List of transformation functions to be sorted

        # Returns
            `List[TransformationFunction]`: List of transformation functions to be sorted
        """
        return sorted(transformation_functions, key=lambda x: x.output_column_names[0])

    def init_batch_scoring(
        self,
        training_dataset_version: Optional[int] = None,
    ) -> None:
        """Initialise feature view to retrieve feature vector from offline feature store.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # initialise feature view to retrieve feature vector from offline feature store
            feature_view.init_batch_scoring(training_dataset_version=1)

            # get batch data
            batch_data = feature_view.get_batch_data(...)
            ```

        # Arguments
            training_dataset_version: int, optional. Default to be None. Transformation statistics
                are fetched from training dataset and applied to the feature vector.
        """
        self._serving_training_dataset_version = training_dataset_version
        self._batch_scoring_server.init_batch_scoring(
            self, training_dataset_version=training_dataset_version
        )

    def get_batch_query(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = None,
        end_time: Optional[Union[str, int, datetime, date]] = None,
    ) -> str:
        """Get a query string of the batch query.

        !!! example "Batch query for the last 24 hours"
            ```python
                # get feature store instance
                fs = ...

                # get feature view instance
                feature_view = fs.get_feature_view(...)

                # set up dates
                import datetime
                start_date = (datetime.datetime.now() - datetime.timedelta(hours=24))
                end_date = (datetime.datetime.now())

                # get a query string of batch query
                query_str = feature_view.get_batch_query(
                    start_time=start_date,
                    end_time=end_date
                )
                # print query string
                print(query_str)
            ```

        # Arguments
            start_time: Start event time for the batch query, inclusive. Optional. Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            end_time: End event time for the batch query, exclusive. Optional. Strings should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`,
                `%Y-%m-%d %H:%M:%S`, or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.

        # Returns
            `str`: batch query
        """
        return self._feature_view_engine.get_batch_query_string(
            self,
            start_time,
            end_time,
            training_dataset_version=self._batch_scoring_server.training_dataset_version
            if self._batch_scoring_server
            else None,
        )

    def get_feature_vector(
        self,
        entry: Optional[Dict[str, Any]] = None,
        passed_features: Optional[Dict[str, Any]] = None,
        external: Optional[bool] = None,
        return_type: Literal["list", "polars", "numpy", "pandas"] = "list",
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
        transform: Optional[bool] = True,
        on_demand_features: Optional[bool] = True,
        request_parameters: Optional[Dict[str, Any]] = None,
        transformation_context: Dict[str, Any] = None,
    ) -> Union[List[Any], pd.DataFrame, np.ndarray, pl.DataFrame]:
        """Returns assembled feature vector from online feature store.
            Call [`feature_view.init_serving`](#init_serving) before this method if the following configurations are needed.
              1. The training dataset version of the transformation statistics
              2. Additional configurations of online serving engine
        !!! warning "Missing primary key entries"
            If the provided primary key `entry` can't be found in one or more of the feature groups
            used by this feature view the call to this method will raise an exception.
            Alternatively, setting `allow_missing` to `True` returns a feature vector with missing values.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get assembled serving vector as a python list
            feature_view.get_feature_vector(
                entry = {"pk1": 1, "pk2": 2}
            )

            # get assembled serving vector as a pandas dataframe
            feature_view.get_feature_vector(
                entry = {"pk1": 1, "pk2": 2},
                return_type = "pandas"
            )

            # get assembled serving vector as a numpy array
            feature_view.get_feature_vector(
                entry = {"pk1": 1, "pk2": 2},
                return_type = "numpy"
            )
            ```

        !!! example "Get feature vector with user-supplied features"
            ```python
            # get feature store instance
            fs = ...
            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # the application provides a feature value 'app_attr'
            app_attr = ...

            # get a feature vector
            feature_view.get_feature_vector(
                entry = {"pk1": 1, "pk2": 2},
                passed_features = { "app_feature" : app_attr }
            )
            ```

        # Arguments
            entry: dictionary of feature group primary key and values provided by serving application.
                Set of required primary keys is [`feature_view.primary_keys`](#primary_keys)
                If the required primary keys is not provided, it will look for name
                of the primary key in feature group in the entry.
            passed_features: dictionary of feature values provided by the application at runtime.
                They can replace features values fetched from the feature store as well as
                providing feature values which are not available in the feature store.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
            return_type: `"list"`, `"pandas"`, `"polars"` or `"numpy"`. Defaults to `"list"`.
            force_rest_client: boolean, defaults to False. If set to True, reads from online feature store
                using the REST client if initialised.
            force_sql_client: boolean, defaults to False. If set to True, reads from online feature store
                using the SQL client if initialised.
            allow_missing: Setting to `True` returns feature vectors with missing values.
            transform: `bool`, default=`True`. If set to `True`, model-dependent transformations are applied to the feature vector, and `on_demand_feature` is automatically set to `True`, ensuring the inclusion of on-demand features.
                If set to `False`, the function returns the feature vector without applying any model-dependent transformations.
            on_demand_features: `bool`, defaults to `True`. Setting this to `False` returns untransformed featuretransform: `bool`, default=`True`. vectors without any on-demand features.
            request_parameters: Request parameters required by on-demand transformation functions to compute on-demand features present in the feature view.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.

        # Returns
            `list`, `pd.DataFrame`, `polars.DataFrame` or `np.ndarray` if `return type` is set to `"list"`, `"pandas"`, `"polars"` or `"numpy"`
            respectively. Defaults to `list`.
            Returned `list`, `pd.DataFrame`, `polars.DataFrame` or `np.ndarray` contains feature values related to provided primary keys,
            ordered according to positions of this features in the feature view query.

        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: When primary key entry cannot be found in one or more of the feature groups used by this
                feature view.
        """
        if not self._vector_server._serving_initialized:
            self.init_serving(external=external)

        vector_db_features = None
        if self._vector_db_client:
            vector_db_features = self._get_vector_db_result(entry)
        return self._vector_server.get_feature_vector(
            entry=entry,
            return_type=return_type,
            passed_features=passed_features,
            allow_missing=allow_missing,
            vector_db_features=vector_db_features,
            force_rest_client=force_rest_client,
            force_sql_client=force_sql_client,
            transform=transform,
            on_demand_features=on_demand_features,
            request_parameters=request_parameters,
            transformation_context=transformation_context,
        )

    def get_feature_vectors(
        self,
        entry: Optional[List[Dict[str, Any]]] = None,
        passed_features: Optional[List[Dict[str, Any]]] = None,
        external: Optional[bool] = None,
        return_type: Literal["list", "polars", "numpy", "pandas"] = "list",
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
        transform: Optional[bool] = True,
        on_demand_features: Optional[bool] = True,
        request_parameters: Optional[List[Dict[str, Any]]] = None,
        transformation_context: Dict[str, Any] = None,
    ) -> Union[List[List[Any]], pd.DataFrame, np.ndarray, pl.DataFrame]:
        """Returns assembled feature vectors in batches from online feature store.
            Call [`feature_view.init_serving`](#init_serving) before this method if the following configurations are needed.
              1. The training dataset version of the transformation statistics
              2. Additional configurations of online serving engine
        !!! warning "Missing primary key entries"
            If any of the provided primary key elements in `entry` can't be found in any
            of the feature groups, no feature vector for that primary key value will be
            returned.
            If it can be found in at least one but not all feature groups used by
            this feature view the call to this method will raise an exception.
            Alternatively, setting `allow_missing` to `True` returns feature vectors with missing values.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get assembled serving vectors as a python list of lists
            feature_view.get_feature_vectors(
                entry = [
                    {"pk1": 1, "pk2": 2},
                    {"pk1": 3, "pk2": 4},
                    {"pk1": 5, "pk2": 6}
                ]
            )

            # get assembled serving vectors as a pandas dataframe
            feature_view.get_feature_vectors(
                entry = [
                    {"pk1": 1, "pk2": 2},
                    {"pk1": 3, "pk2": 4},
                    {"pk1": 5, "pk2": 6}
                ],
                return_type = "pandas"
            )

            # get assembled serving vectors as a numpy array
            feature_view.get_feature_vectors(
                entry = [
                    {"pk1": 1, "pk2": 2},
                    {"pk1": 3, "pk2": 4},
                    {"pk1": 5, "pk2": 6}
                ],
                return_type = "numpy"
            )
            ```

        # Arguments
            entry: a list of dictionary of feature group primary key and values provided by serving application.
                Set of required primary keys is [`feature_view.primary_keys`](#primary_keys)
                If the required primary keys is not provided, it will look for name
                of the primary key in feature group in the entry.
            passed_features: a list of dictionary of feature values provided by the application at runtime.
                They can replace features values fetched from the feature store as well as
                providing feature values which are not available in the feature store.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
            return_type: `"list"`, `"pandas"`, `"polars"` or `"numpy"`. Defaults to `"list"`.
            force_sql_client: boolean, defaults to False. If set to True, reads from online feature store
                using the SQL client if initialised.
            force_rest_client: boolean, defaults to False. If set to True, reads from online feature store
                using the REST client if initialised.
            allow_missing: Setting to `True` returns feature vectors with missing values.
            transform: `bool`, default=`True`. If set to `True`, model-dependent transformations are applied to the feature vector, and `on_demand_feature` is automatically set to `True`, ensuring the inclusion of on-demand features.
                If set to `False`, the function returns the feature vector without applying any model-dependent transformations.
            on_demand_features: `bool`, defaults to `True`. Setting this to `False` returns untransformed feature vectors without any on-demand features.
            request_parameters: Request parameters required by on-demand transformation functions to compute on-demand features present in the feature view.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.

        # Returns
            `List[list]`, `pd.DataFrame`, `polars.DataFrame` or `np.ndarray` if `return type` is set to `"list", `"pandas"`,`"polars"` or `"numpy"`
            respectively. Defaults to `List[list]`.

            Returned `List[list]`, `pd.DataFrame`, `polars.DataFrame` or `np.ndarray` contains feature values related to provided primary
            keys, ordered according to positions of this features in the feature view query.

        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: When primary key entry cannot be found in one or more of the feature groups used by this
                feature view.
        """
        if not self._vector_server._serving_initialized:
            self.init_serving(external=external, init_rest_client=force_rest_client)

        vector_db_features = []
        if self._vector_db_client:
            for _entry in entry:
                vector_db_features.append(self._get_vector_db_result(_entry))

        return self._vector_server.get_feature_vectors(
            entries=entry,
            return_type=return_type,
            passed_features=passed_features,
            allow_missing=allow_missing,
            vector_db_features=vector_db_features,
            force_rest_client=force_rest_client,
            force_sql_client=force_sql_client,
            transform=transform,
            on_demand_features=on_demand_features,
            request_parameters=request_parameters,
            transformation_context=transformation_context,
        )

    def get_inference_helper(
        self,
        entry: Dict[str, Any],
        external: Optional[bool] = None,
        return_type: Literal["pandas", "dict", "polars"] = "pandas",
        force_rest_client: bool = False,
        force_sql_client: bool = False,
    ) -> Union[pd.DataFrame, pl.DataFrame, Dict[str, Any]]:
        """Returns assembled inference helper column vectors from online feature store.
        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get assembled inference helper column vector
            feature_view.get_inference_helper(
                entry = {"pk1": 1, "pk2": 2}
            )
            ```

        # Arguments
            entry: dictionary of feature group primary key and values provided by serving application.
                Set of required primary keys is [`feature_view.primary_keys`](#primary_keys)
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
            return_type: `"pandas"`, `"polars"` or `"dict"`. Defaults to `"pandas"`.

        # Returns
            `pd.DataFrame`, `polars.DataFrame` or `dict`. Defaults to `pd.DataFrame`.

        # Raises
            `Exception`: When primary key entry cannot be found in one or more of the feature groups used by this
                feature view.
        """
        if not self._vector_server._serving_initialized:
            self.init_serving(external=external, init_rest_client=force_rest_client)
        return self._vector_server.get_inference_helper(
            entry, return_type, force_rest_client, force_sql_client
        )

    def get_inference_helpers(
        self,
        entry: List[Dict[str, Any]],
        external: Optional[bool] = None,
        return_type: Literal["pandas", "dict", "polars"] = "pandas",
        force_sql_client: bool = False,
        force_rest_client: bool = False,
    ) -> Union[List[Dict[str, Any]], pd.DataFrame, pl.DataFrame]:
        """Returns assembled inference helper column vectors in batches from online feature store.
        !!! warning "Missing primary key entries"
            If any of the provided primary key elements in `entry` can't be found in any
            of the feature groups, no inference helper column vectors for that primary key value will be
            returned.
            If it can be found in at least one but not all feature groups used by
            this feature view the call to this method will raise an exception.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get assembled inference helper column vectors
            feature_view.get_inference_helpers(
                entry = [
                    {"pk1": 1, "pk2": 2},
                    {"pk1": 3, "pk2": 4},
                    {"pk1": 5, "pk2": 6}
                ]
            )
            ```

        # Arguments
            entry: a list of dictionary of feature group primary key and values provided by serving application.
                Set of required primary keys is [`feature_view.primary_keys`](#primary_keys)
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
            return_type: `"pandas"`, `"polars"` or `"dict"`. Defaults to `"pandas"`.

        # Returns
            `pd.DataFrame`, `polars.DataFrame` or `List[Dict[str, Any]]`.  Defaults to `pd.DataFrame`.

            Returned `pd.DataFrame` or `List[dict]`  contains feature values related to provided primary
            keys, ordered according to positions of this features in the feature view query.

        # Raises
            `Exception`: When primary key entry cannot be found in one or more of the feature groups used by this
                feature view.
        """
        if self._vector_server is None:
            self.init_serving(external=external, init_rest_client=force_rest_client)
        return self._vector_server.get_inference_helpers(
            self, entry, return_type, force_rest_client, force_sql_client
        )

    def _get_vector_db_result(
        self,
        entry: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if not self._vector_db_client:
            return {}
        result_vectors = {}
        for join_index, fg in self._vector_db_client.embedding_fg_by_join_index.items():
            complete, fg_entry = self._vector_db_client.filter_entry_by_join_index(
                entry, join_index
            )
            if not complete:
                # Not retrieving from vector db if entry is not completed
                continue
            vector_db_features = self._vector_db_client.read(
                fg.id,
                fg.features,
                keys=fg_entry,
                index_name=fg.embedding_index.index_name,
            )

            # if result is not empty
            if vector_db_features:
                vector_db_features = vector_db_features[0]  # get the first result
                result_vectors.update(vector_db_features)
        return result_vectors

    def find_neighbors(
        self,
        embedding: List[Union[int, float]],
        feature: Optional[Feature] = None,
        k: Optional[int] = 10,
        filter: Optional[Union[Filter, Logic]] = None,
        external: Optional[bool] = None,
        return_type: Literal["list", "polars", "pandas"] = "list",
    ) -> List[List[Any]]:
        """
        Finds the nearest neighbors for a given embedding in the vector database.

        If `filter` is specified, or if embedding feature is stored in default project index,
        the number of results returned may be less than k. Try using a large value of k and extract the top k
        items from the results if needed.

        !!! warning "Duplicate column error in Polars"
            If the feature view has duplicate column names, attempting to create a polars DataFrame
            will raise an error. To avoid this, set `return_type` to `"list"` or `"pandas"`.

        # Arguments
            embedding: The target embedding for which neighbors are to be found.
            feature: The feature used to compute similarity score. Required only if there
            are multiple embeddings (optional).
            k: The number of nearest neighbors to retrieve (default is 10).
            filter: A filter expression to restrict the search space (optional).
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
            return_type: `"list"`, `"pandas"` or `"polars"`. Defaults to `"list"`.

        # Returns
            `list`, `pd.DataFrame` or `polars.DataFrame` if `return type` is set to `"list"`, `"pandas"` or
            `"polars"` respectively. Defaults to `list`.

        !!! Example
            ```
            embedding_index = EmbeddingIndex()
            embedding_index.add_embedding(name="user_vector", dimension=3)
            fg = fs.create_feature_group(
                        name='air_quality',
                        embedding_index=embedding_index,
                        version=1,
                        primary_key=['id1'],
                        online_enabled=True,
                    )
            fg.insert(data)
            fv = fs.create_feature_view("air_quality", fg.select_all())
            fv.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
            )

            # apply filter
            fg.find_neighbors(
                [0.1, 0.2, 0.3],
                k=5,
                feature=fg.user_vector,  # optional
                filter=(fg.id1 > 10) & (fg.id1 < 30)
            )
            ```
        """
        if self._vector_db_client is None:
            self.init_serving(external=external)
        results = self._vector_db_client.find_neighbors(
            embedding,
            feature=(feature if feature else None),
            k=k,
            filter=filter,
        )
        if len(results) == 0:
            return []

        return self._vector_server.get_feature_vectors(
            [self._extract_primary_key(res[1]) for res in results],
            return_type=return_type,
            vector_db_features=[res[1] for res in results],
            allow_missing=True,
        )

    def _extract_primary_key(self, result_key: Dict[str, str]) -> Dict[str, str]:
        primary_key_map = {}
        for prefix_sk, sk in self._prefix_serving_key_map.items():
            if prefix_sk in result_key:
                primary_key_map[sk.required_serving_key] = result_key[prefix_sk]
            elif sk.feature_name in result_key:  # fall back to use raw feature name
                primary_key_map[sk.required_serving_key] = result_key[sk.feature_name]
        if len(set(self._vector_server.required_serving_keys)) > len(primary_key_map):
            raise FeatureStoreException(
                f"Failed to get feature vector because required primary key [{', '.join([k for k in set([sk.required_serving_key for sk in self._prefix_serving_key_map.values()]) - primary_key_map.keys()])}] are not present in vector db."
                "If the join of the embedding feature group in the query does not have a prefix,"
                " try to create a new feature view with prefix attached."
            )
        return primary_key_map

    def _get_embedding_fgs(
        self,
    ) -> Set[feature_group.FeatureGroup]:
        return set([fg for fg in self.query.featuregroups if fg.embedding_index])

    @usage.method_logger
    def get_batch_data(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = None,
        end_time: Optional[Union[str, int, datetime, date]] = None,
        read_options: Optional[Dict[str, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        primary_key: bool = False,
        event_time: bool = False,
        inference_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        transformed: Optional[bool] = True,
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> TrainingDatasetDataFrameTypes:
        """Get a batch of data from an event time interval from the offline feature store.

        !!! example "Batch data for the last 24 hours"
            ```python
                # get feature store instance
                fs = ...

                # get feature view instance
                feature_view = fs.get_feature_view(...)

                # set up dates
                import datetime
                start_date = (datetime.datetime.now() - datetime.timedelta(hours=24))
                end_date = (datetime.datetime.now())

                # get a batch of data
                df = feature_view.get_batch_data(
                    start_time=start_date,
                    end_time=end_date
                )
            ```

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            start_time: Start event time for the batch query, inclusive. Optional. Strings should be
                formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            end_time: End event time for the batch query, exclusive. Optional. Strings should be
                formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            read_options: User provided read options for python engine, defaults to `{}`:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            inference_helper_columns: whether to include inference helper columns or not.
                Inference helper columns are a list of feature names in the feature view, defined during its creation,
                that may not be used in training the model itself but can be used during batch or online inference
                for extra information. If inference helper columns were not defined in the feature view
                `inference_helper_columns=True` will not any effect. Defaults to `False`, no helper columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            transformed: Setting to `False` returns the untransformed feature vectors.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.

        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`: A Spark DataFrame.
            `pandas.DataFrame`: A Pandas DataFrame.
            `polars.DataFrame`: A Polars DataFrame.
            `numpy.ndarray`: A two-dimensional Numpy array.
            `list`: A two-dimensional Python list.
        """
        if not self._batch_scoring_server._serving_initialized:
            self.init_batch_scoring()

        return self._feature_view_engine.get_batch_data(
            self,
            start_time,
            end_time,
            self._batch_scoring_server.training_dataset_version,
            self._batch_scoring_server._model_dependent_transformation_functions,
            read_options,
            spine,
            kwargs.get("primary_keys") or primary_key,
            event_time,
            inference_helper_columns,
            dataframe_type,
            transformed=transformed,
            transformation_context=transformation_context,
        )

    def add_tag(self, name: str, value: Any) -> None:
        """Attach a tag to a feature view.

        A tag consists of a name and value pair.
        Tag names are unique identifiers across the whole cluster.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # attach a tag to a feature view
            feature_view.add_tag(name="tag_schema", value={"key", "value"})
            ```

        # Arguments
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.add_tag(self, name, value)

    def get_tag(self, name: str) -> Optional[tag.Tag]:
        """Get the tags of a feature view.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get a tag of a feature view
            name = feature_view.get_tag('tag_name')
            ```

        # Arguments
            name: Name of the tag to get.

        # Returns
            tag value or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_tag(self, name)

    def get_tags(self) -> Dict[str, tag.Tag]:
        """Returns all tags attached to a feature view.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get tags
            list_tags = feature_view.get_tags()
            ```

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_tags(self)

    def get_parent_feature_groups(self) -> Optional["explicit_provenance.Links"]:
        """Get the parents of this feature view, based on explicit provenance.
        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only minimal information is
        returned.

        # Returns
            `Links`: Object containing the section of provenance graph requested or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_parent_feature_groups(self)

    def get_newest_model(
        self, training_dataset_version: Optional[int] = None
    ) -> Optional[Any]:
        """Get the latest generated model using this feature view, based on explicit
        provenance. Search only through the accessible models.
        For more items use the base method - get_models_provenance

        # Arguments
            training_dataset_version: Filter generated models based on the used training dataset version.

        # Returns
            `Model`: Newest Generated Model or `None` if it does not exist.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        models = self.get_models(training_dataset_version=training_dataset_version)
        models.sort(key=lambda model: model.created, reverse=True)
        if models:
            return models[0]
        else:
            return None

    def get_models(self, training_dataset_version: Optional[int] = None) -> List[Any]:
        """Get the generated models using this feature view, based on explicit
        provenance. Only the accessible models are returned.
        For more items use the base method - get_models_provenance

        # Arguments
            training_dataset_version: Filter generated models based on the used training dataset version.
        # Returns
            `List[Model]`: List of models.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        models = self.get_models_provenance(
            training_dataset_version=training_dataset_version
        )
        if models and models.accessible:
            return models.accessible
        else:
            return []

    def get_models_provenance(
        self, training_dataset_version: Optional[int] = None
    ) -> "explicit_provenance.Links":
        """Get the generated models using this feature view, based on explicit
        provenance. These models can be accessible or inaccessible. Explicit
        provenance does not track deleted generated model links, so deleted
        will always be empty.
        For inaccessible models, only a minimal information is returned.

        # Arguments
            training_dataset_version: Filter generated models based on the used training dataset version.

        # Returns
            `Links`: Object containing the section of provenance graph requested or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_models_provenance(
            self, training_dataset_version=training_dataset_version
        )

    def delete_tag(self, name: str) -> None:
        """Delete a tag attached to a feature view.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # delete a tag
            feature_view.delete_tag('name_of_tag')
            ```

        # Arguments
            name: Name of the tag to be removed.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.delete_tag(self, name)

    def update_last_accessed_training_dataset(self, version):
        if self._last_accessed_training_dataset is not None:
            _logger.info(
                f"Provenance cached data - overwriting last accessed/created training dataset from {self._last_accessed_training_dataset} to {version}."
            )
        self._last_accessed_training_dataset = version

    @usage.method_logger
    def create_training_data(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = "",
        end_time: Optional[Union[str, int, datetime, date]] = "",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        data_format: Optional[str] = "parquet",
        coalesce: Optional[bool] = False,
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[int, job.Job]:
        """Create the metadata for a training dataset and save the corresponding training data into `location`.
        The training data can be retrieved by calling `feature_view.get_training_data`.

        !!! example "Create training dataset"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # create a training dataset
            version, job = feature_view.create_training_data(
                description='Description of a dataset',
                data_format='csv',
                # async creation in order not to wait till finish of the job
                write_options={"wait_for_job": False}
            )
            ```

        !!! example "Create training data specifying date range  with dates as strings"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            start_time = "2022-01-01 00:00:00"
            end_time = "2022-06-06 23:59:59"

            # create a training dataset
            version, job = feature_view.create_training_data(
                start_time=start_time,
                end_time=end_time,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )

            # When we want to read the training data, we need to supply the training data version returned by the create_training_data method:
            X_train, X_test, y_train, y_test = feature_view.get_training_data(version)
            ```

        !!! example "Create training data specifying date range  with dates as datetime objects"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            from datetime import datetime
            date_format = "%Y-%m-%d %H:%M:%S"

            start_time = datetime.strptime("2022-01-01 00:00:00", date_format)
            end_time = datetime.strptime("2022-06-06 23:59:59", date_format)

            # create a training dataset
            version, job = feature_view.create_training_data(
                start_time=start_time,
                end_time=end_time,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )
            ```

        !!! example "Write training dataset to external storage"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get storage connector instance
            external_storage_connector = fs.get_storage_connector("storage_connector_name")

            # create a train-test split dataset
            version, job = feature_view.create_training_data(
                start_time=...,
                end_time=...,
                storage_connector = external_storage_connector,
                description=...,
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format=...
            )
            ```

        !!! info "Data Formats"
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. tsv
            4. parquet
            5. avro
            6. orc
            7. json

            Currently not supported petastorm, hdf5 and npy file formats.

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            start_time: Start event time for the training dataset query, inclusive. Optional. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            end_time: End event time for the training dataset query, exclusive. Optional. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            storage_connector: Storage connector defining the sink location for the
                training dataset, defaults to `None`, and materializes training dataset
                on HopsFS.
            location: Path to complement the sink storage connector with, e.g if the
                storage connector points to an S3 bucket, this path can be used to
                define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the
                storage connector.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            extra_filter: Additional filters to be attached to the training dataset.
                The filters will be also applied in `get_batch_data`.
            data_format: The data format used to save the training dataset,
                defaults to `"parquet"`-format.
            coalesce: If true the training dataset data will be coalesced into
                a single partition before writing. The resulting training dataset
                will be a single file per split. Default False.
            seed: Optionally, define a seed to create the random splits with, in order
                to guarantee reproducability, defaults to `None`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            write_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `use_spark` and value `True` to materialize training dataset
                  with Spark instead of [Hopsworks Feature Query Service](https://docs.hopsworks.ai/latest/setup_installation/common/arrow_flight_duckdb/).
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (td_version, `Job`): Tuple of training dataset version and job.
                When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=None,
            event_start_time=start_time,
            event_end_time=end_time,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._featurestore_id,
            splits={},
            seed=seed,
            statistics_config=statistics_config,
            coalesce=coalesce,
            extra_filter=extra_filter,
        )
        # td_job is used only if the python engine is used
        td, td_job = self._feature_view_engine.create_training_dataset(
            self,
            td,
            write_options or {},
            spine=spine,
            transformation_context=transformation_context,
        )
        warnings.warn(
            "Incremented version to `{}`.".format(td.version),
            util.VersionWarning,
            stacklevel=1,
        )
        self.update_last_accessed_training_dataset(td.version)

        return td.version, td_job

    @usage.method_logger
    def create_train_test_split(
        self,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        data_format: Optional[str] = "parquet",
        coalesce: Optional[bool] = False,
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[int, job.Job]:
        """Create the metadata for a training dataset and save the corresponding training data into `location`.
        The training data is split into train and test set at random or according to time ranges.
        The training data can be retrieved by calling `feature_view.get_train_test_split`.

        !!! example "Create random splits"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # create a train-test split dataset
            version, job = feature_view.create_train_test_split(
                test_size=0.2,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )
            ```

        !!! example "Create time series splits by specifying date as string"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            train_start = "2022-01-01 00:00:00"
            train_end = "2022-06-06 23:59:59"
            test_start = "2022-06-07 00:00:00"
            test_end = "2022-12-25 23:59:59"

            # create a train-test split dataset
            version, job = feature_view.create_train_test_split(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )
            ```

        !!! example "Create time series splits by specifying date as datetime object"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            from datetime import datetime
            date_format = "%Y-%m-%d %H:%M:%S"

            train_start = datetime.strptime("2022-01-01 00:00:00", date_format)
            train_end = datetime.strptime("2022-06-06 23:59:59", date_format)
            test_start = datetime.strptime("2022-06-07 00:00:00", date_format)
            test_end = datetime.strptime("2022-12-25 23:59:59" , date_format)

            # create a train-test split dataset
            version, job = feature_view.create_train_test_split(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )
            ```

        !!! example "Write training dataset to external storage"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get storage connector instance
            external_storage_connector = fs.get_storage_connector("storage_connector_name")

            # create a train-test split dataset
            version, job = feature_view.create_train_test_split(
                train_start=...,
                train_end=...,
                test_start=...,
                test_end=...,
                storage_connector = external_storage_connector,
                description=...,
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format=...
            )
            ```

        !!! info "Data Formats"
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. tsv
            4. parquet
            5. avro
            6. orc
            7. json

            Currently not supported petastorm, hdf5 and npy file formats.

        !!! warning "Warning, the following code will fail because category column contains sparse values and training dataset may not have all values available in test split."
            ```python
            import pandas as pd

            df = pd.DataFrame({
                'category_col':['category_a','category_b','category_c','category_d'],
                'numeric_col': [40,10,60,40]
            })

            feature_group = fs.get_or_create_feature_group(
                name='feature_group_name',
                version=1,
                primary_key=['category_col']
            )

            feature_group.insert(df)

            label_encoder = fs.get_transformation_function(name='label_encoder')

            feature_view = fs.create_feature_view(
                name='feature_view_name',
                query=feature_group.select_all(),
                transformation_functions={'category_col':label_encoder}
            )

            feature_view.create_train_test_split(
                test_size=0.5
            )
            # Output: KeyError: 'category_c'
            ```

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            test_size: size of test set.
            train_start: Start event time for the train split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            train_end: End event time for the train split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_start: Start event time for the test split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_end: End event time for the test split query, exclusive. Strings should
                be  formatted in one of the following ormats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            storage_connector: Storage connector defining the sink location for the
                training dataset, defaults to `None`, and materializes training dataset
                on HopsFS.
            location: Path to complement the sink storage connector with, e.g if the
                storage connector points to an S3 bucket, this path can be used to
                define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the
                storage connector.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            extra_filter: Additional filters to be attached to the training dataset.
                The filters will be also applied in `get_batch_data`.
            data_format: The data format used to save the training dataset,
                defaults to `"parquet"`-format.
            coalesce: If true the training dataset data will be coalesced into
                a single partition before writing. The resulting training dataset
                will be a single file per split. Default False.
            seed: Optionally, define a seed to create the random splits with, in order
                to guarantee reproducability, defaults to `None`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            write_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `use_spark` and value `True` to materialize training dataset
                  with Spark instead of [Hopsworks Feature Query Service](https://docs.hopsworks.ai/latest/setup_installation/common/arrow_flight_duckdb/).
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (td_version, `Job`): Tuple of training dataset version and job.
                When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        self._validate_train_test_split(
            test_size=test_size, train_end=train_end, test_start=test_start
        )
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=None,
            test_size=test_size,
            time_split_size=2,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._featurestore_id,
            splits={},
            seed=seed,
            statistics_config=statistics_config,
            coalesce=coalesce,
            extra_filter=extra_filter,
        )
        # td_job is used only if the python engine is used
        td, td_job = self._feature_view_engine.create_training_dataset(
            self,
            td,
            write_options or {},
            spine=spine,
            transformation_context=transformation_context,
        )
        warnings.warn(
            "Incremented version to `{}`.".format(td.version),
            util.VersionWarning,
            stacklevel=1,
        )
        self.update_last_accessed_training_dataset(td.version)
        return td.version, td_job

    @usage.method_logger
    def create_train_validation_test_split(
        self,
        validation_size: Optional[float] = None,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        validation_start: Optional[Union[str, int, datetime, date]] = "",
        validation_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        storage_connector: Optional[storage_connector.StorageConnector] = None,
        location: Optional[str] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        data_format: Optional[str] = "parquet",
        coalesce: Optional[bool] = False,
        seed: Optional[int] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[int, job.Job]:
        """Create the metadata for a training dataset and save the corresponding training data into `location`.
        The training data is split into train, validation, and test set at random or according to time range.
        The training data can be retrieved by calling `feature_view.get_train_validation_test_split`.

        !!! example "Create random splits"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # create a train-validation-test split dataset
            version, job = feature_view.create_train_validation_test_split(
                validation_size=0.3,
                test_size=0.2,
                description='Description of a dataset',
                data_format='csv'
            )
            ```

        !!! example "Create time series splits by specifying date as string"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            train_start = "2022-01-01 00:00:00"
            train_end = "2022-06-01 23:59:59"
            validation_start = "2022-06-02 00:00:00"
            validation_end = "2022-07-01 23:59:59"
            test_start = "2022-07-02 00:00:00"
            test_end = "2022-08-01 23:59:59"

            # create a train-validation-test split dataset
            version, job = feature_view.create_train_validation_test_split(
                train_start=train_start,
                train_end=train_end,
                validation_start=validation_start,
                validation_end=validation_end,
                test_start=test_start,
                test_end=test_end,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )
            ```

        !!! example "Create time series splits by specifying date as datetime object"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            from datetime import datetime
            date_format = "%Y-%m-%d %H:%M:%S"

            train_start = datetime.strptime("2022-01-01 00:00:00", date_format)
            train_end = datetime.strptime("2022-06-06 23:59:59", date_format)
            validation_start = datetime.strptime("2022-06-02 00:00:00", date_format)
            validation_end = datetime.strptime("2022-07-01 23:59:59", date_format)
            test_start = datetime.strptime("2022-06-07 00:00:00", date_format)
            test_end = datetime.strptime("2022-12-25 23:59:59", date_format)

            # create a train-validation-test split dataset
            version, job = feature_view.create_train_validation_test_split(
                train_start=train_start,
                train_end=train_end,
                validation_start=validation_start,
                validation_end=validation_end,
                test_start=test_start,
                test_end=test_end,
                description='Description of a dataset',
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format='csv'
            )
            ```

        !!! example "Write training dataset to external storage"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get storage connector instance
            external_storage_connector = fs.get_storage_connector("storage_connector_name")

            # create a train-validation-test split dataset
            version, job = feature_view.create_train_validation_test_split(
                train_start=...,
                train_end=...,
                validation_start=...,
                validation_end=...,
                test_start=...,
                test_end=...,
                description=...,
                storage_connector = external_storage_connector,
                # you can have different data formats such as csv, tsv, tfrecord, parquet and others
                data_format=...
            )
            ```

        !!! info "Data Formats"
            The feature store currently supports the following data formats for
            training datasets:

            1. tfrecord
            2. csv
            3. tsv
            4. parquet
            5. avro
            6. orc
            7. json

            Currently not supported petastorm, hdf5 and npy file formats.

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            validation_size: size of validation set.
            test_size: size of test set.
            train_start: Start event time for the train split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            train_end: End event time for the train split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            validation_start: Start event time for the validation split query, inclusive. Strings
                should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            validation_end: End event time for the validation split query, exclusive. Strings
                should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_start: Start event time for the test split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_end: End event time for the test split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            storage_connector: Storage connector defining the sink location for the
                training dataset, defaults to `None`, and materializes training dataset
                on HopsFS.
            location: Path to complement the sink storage connector with, e.g if the
                storage connector points to an S3 bucket, this path can be used to
                define a sub-directory inside the bucket to place the training dataset.
                Defaults to `""`, saving the training dataset at the root defined by the
                storage connector.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            extra_filter: Additional filters to be attached to the training dataset.
                The filters will be also applied in `get_batch_data`.
            data_format: The data format used to save the training dataset,
                defaults to `"parquet"`-format.
            coalesce: If true the training dataset data will be coalesced into
                a single partition before writing. The resulting training dataset
                will be a single file per split. Default False.
            seed: Optionally, define a seed to create the random splits with, in order
                to guarantee reproducability, defaults to `None`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            write_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `use_spark` and value `True` to materialize training dataset
                  with Spark instead of [Hopsworks Feature Query Service](https://docs.hopsworks.ai/latest/setup_installation/common/arrow_flight_duckdb/).
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (td_version, `Job`): Tuple of training dataset version and job.
                When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """

        self._validate_train_validation_test_split(
            validation_size=validation_size,
            test_size=test_size,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            test_start=test_start,
        )
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=None,
            validation_size=validation_size,
            test_size=test_size,
            time_split_size=3,
            train_start=train_start,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            test_start=test_start,
            test_end=test_end,
            description=description,
            data_format=data_format,
            storage_connector=storage_connector,
            location=location,
            featurestore_id=self._featurestore_id,
            splits={},
            seed=seed,
            statistics_config=statistics_config,
            coalesce=coalesce,
            extra_filter=extra_filter,
        )
        # td_job is used only if the python engine is used
        td, td_job = self._feature_view_engine.create_training_dataset(
            self,
            td,
            write_options or {},
            spine=spine,
            transformation_context=transformation_context,
        )
        warnings.warn(
            "Incremented version to `{}`.".format(td.version),
            util.VersionWarning,
            stacklevel=1,
        )
        self.update_last_accessed_training_dataset(td.version)

        return td.version, td_job

    @usage.method_logger
    def recreate_training_dataset(
        self,
        training_dataset_version: int,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        write_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        transformation_context: Dict[str, Any] = None,
    ) -> job.Job:
        """
        Recreate a training dataset.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # recreate a training dataset that has been deleted
            feature_view.recreate_training_dataset(training_dataset_version=1)
            ```

        !!! info
            If a materialised training data has deleted. Use `recreate_training_dataset()` to
            recreate the training data.

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            training_dataset_version: training dataset version
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            write_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, write_options can contain the
                following entries:
                * key `use_spark` and value `True` to materialize training dataset
                  with Spark instead of [Hopsworks Feature Query Service](https://docs.hopsworks.ai/latest/setup_installation/common/arrow_flight_duckdb/).
                * key `spark` and value an object of type
                [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                * key `wait_for_job` and value `True` or `False` to configure
                  whether or not to the save call should return only
                  after the Hopsworks Job has finished. By default it waits.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.

        # Returns
            `Job`: When using the `python` engine, it returns the Hopsworks Job
                that was launched to create the training dataset.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        td, td_job = self._feature_view_engine.recreate_training_dataset(
            self,
            training_dataset_version=training_dataset_version,
            statistics_config=statistics_config,
            user_write_options=write_options or {},
            spine=spine,
            transformation_context=transformation_context,
        )
        self.update_last_accessed_training_dataset(td.version)

        return td_job

    @usage.method_logger
    def training_data(
        self,
        start_time: Optional[Union[str, int, datetime, date]] = None,
        end_time: Optional[Union[str, int, datetime, date]] = None,
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],  # optional label DataFrame
    ]:
        """
        Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
        This returns the training data in memory and does not materialise data in storage.
        The training data can be recreated by calling `feature_view.get_training_data` with the metadata created.

        !!! example "Create random splits"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training data
            features_df, labels_df  = feature_view.training_data(
                description='Descriprion of a dataset',
            )
            ```

        !!! example "Create time-series based splits"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up a date
            start_time = "2022-05-01 00:00:00"
            end_time = "2022-06-04 23:59:59"
            # you can also pass dates as datetime objects

            # get training data
            features_df, labels_df = feature_view.training_data(
                start_time=start_time,
                end_time=end_time,
                description='Description of a dataset'
            )
            ```

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            start_time: Start event time for the training dataset query, inclusive. Strings should
            be formatted in one of the following
                formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            end_time: End event time for the training dataset query, exclusive. Strings should be
            formatted in one of the following
                formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            extra_filter: Additional filters to be attached to the training dataset.
                The filters will be also applied in `get_batch_data`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, read_options can contain the
                following entries:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`.
                * key `spark` and value an object of type
                  [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            training_helper_columns: whether to include training helper columns or not.
                Training helper columns are a list of feature names in the feature view, defined during its creation,
                that are not the part of the model schema itself but can be used during training as a helper for
                extra information. If training helper columns were not defined in the feature view
                then`training_helper_columns=True` will not have any effect. Defaults to `False`, no training helper
                columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (X, y): Tuple of dataframe of features and labels. If there are no labels, y returns `None`.
        """
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=None,
            splits={},
            event_start_time=start_time,
            event_end_time=end_time,
            description=description,
            storage_connector=None,
            featurestore_id=self._featurestore_id,
            data_format="tsv",
            location="",
            statistics_config=statistics_config,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=extra_filter,
        )
        td, df = self._feature_view_engine.get_training_data(
            self,
            read_options,
            training_dataset_obj=td,
            spine=spine,
            primary_keys=kwargs.get("primary_keys") or primary_key,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            dataframe_type=dataframe_type,
            transformation_context=transformation_context,
        )
        warnings.warn(
            "Incremented version to `{}`.".format(td.version),
            util.VersionWarning,
            stacklevel=1,
        )
        self.update_last_accessed_training_dataset(td.version)
        return df

    @usage.method_logger
    def train_test_split(
        self,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        """
        Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
        This returns the training data in memory and does not materialise data in storage.
        The training data is split into train and test set at random or according to time ranges.
        The training data can be recreated by calling `feature_view.get_train_test_split` with the metadata created.

        !!! example "Create random train/test splits"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training data
            X_train, X_test, y_train, y_test = feature_view.train_test_split(
                test_size=0.2
            )
            ```

        !!! example "Create time-series train/test splits"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            train_start = "2022-05-01 00:00:00"
            train_end = "2022-06-04 23:59:59"
            test_start = "2022-07-01 00:00:00"
            test_end= "2022-08-04 23:59:59"
            # you can also pass dates as datetime objects

            # get training data
            X_train, X_test, y_train, y_test = feature_view.train_test_split(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                description='Description of a dataset'
            )
            ```

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            test_size: size of test set. Should be between 0 and 1.
            train_start: Start event time for the train split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`.
            train_end: End event time for the train split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_start: Start event time for the test split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_end: End event time for the test split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            extra_filter: Additional filters to be attached to the training dataset.
                The filters will be also applied in `get_batch_data`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, read_options can contain the
                following entries:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
                * key `spark` and value an object of type
                  [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            training_helper_columns: whether to include training helper columns or not.
                Training helper columns are a list of feature names in the feature view, defined during its creation,
                that are not the part of the model schema itself but can be used during training as a helper for
                extra information. If training helper columns were not defined in the feature view
                then`training_helper_columns=True` will not have any effect. Defaults to `False`, no training helper
                columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (X_train, X_test, y_train, y_test):
                Tuple of dataframe of features and labels
        """
        self._validate_train_test_split(
            test_size=test_size, train_end=train_end, test_start=test_start
        )
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=None,
            splits={},
            test_size=test_size,
            train_start=train_start,
            train_end=train_end,
            test_start=test_start,
            test_end=test_end,
            time_split_size=2,
            description=description,
            storage_connector=None,
            featurestore_id=self._featurestore_id,
            data_format="tsv",
            location="",
            statistics_config=statistics_config,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=extra_filter,
        )
        td, df = self._feature_view_engine.get_training_data(
            self,
            read_options,
            training_dataset_obj=td,
            splits=[TrainingDatasetSplit.TRAIN, TrainingDatasetSplit.TEST],
            spine=spine,
            primary_keys=kwargs.get("primary_keys") or primary_key,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            dataframe_type=dataframe_type,
            transformation_context=transformation_context,
        )
        warnings.warn(
            "Incremented version to `{}`.".format(td.version),
            util.VersionWarning,
            stacklevel=1,
        )
        self.update_last_accessed_training_dataset(td.version)
        return df

    @staticmethod
    def _validate_train_test_split(
        test_size: Optional[float],
        train_end: Optional[Union[str, int, datetime, date]],
        test_start: Optional[Union[str, int, datetime, date]],
    ) -> None:
        if not ((test_size and 0 < test_size < 1) or (train_end or test_start)):
            raise ValueError(
                "Invalid split input."
                " You should specify either `test_size` or (`train_end` or `test_start`)."
                " `test_size` should be between 0 and 1 if specified."
            )

    @usage.method_logger
    def train_validation_test_split(
        self,
        validation_size: Optional[float] = None,
        test_size: Optional[float] = None,
        train_start: Optional[Union[str, int, datetime, date]] = "",
        train_end: Optional[Union[str, int, datetime, date]] = "",
        validation_start: Optional[Union[str, int, datetime, date]] = "",
        validation_end: Optional[Union[str, int, datetime, date]] = "",
        test_start: Optional[Union[str, int, datetime, date]] = "",
        test_end: Optional[Union[str, int, datetime, date]] = "",
        description: Optional[str] = "",
        extra_filter: Optional[Union[filter.Filter, filter.Logic]] = None,
        statistics_config: Optional[Union[StatisticsConfig, bool, dict]] = None,
        read_options: Optional[Dict[Any, Any]] = None,
        spine: Optional[SplineDataFrameTypes] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        """
        Create the metadata for a training dataset and get the corresponding training data from the offline feature store.
        This returns the training data in memory and does not materialise data in storage.
        The training data is split into train, validation, and test set at random or according to time ranges.
        The training data can be recreated by calling `feature_view.get_train_validation_test_split` with the metadata created.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training data
            X_train, X_val, X_test, y_train, y_val, y_test = feature_view.train_validation_test_split(
                validation_size=0.3,
                test_size=0.2
            )
            ```

        !!! example "Time Series split"
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # set up dates
            start_time_train = '2017-01-01 00:00:01'
            end_time_train = '2018-02-01 23:59:59'

            start_time_val = '2018-02-02 23:59:59'
            end_time_val = '2019-02-01 23:59:59'

            start_time_test = '2019-02-02 23:59:59'
            end_time_test = '2020-02-01 23:59:59'
            # you can also pass dates as datetime objects

            # get training data
            X_train, X_val, X_test, y_train, y_val, y_test = feature_view.train_validation_test_split(
                train_start=start_time_train,
                train_end=end_time_train,
                validation_start=start_time_val,
                validation_end=end_time_val,
                test_start=start_time_test,
                test_end=end_time_test
            )
            ```

        !!! warning "Spine Groups/Dataframes"
            Spine groups and dataframes are currently only supported with the Spark engine and
            Spark dataframes.

        # Arguments
            validation_size: size of validation set. Should be between 0 and 1.
            test_size: size of test set. Should be between 0 and 1.
            train_start: Start event time for the train split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            train_end: End event time for the train split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            validation_start: Start event time for the validation split query, inclusive. Strings
                should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            validation_end: End event time for the validation split query, exclusive. Strings
                should be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_start: Start event time for the test split query, inclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            test_end: End event time for the test split query, exclusive. Strings should
                be formatted in one of the following formats `%Y-%m-%d`, `%Y-%m-%d %H`, `%Y-%m-%d %H:%M`, `%Y-%m-%d %H:%M:%S`,
                or `%Y-%m-%d %H:%M:%S.%f`. Int, i.e Unix Epoch should be in seconds.
            description: A string describing the contents of the training dataset to
                improve discoverability for Data Scientists, defaults to empty string
                `""`.
            extra_filter: Additional filters to be attached to the training dataset.
                The filters will be also applied in `get_batch_data`.
            statistics_config: A configuration object, or a dictionary with keys
                "`enabled`" to generally enable descriptive statistics computation for
                this feature group, `"correlations`" to turn on feature correlation
                computation and `"histograms"` to compute feature value frequencies. The
                values should be booleans indicating the setting. To fully turn off
                statistics computation pass `statistics_config=False`. Defaults to
                `None` and will compute only descriptive statistics.
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                When using the `python` engine, read_options can contain the
                following entries:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
                * key `spark` and value an object of type
                  [hsfs.core.job_configuration.JobConfiguration](../jobs/#jobconfiguration)
                  to configure the Hopsworks Job used to compute the training dataset.
                Defaults to `{}`.
            spine: Spine dataframe with primary key, event time and
                label column to use for point in time join when fetching features. Defaults to `None` and is only required
                when feature view was created with spine group in the feature query.
                It is possible to directly pass a spine group instead of a dataframe to overwrite the left side of the
                feature join, however, the same features as in the original feature group that is being replaced need to
                be available in the spine group.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            training_helper_columns: whether to include training helper columns or not.
                Training helper columns are a list of feature names in the feature view, defined during its creation,
                that are not the part of the model schema itself but can be used during training as a helper for
                extra information. If training helper columns were not defined in the feature view
                then`training_helper_columns=True` will not have any effect. Defaults to `False`, no training helper
                columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (X_train, X_val, X_test, y_train, y_val, y_test):
                Tuple of dataframe of features and labels
        """

        self._validate_train_validation_test_split(
            validation_size=validation_size,
            test_size=test_size,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            test_start=test_start,
        )
        td = training_dataset.TrainingDataset(
            name=self.name,
            version=None,
            splits={},
            validation_size=validation_size,
            test_size=test_size,
            time_split_size=3,
            train_start=train_start,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            test_start=test_start,
            test_end=test_end,
            description=description,
            storage_connector=None,
            featurestore_id=self._featurestore_id,
            data_format="tsv",
            location="",
            statistics_config=statistics_config,
            training_dataset_type=training_dataset.TrainingDataset.IN_MEMORY,
            extra_filter=extra_filter,
        )
        td, df = self._feature_view_engine.get_training_data(
            self,
            read_options,
            training_dataset_obj=td,
            splits=[
                TrainingDatasetSplit.TRAIN,
                TrainingDatasetSplit.VALIDATION,
                TrainingDatasetSplit.TEST,
            ],
            spine=spine,
            primary_keys=kwargs.get("primary_keys") or primary_key,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            dataframe_type=dataframe_type,
            transformation_context=transformation_context,
        )
        warnings.warn(
            "Incremented version to `{}`.".format(td.version),
            util.VersionWarning,
            stacklevel=1,
        )
        self.update_last_accessed_training_dataset(td.version)
        return df

    @staticmethod
    def _validate_train_validation_test_split(
        validation_size: Optional[float],
        test_size: Optional[float],
        train_end: Optional[Union[str, int, datetime, date]],
        validation_start: Optional[Union[str, int, datetime, date]],
        validation_end: Optional[Union[str, int, datetime, date]],
        test_start: Optional[Union[str, int, datetime, date]],
    ) -> None:
        if not (
            (validation_size and 0 < validation_size < 1)
            and (test_size and 0 < test_size < 1)
            and (validation_size + test_size < 1)
            or ((train_end or validation_start) and (validation_end or test_start))
        ):
            raise ValueError(
                "Invalid split input."
                " You should specify either (`validation_size` and `test_size`) or ((`train_end` or `validation_start`) and (`validation_end` or `test_start`))."
                "`validation_size`, `test_size` and sum of `validationSize` and `testSize` should be between 0 and 1 if specified."
            )

    @usage.method_logger
    def get_training_data(
        self,
        training_dataset_version: int,
        read_options: Optional[Dict[str, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        """
        Get training data created by `feature_view.create_training_data`
        or `feature_view.training_data`.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training data
            features_df, labels_df = feature_view.get_training_data(training_dataset_version=1)
            ```

        !!! warning "External Storage Support"
            Reading training data that was written to external storage using a Storage
            Connector other than S3 can currently not be read using HSFS APIs with
            Python as Engine, instead you will have to use the storage's native client.

        # Arguments
            training_dataset_version: training dataset version
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                For python engine:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
                Defaults to `{}`.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            training_helper_columns: whether to include training helper columns or not.
                Training helper columns are a list of feature names in the feature view, defined during its creation,
                that are not the part of the model schema itself but can be used during training as a helper for
                extra information. If training helper columns were not defined in the feature view or during
                materializing training dataset in the file system then`training_helper_columns=True` will not have
                any effect. Defaults to `False`, no training helper columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
        # Returns
            (X, y): Tuple of dataframe of features and labels
        """
        td, df = self._feature_view_engine.get_training_data(
            self,
            read_options,
            training_dataset_version=training_dataset_version,
            primary_keys=kwargs.get("primary_keys") or primary_key,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            dataframe_type=dataframe_type,
            transformation_context=transformation_context,
        )
        self.update_last_accessed_training_dataset(td.version)
        return df

    @usage.method_logger
    def get_train_test_split(
        self,
        training_dataset_version: int,
        read_options: Optional[Dict[Any, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: Optional[str] = "default",
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        """
        Get training data created by `feature_view.create_train_test_split`
        or `feature_view.train_test_split`.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training data
            X_train, X_test, y_train, y_test = feature_view.get_train_test_split(training_dataset_version=1)
            ```

        # Arguments
            training_dataset_version: training dataset version
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                For python engine:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
                Defaults to `{}`.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            training_helper_columns: whether to include training helper columns or not.
                Training helper columns are a list of feature names in the feature view, defined during its creation,
                that are not the part of the model schema itself but can be used during training as a helper for
                extra information. If training helper columns were not defined in the feature view or during
                materializing training dataset in the file system then`training_helper_columns=True` will not have
                any effect. Defaults to `False`, no training helper columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (X_train, X_test, y_train, y_test):
                Tuple of dataframe of features and labels
        """
        td, df = self._feature_view_engine.get_training_data(
            self,
            read_options,
            training_dataset_version=training_dataset_version,
            splits=[TrainingDatasetSplit.TRAIN, TrainingDatasetSplit.TEST],
            primary_keys=kwargs.get("primary_keys") or primary_key,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            dataframe_type=dataframe_type,
            transformation_context=transformation_context,
        )
        self.update_last_accessed_training_dataset(td.version)
        return df

    @usage.method_logger
    def get_train_validation_test_split(
        self,
        training_dataset_version: int,
        read_options: Optional[Dict[str, Any]] = None,
        primary_key: bool = False,
        event_time: bool = False,
        training_helper_columns: bool = False,
        dataframe_type: str = "default",
        transformation_context: Dict[str, Any] = None,
        **kwargs,
    ) -> Tuple[
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        TrainingDatasetDataFrameTypes,
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
        Optional[TrainingDatasetDataFrameTypes],
    ]:
        """
        Get training data created by `feature_view.create_train_validation_test_split`
        or `feature_view.train_validation_test_split`.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training data
            X_train, X_val, X_test, y_train, y_val, y_test = feature_view.get_train_validation_test_splits(training_dataset_version=1)
            ```

        # Arguments
            training_dataset_version: training dataset version
            read_options: Additional options as key/value pairs to pass to the execution engine.
                For spark engine: Dictionary of read options for Spark.
                For python engine:
                * key `"arrow_flight_config"` to pass a dictionary of arrow flight configurations.
                  For example: `{"arrow_flight_config": {"timeout": 900}}`
                Defaults to `{}`.
            primary_key: whether to include primary key features or not.  Defaults to `False`, no primary key
                features.
            event_time: whether to include event time feature or not.  Defaults to `False`, no event time feature.
            training_helper_columns: whether to include training helper columns or not.
                Training helper columns are a list of feature names in the feature view, defined during its creation,
                that are not the part of the model schema itself but can be used during training as a helper for
                extra information. If training helper columns were not defined in the feature view or during
                materializing training dataset in the file system then`training_helper_columns=True` will not have
                any effect. Defaults to `False`, no training helper columns.
            dataframe_type: str, optional. The type of the returned dataframe.
                Possible values are `"default"`, `"spark"`,`"pandas"`, `"polars"`, `"numpy"` or `"python"`.
                Defaults to "default", which maps to Spark dataframe for the Spark Engine and Pandas dataframe for the Python engine.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
        # Returns
            (X_train, X_val, X_test, y_train, y_val, y_test):
                Tuple of dataframe of features and labels
        """
        td, df = self._feature_view_engine.get_training_data(
            self,
            read_options,
            training_dataset_version=training_dataset_version,
            splits=[
                TrainingDatasetSplit.TRAIN,
                TrainingDatasetSplit.VALIDATION,
                TrainingDatasetSplit.TEST,
            ],
            primary_keys=kwargs.get("primary_keys") or primary_key,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            dataframe_type=dataframe_type,
            transformation_context=transformation_context,
        )
        self.update_last_accessed_training_dataset(td.version)
        return df

    @usage.method_logger
    def get_training_datasets(self) -> List["training_dataset.TrainingDatasetBase"]:
        """Returns the metadata of all training datasets created with this feature view.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get all training dataset metadata
            list_tds_meta = feature_view.get_training_datasets()
            ```

        # Returns
            `List[TrainingDatasetBase]` List of training datasets metadata.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_training_datasets(self)

    @usage.method_logger
    def get_training_dataset_statistics(
        self,
        training_dataset_version: int,
        before_transformation: bool = False,
        feature_names: Optional[List[str]] = None,
    ) -> Statistics:
        """
        Get statistics of a training dataset.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get training dataset statistics
            statistics = feature_view.get_training_dataset_statistics(training_dataset_version=1)
            ```

        # Arguments
            training_dataset_version: Training dataset version
            before_transformation: Whether the statistics were computed before transformation functions or not.
            feature_names: List of feature names of which statistics are retrieved.
        # Returns
            `Statistics`
        """
        return self._statistics_engine.get(
            self,
            training_dataset_version=training_dataset_version,
            before_transformation=before_transformation,
            feature_names=feature_names,
        )

    @usage.method_logger
    def add_training_dataset_tag(
        self,
        training_dataset_version: int,
        name: str,
        value: Union[Dict[str, Any], "tag.Tag"],
    ) -> None:
        """Attach a tag to a training dataset.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature feature view instance
            feature_view = fs.get_feature_view(...)

            # attach a tag to a training dataset
            feature_view.add_training_dataset_tag(
                training_dataset_version=1,
                name="tag_schema",
                value={"key", "value"}
            )
            ```

        # Arguments
            training_dataset_version: training dataset version
            name: Name of the tag to be added.
            value: Value of the tag to be added.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.add_tag(
            self, name, value, training_dataset_version=training_dataset_version
        )

    @usage.method_logger
    def get_training_dataset_tag(
        self, training_dataset_version: int, name: str
    ) -> Optional[tag.Tag]:
        """Get the tags of a training dataset.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get a training dataset tag
            tag_str = feature_view.get_training_dataset_tag(
                training_dataset_version=1,
                 name="tag_schema"
            )
            ```

        # Arguments
            training_dataset_version: training dataset version
            name: Name of the tag to get.

        # Returns
            tag value or `None` if it does not exist.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_tag(
            self, name, training_dataset_version=training_dataset_version
        )

    @usage.method_logger
    def get_training_dataset_tags(
        self, training_dataset_version: int
    ) -> Dict[str, tag.Tag]:
        """Returns all tags attached to a training dataset.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # get a training dataset tags
            list_tags = feature_view.get_training_dataset_tags(
                training_dataset_version=1
            )
            ```

        # Returns
            `Dict[str, obj]` of tags.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.get_tags(
            self, training_dataset_version=training_dataset_version
        )

    @usage.method_logger
    def delete_training_dataset_tag(
        self, training_dataset_version: int, name: str
    ) -> None:
        """Delete a tag attached to a training dataset.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # delete training dataset tag
            feature_view.delete_training_dataset_tag(
                training_dataset_version=1,
                name='name_of_dataset'
            )
            ```

        # Arguments
            training_dataset_version: training dataset version
            name: Name of the tag to be removed.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._feature_view_engine.delete_tag(
            self, name, training_dataset_version=training_dataset_version
        )

    @usage.method_logger
    def purge_training_data(self, training_dataset_version: int) -> None:
        """Delete a training dataset (data only).

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # purge training data
            feature_view.purge_training_data(training_dataset_version=1)
            ```

        # Arguments
            training_dataset_version: Version of the training dataset to be removed.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if self._last_accessed_training_dataset == training_dataset_version:
            self.update_last_accessed_training_dataset(None)
        self._feature_view_engine.delete_training_dataset_only(
            self, training_data_version=training_dataset_version
        )

    @usage.method_logger
    def purge_all_training_data(self) -> None:
        """Delete all training datasets (data only).

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # purge all training data
            feature_view.purge_all_training_data()
            ```

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if self._last_accessed_training_dataset is not None:
            self.update_last_accessed_training_dataset(None)
        self._feature_view_engine.delete_training_dataset_only(self)

    @usage.method_logger
    def delete_training_dataset(self, training_dataset_version: int) -> None:
        """Delete a training dataset. This will delete both metadata and training data.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # delete a training dataset
            feature_view.delete_training_dataset(
                training_dataset_version=1
            )
            ```

        # Arguments
            training_dataset_version: Version of the training dataset to be removed.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if self._last_accessed_training_dataset == training_dataset_version:
            self.update_last_accessed_training_dataset(None)
        self._feature_view_engine.delete_training_data(
            self, training_data_version=training_dataset_version
        )

    @usage.method_logger
    def delete_all_training_datasets(self) -> None:
        """Delete all training datasets. This will delete both metadata and training data.

        !!! example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # delete all training datasets
            feature_view.delete_all_training_datasets()
            ```

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        if self._last_accessed_training_dataset is not None:
            self.update_last_accessed_training_dataset(None)
        self._feature_view_engine.delete_training_data(self)

    def get_feature_monitoring_configs(
        self,
        name: Optional[str] = None,
        feature_name: Optional[str] = None,
        config_id: Optional[int] = None,
    ) -> Union[
        "fmc.FeatureMonitoringConfig", List["fmc.FeatureMonitoringConfig"], None
    ]:
        """Fetch feature monitoring configs attached to the feature view.
        If no arguments is provided the method will return all feature monitoring configs
        attached to the feature view, meaning all feature monitoring configs that are attach
        to a feature in the feature view. If you wish to fetch a single config, provide the
        its name. If you wish to fetch all configs attached to a particular feature, provide
        the feature name.
        !!! example
            ```python3
            # fetch your feature view
            fv = fs.get_feature_view(name="my_feature_view", version=1)
            # fetch all feature monitoring configs attached to the feature view
            fm_configs = fv.get_feature_monitoring_configs()
            # fetch a single feature monitoring config by name
            fm_config = fv.get_feature_monitoring_configs(name="my_config")
            # fetch all feature monitoring configs attached to a particular feature
            fm_configs = fv.get_feature_monitoring_configs(feature_name="my_feature")
            # fetch a single feature monitoring config with a particular id
            fm_config = fv.get_feature_monitoring_configs(config_id=1)
            ```
        # Arguments
            name: If provided fetch only the feature monitoring config with the given name.
                Defaults to None.
            feature_name: If provided, fetch only configs attached to a particular feature.
                Defaults to None.
            config_id: If provided, fetch only the feature monitoring config with the given id.
                Defaults to None.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
            `hopsworks.client.exceptions.FeatureStoreException`: If the feature view is not registered in Hopsworks
            `ValueError`: if both name and feature_name are provided.
            `TypeError`: if name or feature_name are not string or None.
        # Return
            Union[`FeatureMonitoringConfig`, List[`FeatureMonitoringConfig`], None]
                A list of feature monitoring configs. If name provided,
                returns either a single config or None if not found.
        """
        # TODO: Should this filter out scheduled statistics only configs?
        if not self._id:
            raise FeatureStoreException(
                "Only Feature Group registered with Hopsworks can fetch feature monitoring configurations."
            )

        return self._feature_monitoring_config_engine.get_feature_monitoring_configs(
            name=name,
            feature_name=feature_name,
            config_id=config_id,
        )

    def get_feature_monitoring_history(
        self,
        config_name: Optional[str] = None,
        config_id: Optional[int] = None,
        start_time: Optional[Union[int, str, datetime, date]] = None,
        end_time: Optional[Union[int, str, datetime, date]] = None,
        with_statistics: Optional[bool] = True,
    ) -> List["fmr.FeatureMonitoringResult"]:
        """Fetch feature monitoring history for a given feature monitoring config.
        !!! example
            ```python3
            # fetch your feature view
            fv = fs.get_feature_view(name="my_feature_group", version=1)
            # fetch feature monitoring history for a given feature monitoring config
            fm_history = fv.get_feature_monitoring_history(
                config_name="my_config",
                start_time="2020-01-01",
            )
            # or use the config id
            fm_history = fv.get_feature_monitoring_history(
                config_id=1,
                start_time=datetime.now() - timedelta(weeks=2),
                end_time=datetime.now() - timedelta(weeks=1),
                with_statistics=False,
            )
            ```
        # Arguments
            config_name: The name of the feature monitoring config to fetch history for.
                Defaults to None.
            config_id: The id of the feature monitoring config to fetch history for.
                Defaults to None.
            start_date: The start date of the feature monitoring history to fetch.
                Defaults to None.
            end_date: The end date of the feature monitoring history to fetch.
                Defaults to None.
            with_statistics: Whether to include statistics in the feature monitoring history.
                Defaults to True. If False, only metadata about the monitoring will be fetched.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: In case the backend encounters an issue
            `hopsworks.client.exceptions.FeatureStoreException`: If the feature view is not registered in Hopsworks
            `ValueError`: if both config_name and config_id are provided.
            `TypeError`: if config_name or config_id are not respectively string, int or None.
        # Return
            List[`FeatureMonitoringResult`]
                A list of feature monitoring results containing the monitoring metadata
                as well as the computed statistics for the detection and reference window
                if requested.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature View registered with Hopsworks can fetch feature monitoring history."
            )

        return self._feature_monitoring_result_engine.get_feature_monitoring_results(
            config_name=config_name,
            config_id=config_id,
            start_time=start_time,
            end_time=end_time,
            with_statistics=with_statistics,
        )

    def create_statistics_monitoring(
        self,
        name: str,
        feature_name: Optional[str] = None,
        description: Optional[str] = None,
        start_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        end_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        cron_expression: Optional[str] = "0 0 12 ? * * *",
    ) -> "fmc.FeatureMonitoringConfig":
        """Run a job to compute statistics on snapshot of feature data on a schedule.
        !!! experimental
            Public API is subject to change, this feature is not suitable for production use-cases.
        !!! example
            ```python3
            # fetch feature view
            fv = fs.get_feature_view(name="my_feature_view", version=1)
            # enable statistics monitoring
            my_config = fv._create_statistics_monitoring(
                name="my_config",
                start_date_time="2021-01-01 00:00:00",
                description="my description",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                # Statistics computed on 10% of the last week of data
                time_offset="1w",
                row_percentage=0.1,
            ).save()
            ```
        # Arguments
            name: Name of the feature monitoring configuration.
                name must be unique for all configurations attached to the feature view.
            feature_name: Name of the feature to monitor. If not specified, statistics
                will be computed for all features.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression to use to schedule the job. The cron expression
                must be in UTC and follow the Quartz specification. Default is '0 0 12 ? * * *',
                every day at 12pm UTC.
        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: If the feature view is not registered in Hopsworks
        # Return
            `FeatureMonitoringConfig` Configuration with minimal information about the feature monitoring.
                Additional information are required before feature monitoring is enabled.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature View registered with Hopsworks can enable scheduled statistics monitoring."
            )

        return self._feature_monitoring_config_engine._build_default_statistics_monitoring_config(
            name=name,
            feature_name=feature_name,
            description=description,
            start_date_time=start_date_time,
            valid_feature_names=[feat.name for feat in self._features],
            cron_expression=cron_expression,
            end_date_time=end_date_time,
        )

    def create_feature_monitoring(
        self,
        name: str,
        feature_name: str,
        description: Optional[str] = None,
        start_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        end_date_time: Optional[Union[int, str, datetime, date, pd.Timestamp]] = None,
        cron_expression: Optional[str] = "0 0 12 ? * * *",
    ) -> "fmc.FeatureMonitoringConfig":
        """Enable feature monitoring to compare statistics on snapshots of feature data over time.
        !!! experimental
            Public API is subject to change, this feature is not suitable for production use-cases.
        !!! example
            ```python3
            # fetch feature view
            fg = fs.get_feature_view(name="my_feature_view", version=1)
            # enable feature monitoring
            my_config = fg.create_feature_monitoring(
                name="my_monitoring_config",
                feature_name="my_feature",
                description="my monitoring config description",
                cron_expression="0 0 12 ? * * *",
            ).with_detection_window(
                # Data inserted in the last day
                time_offset="1d",
                window_length="1d",
            ).with_reference_window(
                # compare to a given value
                specific_value=0.5,
            ).compare_on(
                metric="mean",
                threshold=0.5,
            ).save()
            ```
        # Arguments
            name: Name of the feature monitoring configuration.
                name must be unique for all configurations attached to the feature group.
            feature_name: Name of the feature to monitor.
            description: Description of the feature monitoring configuration.
            start_date_time: Start date and time from which to start computing statistics.
            end_date_time: End date and time at which to stop computing statistics.
            cron_expression: Cron expression to use to schedule the job. The cron expression
                must be in UTC and follow the Quartz specification. Default is '0 0 12 ? * * *',
                every day at 12pm UTC.
        # Raises
            `hopsworks.client.exceptions.FeatureStoreException`: If the feature view is not registered in Hopsworks
        # Return
            `FeatureMonitoringConfig` Configuration with minimal information about the feature monitoring.
                Additional information are required before feature monitoring is enabled.
        """
        if not self._id:
            raise FeatureStoreException(
                "Only Feature View registered with Hopsworks can enable feature monitoring."
            )

        return self._feature_monitoring_config_engine._build_default_feature_monitoring_config(
            name=name,
            feature_name=feature_name,
            description=description,
            start_date_time=start_date_time,
            valid_feature_names=[feat.name for feat in self._features],
            end_date_time=end_date_time,
            cron_expression=cron_expression,
        )

    def get_alerts(self):
        """Get all alerts for this feature view.

        # Returns
            List[FeatureViewAlert] or Alert.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request.
        """
        return self._alert_api.get_feature_view_alerts(
            self._feature_store_id,
            feature_view_name=self._name,
            feature_view_version=self._version,
        )

    def get_alert(self, alert_id: int):
        """Get an alert for this feature view by ID.

        # Arguments
            alert_id: The id of the alert to get..
        # Returns
            A single FeatureViewAlert object is returned.
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request.
        """
        return self._alert_api.get_feature_view_alert(
            self._feature_store_id,
            feature_view_name=self._name,
            feature_view_version=self._version,
            alert_id=alert_id,
        )

    def create_alert(
        self,
        receiver: str,
        status: str,
        severity: str,
    ):
        """Create an alert for this feature view.
        !!! example
            ```python
            # get feature store instance
            fs = ...
            # get feature view instance
            feature_view = fs.get_feature_view(...)
            # create an alert
            alert = feature_view.create_alert(
                receiver="email",
                status="feature_monitor_shift_undetected",
                severity="info",
            )
            ```
        # Arguments
            receiver: str. The receiver of the alert.
            status: str. The status that will trigger the alert. Can be "feature_monitor_shift_undetected" or "feature_monitor_shift_detected".
            severity: str. The severity of the alert. Can be "info", "warning" or "critical".
        # Returns
            The created FeatureViewAlert object.
        # Raises
            `ValueError`: If the status is not valid.
            `ValueError`: If the severity is not valid.
            `hopsworks.client.exceptions.RestAPIError`: If the backend encounters an error when handling the request
        """
        return self._alert_api.create_feature_view_alert(
            feature_store_id=self._feature_store_id,
            feature_view_name=self._name,
            feature_view_version=self._version,
            receiver=receiver,
            status=status,
            severity=severity,
        )

    @classmethod
    def from_response_json(cls, json_dict: Dict[str, Any]) -> "FeatureView":
        """
        Function that constructs the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `TransformationFunction`: Json deserialized class object.
        """
        json_decamelized = humps.decamelize(json_dict)

        serving_keys = json_decamelized.get("serving_keys", None)
        if serving_keys is not None:
            serving_keys = [
                skm.ServingKey.from_response_json(sk) for sk in serving_keys
            ]
        transformation_functions = json_decamelized.get("transformation_functions", {})
        fv = cls(
            id=json_decamelized.get("id", None),
            name=json_decamelized["name"],
            query=query.Query.from_response_json(json_decamelized["query"]),
            featurestore_id=json_decamelized["featurestore_id"],
            version=json_decamelized.get("version", None),
            description=json_decamelized.get("description", None),
            featurestore_name=json_decamelized.get("featurestore_name", None),
            serving_keys=serving_keys,
            logging_enabled=json_decamelized.get("logging_enabled", False),
            transformation_functions=[
                TransformationFunction.from_response_json(
                    {
                        **transformation_function,
                        "transformation_type": TransformationType.MODEL_DEPENDENT,
                    }
                )
                for transformation_function in transformation_functions
            ]
            if transformation_functions
            else [],
        )
        features = json_decamelized.get("features", [])
        if features:
            for feature_index in range(len(features)):
                feature = (
                    training_dataset_feature.TrainingDatasetFeature.from_response_json(
                        features[feature_index]
                    )
                )
                features[feature_index] = feature
        fv.schema = features
        fv.labels = [feature.name for feature in features if feature.label]
        fv.inference_helper_columns = [
            feature.name for feature in features if feature.inference_helper_column
        ]
        fv.training_helper_columns = [
            feature.name for feature in features if feature.training_helper_column
        ]
        return fv

    def update_from_response_json(self, json_dict: Dict[str, Any]) -> "FeatureView":
        """
        Function that updates the class object from its json serialization.

        # Arguments
            json_dict: `Dict[str, Any]`. Json serialized dictionary for the class.
        # Returns
            `TransformationFunction`: Json deserialized class object.
        """
        other = self.from_response_json(json_dict)
        for key in [
            "name",
            "description",
            "id",
            "query",
            "featurestore_id",
            "version",
            "labels",
            "inference_helper_columns",
            "training_helper_columns",
            "transformation_functions",
            "schema",
            "serving_keys",
            "logging_enabled",
        ]:
            self._update_attribute_if_present(self, other, key)
        self._init_feature_monitoring_engine()
        return self

    def compute_on_demand_features(
        self,
        feature_vector: Optional[
            Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]
        ] = None,
        request_parameters: Optional[
            Union[List[Dict[str, Any]], Dict[str, Any]]
        ] = None,
        transformation_context: Dict[str, Any] = None,
        return_type: Union[Literal["list", "numpy", "pandas", "polars"]] = None,
    ):
        """
        Function computes on-demand features present in the feature view.

        # Arguments
            feature_vector: `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`. The feature vector to be transformed.
            request_parameters: Request parameters required by on-demand transformation functions to compute on-demand features present in the feature view.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
            return_type: `"list"`, `"pandas"`, `"polars"` or `"numpy"`. Defaults to the same type as the input feature vector.
        # Returns
            `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`: The feature vector that contains all on-demand features in the feature view.
        """

        return self._vector_server.compute_on_demand_features(
            feature_vectors=feature_vector,
            request_parameters=request_parameters,
            transformation_context=transformation_context,
            return_type=return_type,
        )

    def transform(
        self,
        feature_vector: Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame],
        external: Optional[bool] = None,
        transformation_context: Dict[str, Any] = None,
        return_type: Union[Literal["list", "numpy", "pandas", "polars"]] = None,
    ):
        """
        Transform the input feature vector by applying Model-dependent transformations attached to the feature view.

        !!! warning "List input must match the schema of the feature view"
                    If features are provided as a List to the transform function. Make sure that the input are ordered to match the schema
                    in the feature view.
        # Arguments
            feature_vector: `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`. The feature vector to be transformed.
            external: boolean, optional. If set to True, the connection to the
                online feature store is established using the same host as
                for the `host` parameter in the [`hopsworks.login()`](login.md#login) method.
                If set to False, the online feature store storage connector is used
                which relies on the private IP. Defaults to True if connection to Hopsworks is established from
                external environment (e.g AWS Sagemaker or Google Colab), otherwise to False.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                These variables must be explicitly defined as parameters in the transformation function to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
            return_type: `"list"`, `"pandas"`, `"polars"` or `"numpy"`. Defaults to the same type as the input feature vector.
        # Returns
            `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`: The transformed feature vector obtained by applying Model-Dependent Transformations.
        """
        if not self._vector_server._serving_initialized:
            self.init_serving(external=external)

        return self._vector_server.transform(
            feature_vectors=feature_vector,
            transformation_context=transformation_context,
            return_type=return_type,
        )

    def enable_logging(self) -> None:
        """Enable feature logging for the current feature view.

        This method activates logging of features.

        # Example
            ```python
            # get feature store instance
            fs = ...

            # get feature view instance
            feature_view = fs.get_feature_view(...)

            # enable logging
            feature_view.enable_logging()
            ```
        # Raises
            `hopsworks.client.exceptions.RestAPIError`: In case the backend encounters an issue
        """
        fv = self._feature_view_engine.enable_feature_logging(self)
        self._feature_logging = self._feature_view_engine.get_feature_logging(fv)
        return fv

    def log(
        self,
        untransformed_features: Union[
            pd.DataFrame, list[list], np.ndarray, TypeVar("pyspark.sql.DataFrame")
        ] = None,
        predictions: Optional[Union[pd.DataFrame, list[list], np.ndarray]] = None,
        transformed_features: Union[
            pd.DataFrame, list[list], np.ndarray, TypeVar("pyspark.sql.DataFrame")
        ] = None,
        write_options: Optional[Dict[str, Any]] = None,
        training_dataset_version: Optional[int] = None,
        model: Model = None,
    ) -> Optional[list[Job]]:
        """Log features and optionally predictions for the current feature view. The logged features are written periodically to the offline store. If you need it to be available immediately, call `materialize_log`.

        Note: If features is a `pyspark.Dataframe`, prediction needs to be provided as columns in the dataframe,
            values in `predictions` will be ignored.

        # Arguments
            untransformed_features: The untransformed features to be logged. Can be a pandas DataFrame, a list of lists, or a numpy ndarray.
            prediction: The predictions to be logged. Can be a pandas DataFrame, a list of lists, or a numpy ndarray. Defaults to None.
            transformed_features: The transformed features to be logged. Can be a pandas DataFrame, a list of lists, or a numpy ndarray.
            write_options: Options for writing the log. Defaults to None.
            training_dataset_version: Version of the training dataset. If training dataset version is definied in
                `init_serving` or `init_batch_scoring`, or model has training dataset version,
                or training dataset version was cached, then the version will be used, otherwise defaults to None.
            model: `hsml.model.Model` Hopsworks model associated with the log. Defaults to None.

        # Returns
            `list[Job]` job information for feature insertion if python engine is used

        # Example
            ```python
            # log untransformed features
            feature_view.log(features)
            # log features and predictions
            feature_view.log(features, prediction)
            ```

            ```python
            # log both untransformed and transformed features
            feature_view.log(
                untransformed_features=features,
                transformed_features=transformed_features
            )
            ```

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to log features.
        """
        if not self.logging_enabled:
            warnings.warn(
                "Feature logging is not enabled. It may take longer to enable logging before logging the features. You can call `feature_view.enable_logging()` to enable logging beforehand.",
                stacklevel=1,
            )
            self.enable_logging()
        return self._feature_view_engine.log_features(
            self,
            self.feature_logging,
            untransformed_features,
            transformed_features,
            predictions,
            write_options,
            training_dataset_version=(
                training_dataset_version
                or self._serving_training_dataset_version
                or (model.training_dataset_version if model else None)
                or self.get_last_accessed_training_dataset()
            ),
            hsml_model=model,
            logger=self._feature_logger,
        )

    def get_log_timeline(
        self,
        wallclock_time: Optional[Union[str, int, datetime, datetime.date]] = None,
        limit: Optional[int] = None,
        transformed: Optional[bool] = False,
    ) -> Dict[str, Dict[str, str]]:
        """Retrieve the log timeline for the current feature view.

        # Arguments
            wallclock_time: Specific time to get the log timeline for. Can be a string, integer, datetime, or date. Defaults to None.
            limit: Maximum number of entries to retrieve. Defaults to None.
            transformed: Whether to include transformed logs. Defaults to False.

        # Example
            ```python
            # get log timeline
            log_timeline = feature_view.get_log_timeline(limit=10)
            ```

        # Returns
            `Dict[str, Dict[str, str]]`. Dictionary object of commit metadata timeline, where Key is commit id and value
            is `Dict[str, str]` with key value pairs of date committed on, number of rows updated, inserted and deleted.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to retrieve the log timeline.
        """
        return self._feature_view_engine.get_log_timeline(
            self, wallclock_time, limit, transformed
        )

    def read_log(
        self,
        start_time: Optional[Union[str, int, datetime, datetime.date]] = None,
        end_time: Optional[Union[str, int, datetime, datetime.date]] = None,
        filter: Optional[Union[Filter, Logic]] = None,
        transformed: Optional[bool] = False,
        training_dataset_version: Optional[int] = None,
        model: Model = None,
    ) -> Union[
        TypeVar("pyspark.sql.DataFrame"),
        pd.DataFrame,
        pl.DataFrame,
    ]:
        """Read the log entries for the current feature view.
            Optionally, filter can be applied to start/end time, training dataset version, hsml model,
            and custom fitler.

        # Arguments
            start_time: Start time for the log entries. Can be a string, integer, datetime, or date. Defaults to None.
            end_time: End time for the log entries. Can be a string, integer, datetime, or date. Defaults to None.
            filter: Filter to apply on the log entries. Can be a Filter or Logic object. Defaults to None.
            transformed: Whether to include transformed logs. Defaults to False.
            training_dataset_version: Version of the training dataset. Defaults to None.
            model: HSML model associated with the log. Defaults to None.

        # Example
            ```python
            # read all log entries
            log_entries = feature_view.read_log()
            # read log entries within time ranges
            log_entries = feature_view.read_log(start_time="2022-01-01", end_time="2022-01-31")
            # read log entries of a specific training dataset version
            log_entries = feature_view.read_log(training_dataset_version=1)
            # read log entries of a specific hopsworks model
            log_entries = feature_view.read_log(model=Model(1, "dummy", version=1))
            # read log entries by applying filter on features of feature group `fg` in the feature view
            log_entries = feature_view.read_log(filter=fg.feature1 > 10)
            ```

        # Returns
            `DataFrame`: The spark dataframe containing the feature data.
            `pyspark.DataFrame`. A Spark DataFrame.
            `pandas.DataFrame`. A Pandas DataFrame.
            `polars.DataFrame`. A Polars DataFrame.

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to read the log entries.
        """
        return self._feature_view_engine.read_feature_logs(
            self,
            start_time,
            end_time,
            filter,
            transformed,
            training_dataset_version,
            model,
        )

    def pause_logging(self) -> None:
        """Pause scheduled materialization job for the current feature view.

        # Example
            ```python
            # pause logging
            feature_view.pause_logging()
            ```

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to pause feature logging.
        """
        self._feature_view_engine.pause_logging(self)

    def resume_logging(self) -> None:
        """Resume scheduled materialization job for the current feature view.

        # Example
            ```python
            # resume logging
            feature_view.resume_logging()
            ```

        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to pause feature logging.
        """
        self._feature_view_engine.resume_logging(self)

    def materialize_log(
        self, wait: bool = False, transformed: Optional[bool] = None
    ) -> List[Job]:
        """Materialize the log for the current feature view.

        # Arguments
            wait: Whether to wait for the materialization to complete. Defaults to False.
            transformed: Whether to materialize transformed or untrasformed logs. Defaults to None, in which case the returned list contains a job for materialization of transformed features and then a job for untransformed features. Otherwise the list contains only transformed jobs if transformed is True and untransformed jobs if it is False.

        # Example
            ```python
            # materialize log
            materialization_result = feature_view.materialize_log(wait=True)
            ```

        # Returns
            List[`Job`] Job information for the materialization jobs of transformed and untransformed features.


        # Raises
            `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to materialize the log.
        """
        return self._feature_view_engine.materialize_feature_logs(
            self, wait, transformed
        )

    def delete_log(self, transformed: Optional[bool] = None) -> None:
        """Delete the logged feature data for the current feature view.

        # Arguments
            transformed: Whether to delete transformed logs. Defaults to None. Delete both transformed and untransformed logs.

        # Example
            ```python
            # delete log
            feature_view.delete_log()
            ```

         # Raises
             `hopsworks.client.exceptions.RestAPIError`: in case the backend fails to delete the log.
        """
        if self.feature_logging is not None:
            self._feature_view_engine.delete_feature_logs(
                self, self.feature_logging, transformed
            )

    @staticmethod
    def _update_attribute_if_present(this: "FeatureView", new: Any, key: str) -> None:
        if getattr(new, key):
            setattr(this, key, getattr(new, key))

    def _init_feature_monitoring_engine(self) -> None:
        self._feature_monitoring_config_engine = (
            feature_monitoring_config_engine.FeatureMonitoringConfigEngine(
                feature_store_id=self._featurestore_id,
                feature_view_name=self._name,
                feature_view_version=self._version,
            )
        )
        self._feature_monitoring_result_engine = (
            feature_monitoring_result_engine.FeatureMonitoringResultEngine(
                feature_store_id=self._featurestore_id,
                feature_view_name=self._name,
                feature_view_version=self._version,
            )
        )

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
        return {
            "featurestoreId": self._featurestore_id,
            "name": self._name,
            "version": self._version,
            "description": self._description,
            "query": self._query,
            "features": self._features,
            "loggingEnabled": self._logging_enabled,
            "transformationFunctions": self._transformation_functions,
            "type": "featureViewDTO",
        }

    def get_training_dataset_schema(
        self, training_dataset_version: Optional[int] = None
    ) -> List[training_dataset_feature.TrainingDatasetFeature]:
        """
        Function that returns the schema of the training dataset that is generated from a feature view.
        It provides the schema of the features after all transformation functions have been applied.

        # Arguments
            training_dataset_version: Specifies the version of the training dataset for which the schema should be generated.
                By default, this is set to None. However, if the `one_hot_encoder` transformation function is used, the training dataset version must be provided.
                This is because the schema will then depend on the statistics of the training data used.
        # Example
            ```python
            schema = feature_view.get_training_dataset_schema(training_dataset_version=1)
            ```

        # Returns
            `List[training_dataset_feature.TrainingDatasetFeature]`: List of training dataset features objects.
        """
        return self._feature_view_engine.get_training_dataset_schema(
            self, training_dataset_version
        )

    @property
    def id(self) -> int:
        """Feature view id."""
        return self._id

    @id.setter
    def id(self, id: Optional[int]) -> None:
        self._id = id

    @property
    def featurestore_id(self) -> int:
        """Feature store id."""
        return self._featurestore_id

    @featurestore_id.setter
    def featurestore_id(self, id: Optional[int]) -> None:
        self._featurestore_id = id

    @property
    def feature_store_name(self) -> Optional[str]:
        """Name of the feature store in which the feature group is located."""
        return self._feature_store_name

    @property
    def name(self) -> str:
        """Name of the feature view."""
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def version(self) -> int:
        """Version number of the feature view."""
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def labels(self) -> List[str]:
        """The labels/prediction feature of the feature view.

        Can be a composite of multiple features.
        """
        return self._labels

    @labels.setter
    def labels(self, labels: List[str]) -> None:
        self._labels = [util.autofix_feature_name(lb) for lb in labels]

    @property
    def inference_helper_columns(self) -> List[str]:
        """The helper column sof the feature view.

        Can be a composite of multiple features.
        """
        return self._inference_helper_columns

    @inference_helper_columns.setter
    def inference_helper_columns(self, inference_helper_columns: List[str]) -> None:
        self._inference_helper_columns = [
            util.autofix_feature_name(exf) for exf in inference_helper_columns
        ]

    @property
    def training_helper_columns(self) -> List[str]:
        """The helper column sof the feature view.

        Can be a composite of multiple features.
        """
        return self._training_helper_columns

    @training_helper_columns.setter
    def training_helper_columns(self, training_helper_columns: List[str]) -> None:
        self._training_helper_columns = [
            util.autofix_feature_name(exf) for exf in training_helper_columns
        ]

    @property
    def description(self) -> Optional[str]:
        """Description of the feature view."""
        return self._description

    @description.setter
    def description(self, description: Optional[str]) -> None:
        self._description = description

    @property
    def query(self) -> "query.Query":
        """Query of the feature view."""
        return self._query

    @query.setter
    def query(self, query_obj: "query.Query") -> None:
        self._query = query_obj

    @property
    def transformation_functions(
        self,
    ) -> List[TransformationFunction]:
        """Get transformation functions."""
        return self._transformation_functions

    @transformation_functions.setter
    def transformation_functions(
        self,
        transformation_functions: List[TransformationFunction],
    ) -> None:
        self._transformation_functions = transformation_functions

    @property
    def model_dependent_transformations(self) -> Dict["str", Callable]:
        """Get Model-Dependent transformations as a dictionary mapping transformed feature names to transformation function"""
        return {
            transformation_function.hopsworks_udf.output_column_names[
                0
            ]: transformation_function.hopsworks_udf.get_udf()
            for transformation_function in self.transformation_functions
        }

    @property
    def on_demand_transformations(self) -> Dict["str", Callable]:
        """Get On-Demand transformations as a dictionary mapping on-demand feature names to transformation function"""
        return {
            feature.on_demand_transformation_function.hopsworks_udf.function_name: feature.on_demand_transformation_function.hopsworks_udf.get_udf()
            for feature in self.features
            if feature.on_demand_transformation_function
        }

    @property
    def schema(self) -> List[training_dataset_feature.TrainingDatasetFeature]:
        """Schema of untransformed features in the Feature view."""
        return self._features

    @property
    def features(self) -> List[training_dataset_feature.TrainingDatasetFeature]:
        """Schema of untransformed features in the Feature view. (alias)"""
        return self._features

    @schema.setter
    def schema(
        self, features: List[training_dataset_feature.TrainingDatasetFeature]
    ) -> None:
        self._features = features

    @property
    def primary_keys(self) -> Set[str]:
        """Set of primary key names that is required as keys in input dict object
        for [`get_feature_vector(s)`](#get_feature_vector) method.
        When there are duplicated primary key names and prefix is not defined in the query,
        prefix is generated and prepended to the primary key name in this format
        "fgId_{feature_group_id}_{join_index}" where `join_index` is the order of the join.
        """
        if not (hasattr(self, "_primary_keys") and len(self._primary_keys) > 0):
            self._primary_keys = set(
                [key.required_serving_key for key in self.serving_keys]
            )
        return self._primary_keys

    @property
    def serving_keys(self) -> List[skm.ServingKey]:
        """All primary keys of the feature groups included in the query."""
        if (
            (not hasattr(self, "_serving_keys"))
            or self._serving_keys is None
            or len(self._serving_keys) == 0
        ):
            self._serving_keys = util.build_serving_keys_from_prepared_statements(
                self._feature_view_engine._feature_view_api.get_serving_prepared_statement(
                    name=self.name,
                    version=self.version,
                    batch=False,
                    inference_helper_columns=False,
                ),
                feature_store_id=self.featurestore_id,
                ignore_prefix=True,  # if serving_keys have to be built it is because feature_view older than 3.3, this ensure compatibility
            )
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_keys: List[skm.ServingKey]) -> None:
        self._serving_keys = serving_keys

    @property
    def logging_enabled(self) -> bool:
        return self._logging_enabled

    @logging_enabled.setter
    def logging_enabled(self, logging_enabled) -> None:
        self._logging_enabled = logging_enabled

    @property
    def feature_logging(self) -> Optional[FeatureLogging]:
        if self.logging_enabled and self._feature_logging is None:
            self._feature_logging = self._feature_view_engine.get_feature_logging(self)
        return self._feature_logging

    @property
    def _vector_server(self) -> vector_server.VectorServer:
        if not self.__vector_server:
            self.__vector_server = vector_server.VectorServer(
                feature_store_id=self._featurestore_id,
                features=self._features,
                serving_keys=self._serving_keys,
                skip_fg_ids=set([fg.id for fg in self._get_embedding_fgs()]),
                feature_view_name=self._name,
                feature_view_version=self._version,
                feature_store_name=self._feature_store_name,
            )
        return self.__vector_server

    @property
    def _batch_scoring_server(self) -> vector_server.VectorServer:
        if not self.__batch_scoring_server:
            self.__batch_scoring_server = vector_server.VectorServer(
                feature_store_id=self._featurestore_id,
                features=self._features,
                serving_keys=self._serving_keys,
                skip_fg_ids=set([fg.id for fg in self._get_embedding_fgs()]),
                feature_view_name=self._name,
                feature_view_version=self._version,
                feature_store_name=self._feature_store_name,
            )
        return self.__batch_scoring_server

    @property
    def _feature_groups(self) -> List[feature_group.FeatureGroup]:
        if not self.__feature_groups:
            self.__feature_groups = (
                self._feature_view_engine._get_feature_group_from_query(self.query)
            )
        return self.__feature_groups
