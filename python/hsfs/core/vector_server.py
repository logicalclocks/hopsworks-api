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

import itertools
import logging
import warnings
from base64 import b64decode
from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
)

import pandas as pd
from hopsworks_common import client
from hopsworks_common.core.constants import (
    HAS_AVRO,
    HAS_FAST_AVRO,
    HAS_NUMPY,
    HAS_POLARS,
    avro_not_installed_message,
    numpy_not_installed_message,
    polars_not_installed_message,
)
from hopsworks_common.core.type_systems import create_extended_type
from hsfs.client import exceptions, online_store_rest_client
from hsfs.core import (
    online_store_rest_client_engine,
    online_store_sql_engine,
)
from hsfs.core import (
    transformation_function_engine as tf_engine_mod,
)
from hsfs.core.feature_logging import LoggingMetaData
from hsfs.hopsworks_udf import UDFExecutionMode


if HAS_NUMPY:
    import numpy as np

if HAS_FAST_AVRO:
    from fastavro import schemaless_reader
if HAS_AVRO:
    import avro.io
    import avro.schema
    from avro.io import BinaryDecoder

if HAS_POLARS:
    import polars as pl

if TYPE_CHECKING:
    from hsfs import (
        feature_view,
        training_dataset,
        transformation_function,
    )
    from hsfs import (
        serving_key as sk_mod,
    )
    from hsfs import training_dataset_feature as tdf_mod
    from hsfs.feature_group import FeatureGroup

_logger = logging.getLogger(__name__)


class VectorServer:
    DEFAULT_REST_CLIENT = "rest"
    DEFAULT_SQL_CLIENT = "sql"
    # Compatibility with 3.7
    DEFAULT_CLIENT_KEY = "default_online_store_client"
    REST_CLIENT_CONFIG_OPTIONS_KEY = "config_online_store_rest_client"
    RESET_REST_CLIENT_OPTIONS_KEY = "reset_online_store_rest_client"
    SQL_TIMESTAMP_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        feature_store_id: int,
        features: list[tdf_mod.TrainingDatasetFeature] | None = None,
        serving_keys: list[sk_mod.ServingKey] | None = None,
        skip_fg_ids: set[int] | None = None,
        skip_feature_decoding_fg_ids: set[int] | None = None,
        feature_store_name: str | None = None,
        feature_view_name: str | None = None,
        feature_view_version: int | None = None,
    ):
        self._training_dataset_version = None
        self._feature_store_id = feature_store_id
        self._feature_store_name = feature_store_name
        self._feature_view_name = feature_view_name
        self._feature_view_version = feature_view_version

        if features is None:
            features = []
        self._features = features
        self._feature_vector_col_name = [
            feat.name
            for feat in features
            if not (
                feat.label
                or feat.inference_helper_column
                or feat.training_helper_column
            )
        ]
        # Names of untransformed features excluding on-demand features.
        self._untransformed_feature_vector_col_name = [
            feat.name
            for feat in features
            if not (
                feat.label
                or feat.training_helper_column
                or feat.on_demand_transformation_function
                or feat.inference_helper_column
            )
        ]
        # Names of untransformed features including on-demand features.
        # The untransformed feature vector will by default contain on-demand features if present in the feature view.
        self._on_demand_feature_vector_col_name = [
            feat.name
            for feat in features
            if not (
                feat.label
                or feat.training_helper_column
                or feat.inference_helper_column
            )
        ]
        self._inference_helper_col_name = [
            feat.name for feat in features if feat.inference_helper_column
        ]
        self._transformed_feature_vector_col_name: list[str] = None
        self._skip_fg_ids = skip_fg_ids or set()
        self._serving_keys = serving_keys or []
        self._required_serving_keys = []

        self._model_dependent_transformation_functions: list[
            transformation_function.TransformationFunction
        ] = []
        self._on_demand_transformation_functions: list[
            transformation_function.TransformationFunction
        ] = []
        self._on_demand_transformation_functions_execution_graph: list[
            list[transformation_function.TransformationFunction]
        ] = None
        self._model_dependent_transformation_functions_execution_graph: list[
            list[transformation_function.TransformationFunction]
        ] = None
        self._sql_client = None

        self._rest_client_engine = None
        self._init_rest_client: bool | None = None
        self._init_sql_client: bool | None = None
        self._default_client: Literal["rest", "sql"] | None = None
        self._return_feature_value_handlers: dict[str, Callable] = {}
        self._feature_to_handle_if_rest: set[str] | None = None
        self._feature_to_handle_if_sql: set[str] | None = None
        self._valid_serving_keys: set[str] = set()
        self._serving_initialized: bool = False
        self._parent_feature_groups: list[FeatureGroup] = []
        self.__all_features_on_demand: bool | None = None
        self.__all_feature_groups_online: bool | None = None
        self._feature_view_logging_enabled: bool = False
        self._skip_feature_decoding_fg_ids = skip_feature_decoding_fg_ids or set()
        self._fetch_inference_helpers_for_transformations: bool = False

    def init_serving(
        self,
        entity: feature_view.FeatureView,
        training_dataset_version: int,
        external: bool | None = None,
        inference_helper_columns: bool = False,
        options: dict[str, Any] | None = None,
        init_sql_client: bool | None = None,
        init_rest_client: bool = False,
        reset_rest_client: bool = False,
        config_rest_client: dict[str, Any] | None = None,
        default_client: Literal["rest", "sql"] | None = None,
    ):
        self._training_dataset_version = training_dataset_version

        self._parent_feature_groups = entity.get_parent_feature_groups().accessible

        if not (self._all_feature_groups_online or self._all_features_on_demand):
            raise exceptions.FeatureStoreException(
                "Feature vector retrieval is only available for feature view generated by online enabled feature groups or for feature views that contain only on-demand features. "
                f"Feature group(s) {[f'{fg.name} version : {fg.version}' for fg in self._parent_feature_groups if not fg.online_enabled]} are not online enabled."
            )
        if not self._all_feature_groups_online and self._all_features_on_demand:
            warnings.warn(
                "Features will be computed on-demand during vector retrieval because the feature view includes only offline feature groups.",
                stacklevel=1,
            )

        self._feature_view_logging_enabled = entity.logging_enabled
        self._root_feature_group = entity.query._left_feature_group
        if options is not None:
            reset_rest_client = reset_rest_client or options.get(
                self.RESET_REST_CLIENT_OPTIONS_KEY, False
            )
            if config_rest_client is None:
                config_rest_client = options.get(
                    self.REST_CLIENT_CONFIG_OPTIONS_KEY,
                    None,
                )
            if default_client is None:
                default_client = (
                    options.pop(self.DEFAULT_CLIENT_KEY, self.DEFAULT_SQL_CLIENT)
                    if isinstance(options, dict)
                    else self.DEFAULT_SQL_CLIENT
                )

        self._set_default_client(
            init_rest_client=init_rest_client,
            init_sql_client=init_sql_client,
            default_client=default_client,
        )

        if external is None:
            external = client._is_external()
        # `init_prepared_statement` should be the last because other initialisations
        # has to be done successfully before it is able to fetch feature vectors.
        self.init_transformation(entity)
        self.set_return_feature_value_handlers(features=entity.features)

        if self._init_rest_client and self.__all_feature_groups_online:
            self.setup_rest_client_and_engine(
                entity=entity,
                config_rest_client=config_rest_client,
                reset_rest_client=reset_rest_client,
            )

        if self._init_sql_client and self.__all_feature_groups_online:
            self.setup_sql_client(
                entity=entity,
                external=external,
                inference_helper_columns=inference_helper_columns,
                options=options,
            )
        self._serving_initialized = True

    def init_batch_scoring(
        self,
        entity: feature_view.FeatureView | training_dataset.TrainingDataset,
        training_dataset_version: int,
    ):
        self._training_dataset_version = training_dataset_version
        self.init_transformation(entity)
        self._serving_initialized = True

    def init_transformation(
        self,
        entity: feature_view.FeatureView,
    ):
        # attach transformation functions
        model_dependent_transformations = tf_engine_mod.TransformationFunctionEngine.get_ready_to_use_transformation_fns(
            entity,
            self._training_dataset_version,
        )

        # Filter out model-dependent transformation functions that use label features. Only the first label feature is checked since a transformation function using label feature can only contain label features.
        self._model_dependent_transformation_functions = [
            tf
            for tf in model_dependent_transformations
            if tf.hopsworks_udf.transformation_features[0] not in entity.labels
        ]

        self._on_demand_transformation_functions = []

        for feature in entity.features:
            if (
                feature.on_demand_transformation_function
                and feature.on_demand_transformation_function
                not in self._on_demand_transformation_functions
            ):
                self._on_demand_transformation_functions.append(
                    feature.on_demand_transformation_function
                )

        self._on_demand_feature_names = [
            feature.name
            for feature in entity.features
            if feature.on_demand_transformation_function
        ]

        self._fetch_inference_helpers_for_transformations = (
            self._requires_inference_helpers_for_transformations()
        )
        # Rebuild the transformation DAG for on-demand and model-dependent transformation functions.
        # This is necessary because the transformation functions will be updated with required statistics.
        self._on_demand_transformation_functions_execution_graph = tf_engine_mod.TransformationFunctionEngine.build_transformation_function_execution_graph(
            self._on_demand_transformation_functions
        )
        self._model_dependent_transformation_functions_execution_graph = tf_engine_mod.TransformationFunctionEngine.build_transformation_function_execution_graph(
            self._model_dependent_transformation_functions
        )

    def _requires_inference_helpers_for_transformations(self) -> bool:
        """Check if any on-demand transformation requires inference helper columns.

        Returns True if on-demand transformation functions exist and any of them
        use features that are marked as inference helper columns.
        """
        if (
            not self._on_demand_transformation_functions
            or not self._inference_helper_col_name
        ):
            return False

        inference_helper_set = set(self._inference_helper_col_name)

        for tf in self._on_demand_transformation_functions:
            prefix = tf.hopsworks_udf.feature_name_prefix or ""
            for feature in tf.hopsworks_udf.unprefixed_transformation_features:
                # Check both prefixed and unprefixed names
                prefixed = prefix + feature
                if prefixed in inference_helper_set or feature in inference_helper_set:
                    return True
        return False

    def setup_sql_client(
        self,
        entity: feature_view.FeatureView | training_dataset.TrainingDataset,
        external: bool,
        inference_helper_columns: bool,
        options: dict[str, Any] | None = None,
    ) -> None:
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Initialising Online Store SQL client")
        self._sql_client = online_store_sql_engine.OnlineStoreSqlClient(
            feature_store_id=self._feature_store_id,
            skip_fg_ids=self._skip_fg_ids,
            serving_keys=self.serving_keys,
            external=external,
        )
        self.sql_client.init_prepared_statements(
            entity,
            inference_helper_columns,
            with_logging_meta_data=self._feature_view_logging_enabled,
            feature_vector_with_inference_helpers=self._fetch_inference_helpers_for_transformations,
        )
        self.sql_client.init_async_mysql_connection(options=options)

    def setup_rest_client_and_engine(
        self,
        entity: feature_view.FeatureView | training_dataset.TrainingDataset,
        config_rest_client: dict[str, Any] | None = None,
        reset_rest_client: bool = False,
    ):
        # naming is off here, but it avoids confusion with the argument init_rest_client
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Initialising Online Store REST client")
        self._rest_client_engine = (
            online_store_rest_client_engine.OnlineStoreRestClientEngine(
                feature_store_name=self._feature_store_name,
                feature_view_name=entity.name,
                feature_view_version=entity.version,
                features=entity.features,
            )
        )
        # This logic needs to move to the above engine init
        online_store_rest_client.init_or_reset_online_store_rest_client(
            optional_config=config_rest_client,
            reset_client=reset_rest_client,
        )

    def check_missing_request_parameters(
        self, features: dict[str, Any], request_parameters: dict[str, Any]
    ):
        """Check if any request parameters required for computing on-demand features are missing.

        Parameters:
            feature_vector: `Dict[str, Any]`. The feature vector used to compute on-demand features.
            request_parameters: Request parameters required by on-demand transformation functions to compute on-demand features present in the feature view.
        """
        request_parameters = request_parameters if request_parameters else {}
        available_parameters = set((features | request_parameters).keys())
        missing_request_parameters_features = {}

        for on_demand_transformation in self._on_demand_transformation_functions:
            feature_name_prefix = (
                on_demand_transformation.hopsworks_udf.feature_name_prefix
            )

            transformation_features = set(
                on_demand_transformation.hopsworks_udf.transformation_features
            )
            unprefixed_features = set(
                on_demand_transformation.hopsworks_udf.unprefixed_transformation_features
            )
            # Prefixed feature names for request parameters that are not available in their unprefixed form as request-parameters.
            unprefixed_missing_features = {
                feature_name_prefix + feature_name
                if feature_name_prefix
                else feature_name
                for feature_name in unprefixed_features - available_parameters
            }

            # Prefixed feature names for features that are not available in the in their prefixed form as request-parameters.
            prefixed_missing_features = transformation_features - available_parameters

            # Get Missing request parameters: These are will include request parameters that are not provided in their unprefixed or prefixed form.
            missing_request_parameter = prefixed_missing_features.intersection(
                unprefixed_missing_features
            )

            if missing_request_parameter:
                missing_request_parameters_features.update(
                    {
                        on_demand_feature: sorted(missing_request_parameter)
                        for on_demand_feature in on_demand_transformation.hopsworks_udf.output_column_names
                    }
                )

        if missing_request_parameters_features:
            error = "Missing Request parameters to compute the following the on-demand Features:\n"
            for (
                feature,
                missing_request_parameter,
            ) in missing_request_parameters_features.items():
                missing_request_parameter = "', '".join(missing_request_parameter)
                error += f"On-Demand Feature '{feature}' requires features '{missing_request_parameter}'\n"
            error += (
                "Possible reasons: "
                "1. There is no match in the given entry."
                " Please check if the entry exists in the online feature store"
                " or provide the feature as passed_feature. "
                f"2. Required entries [{', '.join(self.required_serving_keys)}] or "
                f"[{', '.join({sk.feature_name for sk in self._serving_keys})}] are not provided."
            )
            raise exceptions.FeatureStoreException(error)

    def get_feature_vector(
        self,
        entry: dict[str, Any],
        return_type: Literal["list", "numpy", "pandas", "polars"],
        passed_features: dict[str, Any] | None = None,
        vector_db_features: dict[str, Any] | None = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
        transform: bool = True,
        on_demand_features: bool | None = True,
        request_parameters: dict[str, Any] | None = None,
        transformation_context: dict[str, Any] = None,
        logging_data: bool = False,
        n_processes: int = None,
    ) -> pd.DataFrame | pl.DataFrame | np.ndarray | list[Any] | dict[str, Any]:
        """Assembles serving vector from online feature store."""
        online_client_choice = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )

        # Create logging meta data object if required and populate it during the function.
        logging_meta_data = (
            LoggingMetaData()
            if self._feature_view_logging_enabled and logging_data
            else None
        )

        # Make a copy of request parameters to be stored in logging meta data since it might be updated below.
        request_parameters_copy = (
            request_parameters.copy() if request_parameters else {}
        )

        # Adding values in entry to request_parameters if it is not explicitly mentioned so that on-demand feature can be computed using the values in entry if they are not present in retrieved feature vector. This happens when no features can be retrieved from the feature view since the serving key is not yet there.
        if request_parameters and entry:
            for key, value in entry.items():
                request_parameters.setdefault(key, value)

        rondb_entry = self.validate_entry(
            entry=entry,
            allow_missing=allow_missing,
            passed_features=passed_features,
            vector_db_features=vector_db_features,
        )
        if len(rondb_entry) == 0:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Empty entry for rondb, skipping fetching.")
            serving_vector = {}  # updated below with vector_db_features and passed_features
        elif online_client_choice == self.DEFAULT_REST_CLIENT:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("get_feature_vector Online REST client")
            serving_vector = self.rest_client_engine.get_single_feature_vector(
                rondb_entry,
                drop_missing=not allow_missing,
                return_type=self.rest_client_engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
        else:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("get_feature_vector Online SQL client")
            serving_vector = self.sql_client.get_single_feature_vector(
                rondb_entry,
                logging_data=logging_data,
                feature_vector_with_inference_helpers=self._fetch_inference_helpers_for_transformations,
            )

        self._raise_transformation_warnings(
            transform=transform, on_demand_features=on_demand_features
        )

        vector = self.assemble_feature_vector(
            result_dict=serving_vector,
            passed_values=passed_features or {},
            vector_db_result=vector_db_features or {},
            allow_missing=allow_missing,
            client=online_client_choice,
            transform=transform,
            on_demand_features=on_demand_features,
            request_parameters=request_parameters,
            transformation_context=transformation_context,
            logging_meta_data=logging_meta_data,
            n_processes=n_processes,
        )
        if logging_meta_data is not None:
            logging_meta_data.serving_keys.append(entry)
            logging_meta_data.request_parameters.append(request_parameters_copy or {})
            logging_meta_data.event_time.append(
                [
                    serving_vector.get(
                        self._root_feature_group.event_time, datetime.now()
                    )
                ]  # Use current time if event_time is not present in the retrieved vector. For online inference the features will not be the feature group.
            )
            logging_meta_data.inference_helper.append(
                [
                    serving_vector.get(col, None)
                    for col in self._inference_helper_col_name
                ]
            )

        return self.handle_feature_vector_return_type(
            vector,
            batch=False,
            inference_helper=False,
            return_type=return_type,
            transform=transform,
            on_demand_feature=on_demand_features,
            logging_meta_data=logging_meta_data,
        )

    def get_feature_vectors(
        self,
        entries: list[dict[str, Any]],
        return_type: Literal["list", "numpy", "pandas", "polars"] | None = None,
        passed_features: list[dict[str, Any]] | None = None,
        vector_db_features: list[dict[str, Any]] | None = None,
        request_parameters: list[dict[str, Any]] | None = None,
        allow_missing: bool = False,
        force_rest_client: bool = False,
        force_sql_client: bool = False,
        transform: bool = True,
        on_demand_features: bool | None = True,
        transformation_context: dict[str, Any] = None,
        logging_data: bool = False,
        n_processes: int = None,
    ) -> pd.DataFrame | pl.DataFrame | np.ndarray | list[Any] | list[dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        if passed_features is None:
            passed_features = []
        # Assertions on passed_features and vector_db_features
        assert (
            passed_features is None
            or len(passed_features) == 0
            or len(passed_features) == len(entries)
        ), (
            "Passed features should be None, empty or have the same length as the entries"
        )
        assert (
            vector_db_features is None
            or len(vector_db_features) == 0
            or len(vector_db_features) == len(entries)
        ), (
            "Vector DB features should be None, empty or have the same length as the entries"
        )
        assert (
            request_parameters is None
            or len(request_parameters) == 0
            or isinstance(request_parameters, dict)
            or not entries
            or len(request_parameters) == len(entries)
        ), (
            "Request Parameters should be a Dictionary, None, empty or have the same length as the entries if they are not None or empty."
        )

        # Create logging meta data object if required and populate it during the function.
        logging_meta_data = (
            LoggingMetaData()
            if self._feature_view_logging_enabled and logging_data
            else None
        )

        request_parameters_copy = (
            deepcopy(request_parameters) if request_parameters else None
        )
        # Adding values in entry to request_parameters if it is not explicitly mentioned so that on-demand feature can be computed using the values in entry if they are not present in retrieved feature vector.
        if request_parameters and entries:
            if isinstance(request_parameters, list) and len(entries) == len(
                request_parameters
            ):
                for idx, entry in enumerate(entries):
                    for key, value in entry.items():
                        request_parameters[idx].setdefault(key, value)
            elif isinstance(request_parameters, dict) and len(entries) == 1:
                for key, value in entries[0].items():
                    request_parameters.setdefault(key, value)

        online_client_choice = self.which_client_and_ensure_initialised(
            force_rest_client=force_rest_client, force_sql_client=force_sql_client
        )
        rondb_entries = []
        skipped_empty_entries = []

        if not entries:
            entries = (
                [[] * len(request_parameters)]
                if isinstance(request_parameters, list)
                else [[]]
            )

        self._raise_transformation_warnings(
            transform=transform, on_demand_features=on_demand_features
        )

        for (idx, entry), passed, vector_features in itertools.zip_longest(
            enumerate(entries),
            passed_features,
            vector_db_features,
        ):
            rondb_entry = self.validate_entry(
                entry=entry,
                allow_missing=allow_missing,
                passed_features=passed,
                vector_db_features=vector_features,
            )
            if len(rondb_entry) != 0:
                rondb_entries.append(rondb_entry)
            else:
                skipped_empty_entries.append(idx)

        if online_client_choice == self.DEFAULT_REST_CLIENT and len(rondb_entries) > 0:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("get_batch_feature_vector Online REST client")
            batch_results = self.rest_client_engine.get_batch_feature_vectors(
                entries=rondb_entries,
                drop_missing=not allow_missing,
                return_type=self.rest_client_engine.RETURN_TYPE_FEATURE_VALUE_DICT,
            )
        elif len(rondb_entries) > 0:
            # get result row
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("get_batch_feature_vectors through SQL client")
            batch_results, _ = self.sql_client.get_batch_feature_vectors(
                rondb_entries,
                logging_data=logging_data,
                feature_vector_with_inference_helpers=self._fetch_inference_helpers_for_transformations,
            )
        else:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Empty entries for rondb, skipping fetching.")
            batch_results = []

        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Assembling feature vectors from batch results")
        next_skipped = (
            skipped_empty_entries.pop(0) if len(skipped_empty_entries) > 0 else None
        )
        vectors = []

        # If request parameter is a dictionary then copy it to list with the same length as that of entires
        request_parameters = (
            [request_parameters] * len(entries)
            if isinstance(request_parameters, dict)
            else request_parameters
        )

        if logging_meta_data is not None:
            request_parameters_copy = (
                [request_parameters_copy] * len(entries)
                if isinstance(request_parameters_copy, dict)
                else request_parameters_copy
            )
            logging_meta_data.serving_keys.extend(entries)
            logging_meta_data.request_parameters.extend(request_parameters_copy)
        for (
            idx,
            passed_values,
            vector_db_result,
            request_parameter,
        ) in itertools.zip_longest(
            range(len(entries)),
            passed_features or [],
            vector_db_features or [],
            request_parameters or [],
            fillvalue=None,
        ):
            if next_skipped == idx:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug("Entry %d was skipped, setting to empty dict.", idx)
                next_skipped = (
                    skipped_empty_entries.pop(0)
                    if len(skipped_empty_entries) > 0
                    else None
                )
                result_dict = {}
            else:
                result_dict = batch_results.pop(0)

            vector = self.assemble_feature_vector(
                result_dict=result_dict,
                passed_values=passed_values,
                vector_db_result=vector_db_result,
                allow_missing=allow_missing,
                client=online_client_choice,
                transform=transform,
                on_demand_features=on_demand_features,
                request_parameters=request_parameter,
                transformation_context=transformation_context,
                logging_meta_data=logging_meta_data,
                n_processes=n_processes,
            )

            if logging_meta_data is not None:
                logging_meta_data.event_time.append(
                    [
                        result_dict.get(
                            self._root_feature_group.event_time, datetime.now()
                        )
                    ]  # Use current time if event_time is not present in the retrieved vector. For online inference the features will not be the feature group.
                )
                logging_meta_data.inference_helper.append(
                    [
                        result_dict.get(col, None)
                        for col in self._inference_helper_col_name
                    ]
                )

            if vector is not None:
                vectors.append(vector)

        return self.handle_feature_vector_return_type(
            vectors,
            batch=True,
            inference_helper=False,
            return_type=return_type,
            transform=transform,
            on_demand_feature=transform,
            logging_meta_data=logging_meta_data,
        )

    def assemble_feature_vector(
        self,
        result_dict: dict[str, Any],
        passed_values: dict[str, Any] | None,
        vector_db_result: dict[str, Any] | None,
        allow_missing: bool,
        client: Literal["rest", "sql"],
        transform: bool,
        on_demand_features: bool,
        request_parameters: dict[str, Any] | None = None,
        transformation_context: dict[str, Any] = None,
        logging_meta_data: LoggingMetaData = None,
        n_processes: int = None,
    ) -> list[Any] | None:
        """Assembles serving vector from online feature store."""
        # Errors in batch requests are returned as None values
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Assembling serving vector: %s", result_dict)
        if result_dict is None:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Found null result, setting to empty dict.")
            result_dict = {}
        if vector_db_result is not None and len(vector_db_result) > 0:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Updating with vector_db features: %s", vector_db_result)
            result_dict.update(vector_db_result)
        if passed_values is not None and len(passed_values) > 0:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Updating with passed features: %s", passed_values)
            result_dict.update(passed_values)

        missing_features = (
            set(self.feature_vector_col_name)
            .difference(result_dict.keys())
            .difference(self._on_demand_feature_names)
        )

        # for backward compatibility, before 3.4, if result is empty,
        # instead of throwing error, it skips the result
        # Maybe we drop this behaviour for 4.0
        if (
            len(result_dict) == 0
            and not allow_missing
            and not self._all_features_on_demand
        ):
            return None

        if not allow_missing and len(missing_features) > 0:
            raise exceptions.FeatureStoreException(
                f"Feature(s) {str(missing_features)} is missing from vector."
                "Possible reasons: "
                "1. There is no match in the given entry."
                " Please check if the entry exists in the online feature store"
                " or provide the feature as passed_feature. "
                f"2. Required entries [{', '.join(self.required_serving_keys)}] are not provided."
            )
        if len(self.return_feature_value_handlers) > 0:
            self.apply_return_value_handlers(result_dict, client=client)
        feature_dict, encoded_feature_dict = result_dict, result_dict
        if (
            len(self.model_dependent_transformation_functions) > 0
            or len(self.on_demand_transformation_functions) > 0
        ):
            feature_dict, encoded_feature_dict = self.apply_transformation(
                result_dict.copy(),
                request_parameters or {},
                transformation_context,
                transform=transform,
                on_demand_features=on_demand_features,
                logging_meta_data=logging_meta_data,
                n_processes=n_processes,
            )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Assembled and transformed dict feature vector: %s", result_dict
            )
        if transform:
            return [
                encoded_feature_dict.get(fname, None)
                for fname in self.transformed_feature_vector_col_name
            ]
        if on_demand_features:
            return [
                feature_dict.get(fname, None)
                for fname in self._on_demand_feature_vector_col_name
            ]
        return [
            result_dict.get(fname)
            for fname in self._untransformed_feature_vector_col_name
        ]

    def _validate_input_features(
        self,
        feature_vectors: list[Any] | list[list[Any]] | pd.DataFrame | pl.DataFrame,
        on_demand_features: bool = False,
    ) -> None:
        """Validate if an feature-vector provided contain all required features.

        Parameters:
            feature_vectors: `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`. The feature vectors to be converted.
            on_demand_features : `bool`. Specify if on-demand features provided in the input feature vector.

        """
        required_features = (
            set(self._untransformed_feature_vector_col_name)
            if not on_demand_features
            else set(self._on_demand_feature_vector_col_name)
        )

        if isinstance(
            feature_vectors, pd.DataFrame or isinstance(feature_vectors, pl.DataFrame)
        ):
            missing_features = required_features - set(feature_vectors.columns)
            if missing_features:
                raise exceptions.FeatureStoreException(
                    f"The input feature vector is missing the following required features: `{'`, `'.join(missing_features)}`. Please include these features in the feature vector."
                )
        else:
            if isinstance(feature_vectors, list):
                if feature_vectors and all(
                    isinstance(feature_vector, list)
                    for feature_vector in feature_vectors
                ):
                    if any(
                        len(feature_vector) != len(required_features)
                        for feature_vector in feature_vectors
                    ):
                        raise exceptions.FeatureStoreException(
                            f"Input feature vector is missing required features. Please ensure the following features are included: `{'`, `'.join(required_features)}`."
                        )
                else:
                    if len(feature_vectors) != len(required_features):
                        raise exceptions.FeatureStoreException(
                            f"Input feature vector is missing required features. Please ensure the following features are included: '{', '.join(required_features)}'."
                        )

    def _check_feature_vectors_type_and_convert_to_dict(
        self,
        feature_vectors: list[Any] | list[list[Any]] | pd.DataFrame | pl.DataFrame,
        on_demand_features: bool = False,
    ) -> tuple[dict[str, Any], Literal["pandas", "polars", "list"]]:
        """Function that converts an input feature vector into a list of dictionaries.

        Parameters:
            feature_vectors: `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`. The feature vectors to be converted.
            on_demand_features : `bool`. Specify if on-demand features provided in the input feature vector.

        Returns:
            `Tuple[Dict[str, Any], Literal["pandas", "polars", "list"]]`: A tuple that contains the feature vector as a dictionary and a string denoting the data type of the input feature vector.

        """
        if isinstance(feature_vectors, pd.DataFrame):
            return_type = "pandas"
            feature_vectors = feature_vectors.to_dict(orient="records")

        elif HAS_POLARS and isinstance(feature_vectors, pl.DataFrame):
            return_type = "polars"
            feature_vectors = feature_vectors.to_pandas().to_dict(orient="records")

        elif isinstance(feature_vectors, list):
            if feature_vectors and all(
                isinstance(feature_vector, list) for feature_vector in feature_vectors
            ):
                return_type = "list"
                feature_vectors = [
                    self.get_untransformed_features_map(
                        feature_vector, on_demand_features=on_demand_features
                    )
                    for feature_vector in feature_vectors
                ]

            else:
                return_type = "list"
                feature_vectors = [
                    self.get_untransformed_features_map(
                        feature_vectors, on_demand_features=on_demand_features
                    )
                ]

        else:
            raise exceptions.FeatureStoreException(
                "Unsupported input type for feature vector. Supported types are `List`, `pandas.DataFrame`, `polars.DataFrame`"
            )
        return feature_vectors, return_type

    def transform(
        self,
        feature_vectors: list[Any] | list[list[Any]] | pd.DataFrame | pl.DataFrame,
        transformation_context: dict[str, Any] = None,
        return_type: Literal["list", "numpy", "pandas", "polars"] = None,
        n_processes: int = None,
    ) -> list[Any] | list[list[Any]] | pd.DataFrame | pl.DataFrame:
        """Applies model dependent transformation on the provided feature vector.

        Parameters:
            feature_vectors: `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`. The feature vectors to be transformed using attached model-dependent transformations.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
            return_type: `"list"`, `"pandas"`, `"polars"` or `"numpy"`. Defaults to the same type as the input feature vector.
            n_processes: Number of processes to use for parallel execution of transformation functions.
                If not provided, the number of processes will be set to the number of available CPU cores.
                This parameter is only applicable when the engine is `python`.

        Returns:
            `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`: The transformed feature vector.
        """
        if not self._model_dependent_transformation_functions:
            warnings.warn(
                "Feature view does not have any attached model-dependent transformations. Returning input feature vectors.",
                stacklevel=0,
            )
            return feature_vectors

        feature_vectors, default_return_type = (
            self._check_feature_vectors_type_and_convert_to_dict(
                feature_vectors, on_demand_features=True
            )
        )

        return_type = return_type if return_type else default_return_type

        transformed_feature_vectors = []
        for feature_vector in feature_vectors:
            transformed_feature_vector = tf_engine_mod.TransformationFunctionEngine.apply_transformation_functions(
                execution_graph=self._model_dependent_transformation_functions_execution_graph,
                data=feature_vector,
                online=True,
                transformation_context=transformation_context,
                n_processes=n_processes,
                expected_features=set(self.transformed_feature_vector_col_name),
            )
            transformed_feature_vectors.append(
                [
                    transformed_feature_vector.get(fname, None)
                    for fname in self.transformed_feature_vector_col_name
                ]
            )

        if len(transformed_feature_vectors) == 1:
            batch = False
            transformed_feature_vectors = transformed_feature_vectors[0]
        else:
            batch = True

        return self.handle_feature_vector_return_type(
            transformed_feature_vectors,
            batch=batch,
            inference_helper=False,
            return_type=return_type,
            transform=True,
        )

    def compute_on_demand_features(
        self,
        feature_vectors: list[Any]
        | list[list[Any]]
        | pd.DataFrame
        | pl.DataFrame
        | None = None,
        request_parameters: list[dict[str, Any]] | dict[str, Any] = None,
        transformation_context: dict[str, Any] = None,
        return_type: Literal["list", "numpy", "pandas", "polars"] = None,
        n_processes: int = None,
    ):
        """Function computes on-demand features present in the feature view.

        Parameters:
            feature_vector: `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`. The feature vector to be transformed.
            request_parameters: Request parameters required by on-demand transformation functions to compute on-demand features present in the feature view.
                These parameters take **highest priority** when resolving feature values - if a key exists in both `request_parameters` and the `feature_vector`, the value from `request_parameters` is used.
            transformation_context: `Dict[str, Any]` A dictionary mapping variable names to objects that will be provided as contextual information to the transformation function at runtime.
                The `context` variable must be explicitly defined as parameters in the transformation function for these to be accessible during execution. If no context variables are provided, this parameter defaults to `None`.
            return_type: `"list"`, `"pandas"`, `"polars"` or `"numpy"`. Defaults to the same type as the input feature vector.
            n_processes: Number of processes to use for parallel execution of transformation functions.
                If not provided, the number of processes will be set to the number of available CPU cores.
                This parameter is only applicable when the engine is `python`.

        Returns:
            `Union[List[Any], List[List[Any]], pd.DataFrame, pl.DataFrame]`: The feature vector that contains all on-demand features in the feature view.
        """
        if not self._on_demand_transformation_functions:
            warnings.warn(
                "Feature view does not have any on-demand features. Returning input feature vectors.",
                stacklevel=1,
            )
            return feature_vectors

        feature_vectors = [] if feature_vectors is None else feature_vectors

        request_parameters = request_parameters if request_parameters else {}
        # Convert feature vectors to dictionary
        feature_vectors, default_return_type = (
            self._check_feature_vectors_type_and_convert_to_dict(feature_vectors)
        )

        return_type = return_type if return_type else default_return_type

        # Check if all request parameters are provided
        # If request parameter is a dictionary then copy it to list with the same length as that of entires
        request_parameters = (
            [request_parameters] * len(feature_vectors)
            if isinstance(request_parameters, dict)
            else request_parameters
        )
        self.check_missing_request_parameters(
            features=feature_vectors[0], request_parameters=request_parameters[0]
        )
        on_demand_feature_vectors = []
        for feature_vector, request_parameter in zip(
            feature_vectors, request_parameters
        ):
            on_demand_feature_vector = tf_engine_mod.TransformationFunctionEngine.apply_transformation_functions(
                execution_graph=self._on_demand_transformation_functions_execution_graph,
                data=feature_vector,
                online=True,
                transformation_context=transformation_context,
                request_parameters=request_parameter,
                expected_features=set(self._on_demand_feature_vector_col_name),
                n_processes=n_processes,
            )
            on_demand_feature_vectors.append(
                [
                    on_demand_feature_vector.get(fname, None)
                    for fname in self._on_demand_feature_vector_col_name
                ]
            )

        if len(on_demand_feature_vectors) == 1:
            batch = False
            on_demand_feature_vectors = on_demand_feature_vectors[0]
        else:
            batch = True

        return self.handle_feature_vector_return_type(
            on_demand_feature_vectors,
            batch=batch,
            inference_helper=False,
            return_type=return_type,
            transform=False,
            on_demand_feature=True,
        )

    def get_untransformed_features_map(
        self, features: list[Any], on_demand_features: bool = False
    ) -> dict[str, Any]:
        """Function that accepts a feature vectors as a list and returns the untransformed features as a dict that maps feature names to their values.

        Parameters:
            features : `List[Any]`. List of feature vectors.
            on_demand_features : `bool`. Specify if on-demand features provided in the input feature vector.

        Returns:
            `Dict[str, Any]` : Dictionary mapping features name to values.
        """
        if on_demand_features:
            return dict(zip(self._on_demand_feature_vector_col_name, features))
        return dict(zip(self._untransformed_feature_vector_col_name, features))

    def handle_feature_vector_return_type(
        self,
        feature_vectorz: list[Any]
        | list[list[Any]]
        | dict[str, Any]
        | list[dict[str, Any]],
        batch: bool,
        inference_helper: bool,
        return_type: Literal["list", "dict", "numpy", "pandas", "polars"],
        transform: bool = False,
        on_demand_feature: bool = False,
        logging_meta_data: dict[str, Any] | None = None,
    ) -> (
        pd.DataFrame
        | pl.DataFrame
        | np.ndarray
        | list[Any]
        | list[list[Any]]
        | dict[str, Any]
        | list[dict[str, Any]]
    ):
        if transform:
            column_names = self.transformed_feature_vector_col_name
        else:
            if on_demand_feature:
                column_names = self._on_demand_feature_vector_col_name
            else:
                column_names = self._untransformed_feature_vector_col_name

        # Only get-feature-vector and get-feature-vectors can return list or numpy
        if return_type.lower() == "list" and not inference_helper:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as value list")
            feature_vector = feature_vectorz
        elif return_type.lower() == "numpy" and not inference_helper:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as numpy array")
            if not HAS_NUMPY:
                raise ModuleNotFoundError(numpy_not_installed_message)
            feature_vector = np.array(feature_vectorz)
        # Only inference helper can return dict
        elif return_type.lower() == "dict" and inference_helper:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as dictionary")
            feature_vector = feature_vectorz
        # Both can return pandas and polars
        elif return_type.lower() == "pandas":
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as pandas dataframe")
            if batch and inference_helper:
                feature_vector = pd.DataFrame(feature_vectorz)
            elif inference_helper:
                feature_vector = pd.DataFrame([feature_vectorz])
            elif batch:
                feature_vector = pd.DataFrame(feature_vectorz, columns=column_names)
            else:
                feature_vector = pd.DataFrame([feature_vectorz], columns=column_names)
        elif return_type.lower() == "polars":
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Returning feature vector as polars dataframe")
            if not HAS_POLARS:
                raise ModuleNotFoundError(polars_not_installed_message)
            feature_vector = pl.DataFrame(
                feature_vectorz if batch else [feature_vectorz],
                schema=column_names if not inference_helper else None,
                orient="row",
            )
        else:
            raise ValueError(
                f"""Unknown return type. Supported return types are {"'list', 'numpy'" if not inference_helper else "'dict'"}, 'polars' and 'pandas''"""
            )
        if logging_meta_data:
            extended_type = create_extended_type(type(feature_vector))
            feature_vector = extended_type(feature_vector)
            feature_vector.hopsworks_logging_metadata = logging_meta_data
        return feature_vector

    def get_inference_helper(
        self,
        entry: dict[str, Any],
        return_type: Literal["dict", "pandas", "polars"],
        force_rest_client: bool,
        force_sql_client: bool,
    ) -> pd.DataFrame | pl.DataFrame | dict[str, Any]:
        """Assembles serving vector from online feature store."""
        default_client = self.which_client_and_ensure_initialised(
            force_rest_client, force_sql_client
        )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Retrieve inference helper values for single entry via {default_client.upper()} client."
            )
            _logger.debug(f"entry: {entry} as return type: {return_type}")
        if default_client == self.DEFAULT_REST_CLIENT:
            return self.handle_feature_vector_return_type(
                self.rest_client_engine.get_single_feature_vector(
                    entry,
                    return_type=self.rest_client_engine.RETURN_TYPE_FEATURE_VALUE_DICT,
                    inference_helpers_only=True,
                ),
                batch=False,
                inference_helper=True,
                return_type=return_type,
                transform=False,
                on_demand_feature=False,
            )
        return self.handle_feature_vector_return_type(
            self.sql_client.get_inference_helper_vector(entry),
            batch=False,
            inference_helper=True,
            return_type=return_type,
            transform=False,
            on_demand_feature=False,
        )

    def get_inference_helpers(
        self,
        entries: list[dict[str, Any]],
        return_type: Literal["dict", "pandas", "polars"],
        force_rest_client: bool,
        force_sql_client: bool,
    ) -> pd.DataFrame | pl.DataFrame | list[dict[str, Any]]:
        """Assembles serving vector from online feature store."""
        default_client = self.which_client_and_ensure_initialised(
            force_rest_client, force_sql_client
        )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Retrieve inference helper values for batch entries via {default_client.upper()} client."
            )
            _logger.debug(f"entries: {entries} as return type: {return_type}")

        if default_client == self.DEFAULT_REST_CLIENT:
            batch_results = self.rest_client_engine.get_batch_feature_vectors(
                entries,
                return_type=self.rest_client_engine.RETURN_TYPE_FEATURE_VALUE_DICT,
                inference_helpers_only=True,
            )
        else:
            batch_results, serving_keys = (
                self.sql_client.get_batch_inference_helper_vectors(entries)
            )
            # Batch retrieval of inference helper returns primary keys as well to enable merging of vectors retrieved from different feature groups
            # Filtering out the primary keys from the results
            batch_results = [
                {col: result.get(col, None) for col in self._inference_helper_col_name}
                for result in batch_results
            ]

        return self.handle_feature_vector_return_type(
            batch_results,
            batch=True,
            inference_helper=True,
            return_type=return_type,
            transform=False,
            on_demand_feature=False,
        )

    def which_client_and_ensure_initialised(
        self, force_rest_client: bool, force_sql_client: bool
    ) -> str:
        """Check if the requested client is initialised as well as deciding which client to use based on default.

        Parameters:
            force_rest_client: bool. user specified override to use rest_client.
            force_sql_client: bool. user specified override to use sql_client.

        Returns:
            An enum specifying the client to be used.
        """
        if force_rest_client and force_sql_client:
            raise ValueError(
                "force_rest_client and force_sql_client cannot be used at the same time."
            )

        # No override, use default client
        if not force_rest_client and not force_sql_client:
            return self.default_client

        if self._init_rest_client is False and self._init_sql_client is False:
            raise ValueError(
                "No client is initialised. Call `init_serving` with initsql_client or init_rest_client set to True before using it."
            )
        if force_sql_client and (self._init_sql_client is False):
            raise ValueError(
                "SQL Client is not initialised. Call `init_serving` with init_sql_client set to True before using it."
            )
        if force_sql_client:
            return self.DEFAULT_SQL_CLIENT

        if force_rest_client and (self._init_rest_client is False):
            raise ValueError(
                "RonDB Rest Client is not initialised. Call `init_serving` with init_rest_client set to True before using it."
            )
        if force_rest_client:
            return self.DEFAULT_REST_CLIENT
        return None

    def _set_default_client(
        self,
        init_rest_client: bool,
        init_sql_client: bool,
        default_client: str | None = None,
    ):
        if init_rest_client is False and init_sql_client is False:
            raise ValueError(
                "At least one of the clients should be initialised. Set init_sql_client or init_rest_client to True."
            )
        self._init_rest_client = init_rest_client
        self._init_sql_client = init_sql_client

        if init_rest_client is True and init_sql_client is True:
            self.default_client = default_client

        elif init_rest_client is True:
            self.default_client = self.DEFAULT_REST_CLIENT
        else:
            self.default_client = self.DEFAULT_SQL_CLIENT
            self._init_sql_client = True

    def parse_transformed_result(self, transformed_results, transformation_function):
        rows = {}
        if (
            transformation_function.hopsworks_udf.execution_mode.get_current_execution_mode(
                online=True
            )
            == UDFExecutionMode.PANDAS
        ):
            if isinstance(transformed_results, pd.Series):
                rows[transformed_results.name] = transformed_results.values[0]
            else:
                for col in transformed_results:
                    rows[col] = transformed_results[col].values[0]
        else:
            if isinstance(transformed_results, (tuple, list)):
                for index, result in enumerate(transformed_results):
                    rows[transformation_function.output_column_names[index]] = result
            else:
                rows[transformation_function.output_column_names[0]] = (
                    transformed_results
                )
        return rows

    def _raise_transformation_warnings(self, transform: bool, on_demand_features: bool):
        """Function that raises warnings based on the values of `transform` and `on_demand_features` parameters to let users know about the behavior of the function.

        Parameters:
            transform : `bool`. Specify if model-dependent transformations should be applied.
            on_demand_features : `bool`. Specify if on-demand features should be computed.
        """
        warn_on_demand_features = (
            not on_demand_features and self.on_demand_transformation_functions
        )
        warn_model_dependent_features = (
            not transform and self.model_dependent_transformation_functions
        )

        if transform and not on_demand_features:
            if _logger.isEnabledFor(logging.WARNING):
                _logger.warning(
                    "On-demand features are always returned when `transform=True`, regardless of `on_demand_features`. "
                    "To fetch an untransformed feature vector without on-demand features, set both `transform=False` and `on_demand_features=False`."
                )
        elif warn_on_demand_features and warn_model_dependent_features:
            if _logger.isEnabledFor(logging.INFO):
                _logger.info(
                    "Both `transform` and `on_demand_features` are set to False. Returning feature vector without on-demand features or model-dependent transformations."
                )
        elif warn_on_demand_features:
            if _logger.isEnabledFor(logging.INFO):
                _logger.info(
                    "On-demand features are not computed when `on_demand_features=False`. Returning feature vector without on-demand features."
                )
        elif warn_model_dependent_features and _logger.isEnabledFor(logging.INFO):
            _logger.info(
                "Model-dependent transformation functions are not applied when `transform=False`. Returning feature vector without model-dependent transformations."
            )

    def apply_transformation(
        self,
        row_dict: dict | pd.DataFrame,
        request_parameter: dict[str, Any],
        transformation_context: dict[str, Any] = None,
        transform: bool = True,
        on_demand_features: bool = True,
        logging_meta_data: LoggingMetaData = None,
        n_processes: int = None,
    ):
        """Function that applies both on-demand and model dependent transformation to the input dictonary.

        Returns:
            feature_vector: The untransformed feature vector with untransformed features.
            encoded_feature_dict: The transformed feature vector with transformed features.
        """
        feature_dict = row_dict
        encoded_feature_dict = None

        if transform or on_demand_features or logging_meta_data:
            # Check for any missing request parameters
            self.check_missing_request_parameters(
                features=row_dict, request_parameters=request_parameter
            )

            # Apply on-demand transformations
            feature_dict = tf_engine_mod.TransformationFunctionEngine.apply_transformation_functions(
                data=feature_dict,
                online=True,
                transformation_context=transformation_context,
                request_parameters=request_parameter,
                execution_graph=self._on_demand_transformation_functions_execution_graph,
                expected_features=set(self._on_demand_feature_vector_col_name),
                n_processes=n_processes,
            )
            if logging_meta_data:
                logging_meta_data.untransformed_features.append(
                    [
                        feature_dict.get(fname, None)
                        for fname in self._on_demand_feature_vector_col_name
                    ]
                )

        if transform or logging_meta_data:
            # Apply model dependent transformations
            encoded_feature_dict = tf_engine_mod.TransformationFunctionEngine.apply_transformation_functions(
                execution_graph=self._model_dependent_transformation_functions_execution_graph,
                data=feature_dict,
                online=True,
                transformation_context=transformation_context,
                expected_features=set(self.transformed_feature_vector_col_name),
                n_processes=n_processes,
            )
            if logging_meta_data:
                logging_meta_data.transformed_features.append(
                    [
                        encoded_feature_dict.get(fname, None)
                        for fname in self.transformed_feature_vector_col_name
                    ]
                )

        return feature_dict, encoded_feature_dict

    def apply_return_value_handlers(
        self, row_dict: dict[str, Any], client: Literal["rest", "sql"]
    ):
        if client == self.DEFAULT_REST_CLIENT:
            matching_keys = self.feature_to_handle_if_rest.intersection(row_dict.keys())
        else:
            matching_keys = set(self.feature_to_handle_if_sql).intersection(
                row_dict.keys()
            )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Applying return value handlers to : %s", matching_keys)
        for fname in matching_keys:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Applying return value handler to feature: %s", fname)
            row_dict[fname] = self.return_feature_value_handlers[fname](row_dict[fname])
        return row_dict

    def build_complex_feature_decoders(self) -> dict[str, Callable]:
        """Build decoders for complex features.

        Decodes feature values for features marked as complex (e.g., structs, arrays, maps) that are stored in the online store as Avro-encoded payloads (bytes or base64 strings).
        Values already provided as native Python objects (e.g., via passed_features or REST) are returned unchanged.
        Embedding vectors are already deserialized, but complex features stored in OpenSearch must be deserialized here.
        Timestamp conversion is handled separately.
        """
        if not HAS_AVRO:
            raise ModuleNotFoundError(avro_not_installed_message)

        complex_feature_schemas = {
            f.name: avro.io.DatumReader(
                avro.schema.parse(
                    f._feature_group._get_feature_avro_schema(
                        f.feature_group_feature_name
                    )
                )
            )
            for f in self._features
            if f.is_complex()
            and f.feature_group.id not in self._skip_feature_decoding_fg_ids
        }

        if len(complex_feature_schemas) == 0:
            return {}
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Building complex feature decoders corresponding to {complex_feature_schemas}."
            )
        if HAS_FAST_AVRO:
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug("Using fastavro for deserialization.")
            return {
                f_name: (
                    lambda feature_value, avro_schema=schema: (
                        schemaless_reader(
                            BytesIO(
                                feature_value
                                if isinstance(feature_value, bytes)
                                else b64decode(feature_value)
                            ),
                            avro_schema.writers_schema.to_json(),
                        )
                        # embedded features are deserialized already but not complex features stored in Opensearch
                        if (isinstance(feature_value, (bytes, str)))
                        else feature_value
                    )
                )
                for (f_name, schema) in complex_feature_schemas.items()
            }
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Fast Avro not found, using avro for deserialization.")
        return {
            f_name: (
                lambda feature_value, avro_schema=schema: avro_schema.read(
                    BinaryDecoder(
                        BytesIO(
                            feature_value
                            if isinstance(feature_value, bytes)
                            else b64decode(feature_value)
                        )
                    )
                )
                # embedded features are deserialized already but not complex features stored in Opensearch
                if (isinstance(feature_value, (str, bytes)))
                else feature_value
            )
            for (f_name, schema) in complex_feature_schemas.items()
        }

    def set_return_feature_value_handlers(
        self, features: list[tdf_mod.TrainingDatasetFeature]
    ):
        """Build a dictionary of functions to convert/deserialize/transform the feature values returned from RonDB Server.

        Re-using the current logic from the vector server means that we currently iterate over the feature vectors
        and values multiple times, as well as converting the feature values to a dictionary and then back to a list.
        """
        if (
            hasattr(self, "_return_feature_value_handlers")
            and len(self._return_feature_value_handlers) > 0
        ):
            return
        if len(features) == 0:
            return
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                f"Setting return feature value handlers for Feature View {self._feature_view_name},"
                f" version: {self._feature_view_version} in Feature Store {self._feature_store_name}."
            )
        self._return_feature_value_handlers.update(
            self.build_complex_feature_decoders()
        )
        for feature in features:
            if feature.type == "timestamp":
                self._return_feature_value_handlers[feature.name] = (
                    self._handle_timestamp_based_on_dtype
                )

    def _handle_timestamp_based_on_dtype(
        self, timestamp_value: str | int
    ) -> datetime | None:
        """Handle the timestamp based on the dtype which is returned.

        Currently timestamp which are in the database are returned as string. Whereas
        passed features which were given as datetime are returned as integer timestamp.

        Parameters:
            timestamp_value: The timestamp value to be handled, either as int or str.
        """
        if timestamp_value is None:
            return None
        if isinstance(timestamp_value, int):
            # in case of passed features, an event time could be passed as int timestamp
            return datetime.fromtimestamp(
                timestamp_value / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
        if isinstance(timestamp_value, str):
            # rest client returns timestamp as string
            return datetime.strptime(timestamp_value, self.SQL_TIMESTAMP_STRING_FORMAT)
        if isinstance(timestamp_value, datetime):
            # sql client returns already datetime object
            return timestamp_value
        raise ValueError(
            f"Timestamp value {timestamp_value} was expected to be of type int, str or datetime."
        )

    def validate_entry(
        self,
        entry: dict[str, Any],
        allow_missing: bool,
        passed_features: dict[str, Any],
        vector_db_features: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate the provided key, value pair in the entry.

        The following checks are performed:
            - Check that all keys in the entry correspond to a serving key.
            - Check that composite keys are provided together.

        If allow_missing is set to False:
            - check that for missing serving key in entry, the feature are provided
                via passed_features or vector_db_features.

        Keys relevant to vector_db are filtered out.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Checking if entry is None and all features in the feature view are on-demand."
            )
        if not entry:
            if self._all_features_on_demand:
                return {}
            raise exceptions.FeatureStoreException(
                "The required argument `entries` is missing. If the feature view includes only on-demand features, entries may be left empty or set to None."
            )
        if not self._all_feature_groups_online and self._all_features_on_demand:
            warnings.warn(
                "Ignoring passed entries as all parent feature groups of feature view are not online and all features will be computed on-demand.",
                stacklevel=1,
            )
            return {}
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug("Checking keys in entry are valid serving keys.")
        for key in entry:
            if key not in self.valid_serving_keys:
                raise exceptions.FeatureStoreException(
                    f"Provided key {key} is not a serving key. Required serving keys: {self.required_serving_keys}."
                )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Checking entry has either all or none of composite serving keys"
            )
        for composite_group in self.groups_of_composite_serving_keys.values():
            present_keys = [
                bool(sk_required in entry or sk_name in entry)
                for (sk_required, sk_name) in composite_group
            ]
            if not all(present_keys) and any(present_keys):
                raise exceptions.FeatureStoreException(
                    "Provide either all composite serving keys or none. "
                    f"Composite keys: {[prefix_key for (prefix_key, _) in composite_group]} or {[key for (_, key) in composite_group]} in entry {entry}."
                )

        if allow_missing is False:
            self.identify_missing_features_pre_fetch(
                entry=entry,
                passed_features=passed_features,
                vector_db_features=vector_db_features,
            )

        return {
            key: value for key, value in entry.items() if key in self.rondb_serving_keys
        }

    def identify_missing_features_pre_fetch(
        self,
        entry: dict[str, Any],
        passed_features: dict[str, Any],
        vector_db_features: dict[str, Any],
    ):
        """Identify feature which will be missing in the fetched feature vector and which are not passed.

        Each serving key, value (or composite keys) need not be present in entry mapping. Missing key, value
        lead to only fetching a subset of the feature values in the query. The missing values can be filled
        via the `passed_feature` args. If `allow_missing` is set to false, then an exception should be thrown
        to match the behaviour of the sql client prior to 3.7.

        Limitation:
        - The method does not check whether serving keys correspond to existing rows in the online feature store.
        - The method does not check whether the passed features names and data types correspond to the query schema.
        """
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(
                "Checking missing serving keys in entry correspond to passed features."
            )
        missing_features_per_serving_keys = {}
        has_missing = False
        for sk_name, (
            sk_no_prefix,
            fetched_features,
        ) in self.per_serving_key_features.items():
            passed_feature_names = (
                set(passed_features.keys()) if passed_features else set()
            )
            if vector_db_features and len(vector_db_features) > 0:
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        "vector_db_features for pre-fetch missing : %s",
                        vector_db_features,
                    )
                passed_feature_names = passed_feature_names.union(
                    vector_db_features.keys()
                )
            if self._on_demand_feature_names and len(self._on_demand_feature_names) > 0:
                # Remove on-demand features from validation check as they would be computed.
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        "Appending on_demand_feature_names : %s, to passed_feature_names for pre-fetch missing",
                        self._on_demand_feature_names,
                    )
                passed_feature_names = passed_feature_names.union(
                    self._on_demand_feature_names
                )
            neither_fetched_nor_passed = fetched_features.difference(
                passed_feature_names
            )
            # if not present and all corresponding features are not passed via passed_features
            # or vector_db_features
            if (
                sk_name not in entry and sk_no_prefix not in entry
            ) and not fetched_features.issubset(passed_feature_names):
                if _logger.isEnabledFor(logging.DEBUG):
                    _logger.debug(
                        f"Missing serving key {sk_name} and corresponding features {neither_fetched_nor_passed}."
                    )
                has_missing = True
                missing_features_per_serving_keys[sk_name] = neither_fetched_nor_passed

        if has_missing:
            raise exceptions.FeatureStoreException(
                f"Incomplete feature vector requests: {missing_features_per_serving_keys}."
                "To fetch or build a complete feature vector provide either (or both):\n"
                "\t- the missing features by adding corresponding serving key, value to the entry.\n"
                "\t- the missing features as key, value pair using the passed_features kwarg.\n"
                "Use `allow_missing=True` to allow missing features in the fetched feature vector."
            )

    def build_per_serving_key_features(
        self,
        serving_keys: list[sk_mod.ServingKey],
        features: list[tdf_mod.TrainingDatasetFeature],
    ) -> dict[str, tuple[str, set[str]]]:
        """Build a dictionary of feature names which will be fetched per serving key."""
        per_serving_key_features = {}
        for serving_key in serving_keys:
            per_serving_key_features[serving_key.required_serving_key] = (
                serving_key.feature_name,
                {
                    f.name
                    for f in features
                    if (
                        f.feature_group.name == serving_key.feature_group.name
                        and not f.label
                    )
                },
            )
        return per_serving_key_features

    @property
    def sql_client(
        self,
    ) -> online_store_sql_engine.OnlineStoreSqlClient | None:
        return self._sql_client

    @property
    def rest_client_engine(
        self,
    ) -> online_store_rest_client_engine.OnlineStoreRestClientEngine | None:
        return self._rest_client_engine

    @property
    def serving_keys(self) -> list[sk_mod.ServingKey]:
        return self._serving_keys

    @serving_keys.setter
    def serving_keys(self, serving_vector_keys: list[sk_mod.ServingKey]):
        self._serving_keys = serving_vector_keys

    @property
    def required_serving_keys(self) -> list[str]:
        if len(self._required_serving_keys) == 0:
            self._required_serving_keys = [
                sk.required_serving_key for sk in self.serving_keys
            ]
        return self._required_serving_keys

    @property
    def groups_of_composite_serving_keys(self) -> dict[int, list[str]]:
        if not hasattr(self, "_groups_of_composite_serving_keys"):
            self._groups_of_composite_serving_keys = {
                sk.feature_group.id: (
                    [
                        (sk_match.required_serving_key, sk_match.feature_name)
                        for sk_match in self.serving_keys
                        if sk_match.feature_group.id == sk.feature_group.id
                    ]
                )
                for sk in self.serving_keys
            }
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(
                    f"Groups of composite serving keys: {self._groups_of_composite_serving_keys}."
                )
        return self._groups_of_composite_serving_keys

    @property
    def rondb_serving_keys(self) -> list[str]:
        if not hasattr(self, "_rondb_serving_keys"):
            self._rondb_serving_keys = [
                sk.required_serving_key
                for sk in self.serving_keys
                if sk.feature_group.id not in self._skip_fg_ids
            ]
            if _logger.isEnabledFor(logging.DEBUG):
                _logger.debug(f"RonDB serving keys: {self._rondb_serving_keys}.")
        return self._rondb_serving_keys

    @property
    def training_dataset_version(self) -> int | None:
        return self._training_dataset_version

    @training_dataset_version.setter
    def training_dataset_version(self, training_dataset_version: int | None):
        self._training_dataset_version = training_dataset_version

    @property
    def feature_vector_col_name(self) -> list[str]:
        return self._feature_vector_col_name

    @property
    def per_serving_key_features(self) -> dict[str, set[str]]:
        if (
            not hasattr(self, "_per_serving_key_features")
            or self._per_serving_key_features is None
            or len(self._per_serving_key_features) == 0
        ):
            self._per_serving_key_features = self.build_per_serving_key_features(
                self.serving_keys, self._features
            )
        return self._per_serving_key_features

    @property
    def model_dependent_transformation_functions(
        self,
    ) -> list[transformation_function.TransformationFunction] | None:
        return self._model_dependent_transformation_functions

    @property
    def on_demand_transformation_functions(
        self,
    ) -> list[transformation_function.TransformationFunction] | None:
        return self._on_demand_transformation_functions

    @property
    def return_feature_value_handlers(self) -> dict[str, Callable]:
        """A dictionary of functions to the feature values returned from RonDB Server."""
        return self._return_feature_value_handlers

    @property
    def feature_to_handle_if_rest(self) -> set[str]:
        # RonDB REST client already deserialize complex features
        # Only timestamp need converting to datetime obj and
        # complex features retrieved via opensearch need to be deserialized
        if self._feature_to_handle_if_rest is None:
            self._feature_to_handle_if_rest = {
                f.name
                for f in self._features
                if (
                    (
                        f.type == "timestamp"
                        or (f.feature_group.id in self._skip_fg_ids and f.is_complex())
                    )
                    and not (
                        f.label or f.training_helper_column or f.inference_helper_column
                    )
                )
            }
        return self._feature_to_handle_if_rest

    @property
    def feature_to_handle_if_sql(self) -> set[str]:
        # Unlike REST client, SQL client does not deserialize complex features
        # however, it does convert timestamp to datetime obj
        if self._feature_to_handle_if_sql is None:
            self._feature_to_handle_if_sql = {
                f.name
                for f in self._features
                if (
                    f.is_complex()
                    and not (
                        f.label or f.training_helper_column or f.inference_helper_column
                    )
                )
            }
        return self._feature_to_handle_if_sql

    @property
    def valid_serving_keys(self):
        if not self._valid_serving_keys or len(self._valid_serving_keys) == 0:
            self._valid_serving_keys = {
                sk.required_serving_key for sk in self.serving_keys
            }
        return self._valid_serving_keys

    @property
    def default_client(self) -> str:
        return self._default_client

    @default_client.setter
    def default_client(self, default_client: Literal["rest", "sql"]):
        if default_client not in [
            self.DEFAULT_REST_CLIENT,
            self.DEFAULT_SQL_CLIENT,
        ]:
            raise ValueError(
                f"Default Online Feature Store Client should be one of {self.DEFAULT_REST_CLIENT} or {self.DEFAULT_SQL_CLIENT}."
            )

        if (
            default_client == self.DEFAULT_REST_CLIENT
            and self._init_rest_client is False
        ):
            raise ValueError(
                f"Default Online Store Cient is set to {self.DEFAULT_REST_CLIENT} but REST client"
                " is not initialised. Call `init_serving` with init_rest_client set to True before using it."
            )
        if default_client == self.DEFAULT_SQL_CLIENT and self._init_sql_client is False:
            raise ValueError(
                f"Default Online Store client is set to {self.DEFAULT_SQL_CLIENT} but Online Store SQL client"
                " is not initialised. Call `init_serving` with init_sql_client set to True before using it."
            )
        if _logger.isEnabledFor(logging.DEBUG):
            _logger.debug(f"Default Online Store Client is set to {default_client}.")
        self._default_client = default_client

    @property
    def transformed_feature_vector_col_name(self):
        if self._transformed_feature_vector_col_name is None:
            dropped_features = []
            output_column_names = []
            for (
                transformation_function
            ) in self._model_dependent_transformation_functions:
                if transformation_function.hopsworks_udf.dropped_features:
                    dropped_features += (
                        transformation_function.hopsworks_udf.dropped_features
                    )
                output_column_names += (
                    transformation_function.hopsworks_udf.output_column_names
                )

            self._transformed_feature_vector_col_name = [
                feature
                for feature in self._feature_vector_col_name
                if feature not in dropped_features
            ]
            self._transformed_feature_vector_col_name.extend(output_column_names)
        return self._transformed_feature_vector_col_name

    @property
    def _all_features_on_demand(self) -> bool:
        """True if all features in the feature view are on-demand."""
        if self.__all_features_on_demand is None:
            self.__all_features_on_demand = all(
                feature.on_demand_transformation_function
                for feature in self._features
                if not feature.label
            )
        return self.__all_features_on_demand

    @property
    def _all_feature_groups_online(self) -> bool:
        """True if all feature groups in the feature view are online."""
        if self.__all_feature_groups_online is None:
            self.__all_feature_groups_online = all(
                fg.online_enabled
                for fg in self._parent_feature_groups
                if fg.id not in self._skip_fg_ids
            )
        return self.__all_feature_groups_online
