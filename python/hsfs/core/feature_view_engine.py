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

import datetime
import logging
import warnings
from typing import TYPE_CHECKING, Any, TypeVar

from hopsworks_common import client, constants
from hopsworks_common.client import exceptions
from hopsworks_common.client.exceptions import FeatureStoreException
from hopsworks_common.core.constants import HAS_NUMPY
from hsfs import (
    engine,
    feature,
    feature_group,
    feature_view,
    training_dataset_feature,
    util,
)
from hsfs.constructor.filter import Filter, Logic
from hsfs.core import (
    feature_view_api,
    query_constructor_api,
    statistics_engine,
    tags_api,
    training_dataset_engine,
    transformation_function_engine,
)
from hsfs.core.feature_logging import FeatureLogging
from hsfs.training_dataset_split import TrainingDatasetSplit


if HAS_NUMPY:
    import numpy as np

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from hsfs.constructor.join import Join
    from hsfs.constructor.query import Query
    from hsfs.core.feature_logging import LoggingMetaData
    from hsfs.feature_logger import FeatureLogger
    from hsfs.transformation_function import TransformationFunction

_logger = logging.getLogger(__name__)


class FeatureViewEngine:
    ENTITY_TYPE = "featureview"
    _TRAINING_DATA_API_PATH = "trainingdatasets"
    _OVERWRITE = "overwrite"
    _APPEND = "append"

    def __init__(self, feature_store_id):
        self._feature_store_id = feature_store_id

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._tags_api = tags_api.TagsApi(feature_store_id, self.ENTITY_TYPE)
        self._statistics_engine = statistics_engine.StatisticsEngine(
            feature_store_id, self._TRAINING_DATA_API_PATH
        )
        self._training_dataset_engine = training_dataset_engine.TrainingDatasetEngine(
            feature_store_id
        )
        self._transformation_function_engine = (
            transformation_function_engine.TransformationFunctionEngine(
                feature_store_id
            )
        )
        self._query_constructor_api = query_constructor_api.QueryConstructorApi()

    def save(
        self, feature_view_obj: feature_view.FeatureView
    ) -> feature_view.FeatureView:
        """Save a feature view to the backend.

        Parameters:
            feature_view_obj `FeatureView` : The feature view object to be saved.

        Returns:
            `FeatureView` : Updated feature view that has the ID used to save in the backend.
        """
        if feature_view_obj.query.is_time_travel():
            warnings.warn(
                "`as_of` argument in the `Query` will be ignored because"
                " feature view does not support time travel query.",
                stacklevel=1,
            )
        if feature_view_obj.labels:
            for label_name in feature_view_obj.labels:
                (
                    feature,
                    prefix,
                    featuregroup,
                ) = feature_view_obj.query._get_feature_by_name(label_name)
                feature_view_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature.name,
                        label=True,
                        featuregroup=featuregroup,
                    )
                )
        if feature_view_obj.inference_helper_columns:
            for helper_column_name in feature_view_obj.inference_helper_columns:
                (
                    feature,
                    prefix,
                    featuregroup,
                ) = feature_view_obj.query._get_feature_by_name(helper_column_name)
                feature_view_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature.name,
                        inference_helper_column=True,
                        featuregroup=featuregroup,
                    )
                )

        if feature_view_obj.training_helper_columns:
            for helper_column_name in feature_view_obj.training_helper_columns:
                (
                    feature,
                    prefix,
                    featuregroup,
                ) = feature_view_obj.query._get_feature_by_name(helper_column_name)
                feature_view_obj._features.append(
                    training_dataset_feature.TrainingDatasetFeature(
                        name=feature.name,
                        training_helper_column=True,
                        featuregroup=featuregroup,
                    )
                )

        updated_fv = self._feature_view_api.post(feature_view_obj)
        print(
            "Feature view created successfully, explore it at \n"
            + self._get_feature_view_url(updated_fv)
        )
        return updated_fv

    def update(
        self, feature_view_obj: feature_view.FeatureView
    ) -> feature_view.FeatureView:
        """Update the feature view object saved in the backend.

        Parameters:
            feature_view_obj `FeatureView` : The feature view object to be saved.

        Returns:
            `FeatureView` : Updated feature view that has the ID used to save in the backend.
        """
        self._feature_view_api.update(feature_view_obj)
        return feature_view_obj

    def get(
        self, name: str, version: int = None
    ) -> feature_view.FeatureView | list[feature_view.FeatureView]:
        """Get a feature view from the backend using name or using name and version.

        If version is not provided then a List of feature views containing all of its versions is returned.

        Parameters:
            name `str`: Name of feature view.
            version `version`: Version of the feature view.

        Returns:
            `Union[FeatureView, List[FeatureView]]`

        Raises:
            hopsworks.client.exceptions.RestAPIError: If the backend encounters an error when handling the request
            ValueError: If the feature group associated with the feature view cannot be found.
        """
        if version:
            fv = self._feature_view_api.get_by_name_version(name, version)
        else:
            fv = self._feature_view_api.get_by_name(name)
        return fv

    def delete(self, name, version=None):
        if version:
            return self._feature_view_api.delete_by_name_version(name, version)
        return self._feature_view_api.delete_by_name(name)

    def get_training_dataset_schema(
        self,
        feature_view: feature_view.FeatureView,
        training_dataset_version: int | None = None,
    ):
        """Function that returns the schema of the training dataset generated using the feature view.

        Parameters:
            feature_view: `FeatureView`. The feature view for which the schema is to be generated.
            training_dataset_version: `int`. Specifies the version of the training dataset for which the schema should be generated.
                By default, this is set to None. However, if the `one_hot_encoder` transformation function is used, the training dataset version must be provided.
                This is because the schema will then depend on the statistics of the training data used.

        Returns:
            `List[training_dataset_feature.TrainingDatasetFeature]`: List of training dataset features objects.
        """
        # This is used to verify that the training dataset version actually exists, otherwise it raises an exception
        if training_dataset_version:
            self._get_training_dataset_metadata(feature_view, training_dataset_version)
        if not feature_view.transformation_functions:
            # Return the features in the feature view if the no transformation functions in the feature view.
            return feature_view.features
        transformed_features = []
        transformed_labels = []
        dropped_features = set()

        # Statistics only required for computing schema if one-hot-encoder in the transformation functions
        statistics_required = any(
            tf.hopsworks_udf.function_name == "one_hot_encoder"
            for tf in feature_view.transformation_functions
        )

        if statistics_required:
            if not training_dataset_version:
                raise FeatureStoreException(
                    "The feature view includes the one_hot_encoder transformation function. As a result, the schema of the generated training dataset depends on its statistics. Please specify the version of the training dataset for which the schema should be generated."
                )

            # Get transformation functions with correct statistics based on training dataset version.
            transformation_functions = self._transformation_function_engine.get_ready_to_use_transformation_fns(
                feature_view=feature_view,
                training_dataset_version=training_dataset_version,
            )
        else:
            transformation_functions = feature_view.transformation_functions

        # Getting all dropped features
        for tf in transformation_functions:
            if tf.hopsworks_udf.dropped_features:
                dropped_features.update(tf.hopsworks_udf.dropped_features)

        # Creating list of features not dropped after transformations
        transformed_features = [
            feature
            for feature in feature_view.features
            if feature.name not in dropped_features and not feature.label
        ]

        # Creating list of labels not dropped after transformations
        transformed_labels = [
            feature
            for feature in feature_view.features
            if feature.name not in dropped_features and feature.label
        ]

        for tf in transformation_functions:
            # create transformed labels if a transformation functions take a labels as input.
            if any(
                label in tf.hopsworks_udf.transformation_features
                for label in feature_view.labels
            ):
                transformed_labels.extend(
                    [
                        training_dataset_feature.TrainingDatasetFeature(
                            name=transformed_label_name,
                            type=output_type,
                            label=True,
                        )
                        for transformed_label_name, output_type in zip(
                            tf.output_column_names, tf.hopsworks_udf.return_types
                        )
                    ]
                )
            # create transformed features if a transformation functions take a no labels as input.
            else:
                transformed_features.extend(
                    [
                        training_dataset_feature.TrainingDatasetFeature(
                            name=transformed_feature_name,
                            type=output_type,
                            label=False,
                        )
                        for transformed_feature_name, output_type in zip(
                            tf.output_column_names, tf.hopsworks_udf.return_types
                        )
                    ]
                )

        return transformed_features + transformed_labels

    def get_batch_query(
        self,
        feature_view_obj,
        start_time,
        end_time,
        with_label=False,
        primary_keys=False,
        event_time=False,
        inference_helper_columns=False,
        training_helper_columns=False,
        training_dataset_version=None,
        spine=None,
    ):
        try:
            query = self._feature_view_api.get_batch_query(
                feature_view_obj.name,
                feature_view_obj.version,
                util.convert_event_time_to_timestamp(start_time),
                util.convert_event_time_to_timestamp(end_time),
                training_dataset_version=training_dataset_version,
                is_python_engine=engine.get_type() == "python",
                with_label=with_label,
                primary_keys=primary_keys,
                event_time=event_time,
                inference_helper_columns=inference_helper_columns,
                training_helper_columns=training_helper_columns,
            )
            # verify whatever is passed 1. spine group with dataframe contained, or 2. dataframe
            # the schema has to be consistent

            # allow passing new spine group or dataframe
            if isinstance(spine, feature_group.SpineGroup):
                # schema of original fg on left side needs to be consistent with schema contained in the
                # spine group to overwrite the feature group
                dataframe_features = engine.get_instance().parse_schema_feature_group(
                    spine.dataframe
                )
                spine._feature_group_engine._verify_schema_compatibility(
                    query._left_feature_group.features, dataframe_features
                )
                query._left_feature_group = spine
            elif isinstance(query._left_feature_group, feature_group.SpineGroup):
                if spine is None:
                    raise FeatureStoreException(
                        "Feature View was created with a spine group, setting the `spine` argument is mandatory."
                    )
                # the dataframe setter will verify the schema of the dataframe
                query._left_feature_group.dataframe = spine
            return query
        except exceptions.RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270172:
                raise ValueError(
                    "Cannot generate dataset(s) from the given start/end time because"
                    " event time column is not available in the left feature groups."
                    " A start/end time should not be provided as parameters."
                ) from e
            raise e

    def get_batch_query_string(
        self, feature_view_obj, start_time, end_time, training_dataset_version=None
    ):
        try:
            query_obj = self._feature_view_api.get_batch_query(
                feature_view_obj.name,
                feature_view_obj.version,
                util.convert_event_time_to_timestamp(start_time),
                util.convert_event_time_to_timestamp(end_time),
                training_dataset_version=training_dataset_version,
                is_python_engine=engine.get_type() == "python",
            )
        except exceptions.RestAPIError as e:
            if e.response.json().get("errorCode", "") == 270172:
                raise ValueError(
                    "Cannot generate a query from the given start/end time because"
                    " event time column is not available in the left feature groups."
                    " A start/end time should not be provided as parameters."
                ) from e
            raise e

        fs_query = self._query_constructor_api.construct_query(query_obj)
        if fs_query.pit_query is not None:
            return fs_query.pit_query
        return fs_query.query

    def create_training_dataset(
        self,
        feature_view_obj,
        training_dataset_obj,
        user_write_options,
        spine=None,
        primary_keys=True,
        event_time=True,
        training_helper_columns=True,
        transformation_context: dict[str, Any] = None,
    ):
        self._set_event_time(feature_view_obj, training_dataset_obj)
        updated_instance = self._create_training_data_metadata(
            feature_view_obj, training_dataset_obj
        )
        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj,
            spine=spine,
            primary_keys=primary_keys,
            event_time=event_time,
            training_helper_columns=training_helper_columns,
            transformation_context=transformation_context,
        )
        return updated_instance, td_job

    def get_training_data(
        self,
        feature_view_obj: feature_view.FeatureView,
        read_options=None,
        splits=None,
        training_dataset_obj=None,
        training_dataset_version=None,
        spine=None,
        primary_keys=False,
        event_time=False,
        training_helper_columns=False,
        dataframe_type="default",
        transformation_context: dict[str, Any] = None,
        n_processes: int = None,
    ):
        # check if provided td version has already existed.
        if training_dataset_version:
            td_updated = self._get_training_dataset_metadata(
                feature_view_obj, training_dataset_version
            )
        else:
            self._set_event_time(feature_view_obj, training_dataset_obj)
            td_updated = self._create_training_data_metadata(
                feature_view_obj, training_dataset_obj
            )
        # check splits
        if splits is None:
            splits = []
        if len(splits) != len(td_updated.splits):
            if len(td_updated.splits) == 0:
                method_name = "get_training_data"
            elif len(td_updated.splits) == 2:
                method_name = "get_train_test_split"
            elif len(td_updated.splits) == 3:
                method_name = "get_train_validation_test_split"
            raise ValueError(
                f"Incorrect `get` method is used. Use `feature_view.{method_name}` instead."
            )

        read_options = engine.get_instance().read_options(
            td_updated.data_format, read_options
        )

        if td_updated.training_dataset_type != td_updated.IN_MEMORY:
            split_df = self._read_from_storage_connector(
                td_updated,
                td_updated.splits,
                read_options,
                with_primary_keys=primary_keys,
                # at this stage training dataset was already written and if there was any name clash it should have
                # already failed in creation phase, so we don't need to check it here. This is to make
                # sure that both it is backwards compatible and also to drop columns if user set
                # primary_keys and event_times to True on creation stage.
                primary_keys=self._get_primary_keys_from_query(
                    feature_view_obj.query, False
                ),
                with_event_time=event_time,
                event_time=self._get_eventtimes_from_query(
                    feature_view_obj.query, False
                ),
                with_training_helper_columns=training_helper_columns,
                training_helper_columns=feature_view_obj.training_helper_columns,
                feature_view_features=[
                    feature.name for feature in feature_view_obj.features
                ],
                # forcing dataframe type to default here since dataframe operations are required for training data split.
                dataframe_type="default"
                if dataframe_type.lower() in ["numpy", "python"]
                else dataframe_type,  # forcing dataframe type to default here since dataframe operations are required for training data split.
            )
        else:
            self._check_feature_group_accessibility(feature_view_obj)
            query = self.get_batch_query(
                feature_view_obj,
                training_dataset_version=td_updated.version,
                start_time=td_updated.event_start_time,
                end_time=td_updated.event_end_time,
                with_label=True,
                inference_helper_columns=False,
                primary_keys=primary_keys,
                event_time=event_time,
                training_helper_columns=training_helper_columns,
                spine=spine,
            )
            split_df = engine.get_instance().get_training_data(
                td_updated,
                feature_view_obj,
                query,
                read_options,
                dataframe_type,
                training_dataset_version,
                transformation_context=transformation_context,
                n_processes=n_processes,
            )
            self.compute_training_dataset_statistics(
                feature_view_obj, td_updated, split_df
            )

        # Getting transformed label names
        transformed_labels = [
            feature.name
            for feature in self.get_training_dataset_schema(
                feature_view=feature_view_obj,
                training_dataset_version=td_updated.version,
            )
            if feature.label
        ]

        # Updating labels based if transformation functions are attached to the feature view.
        labels = (
            transformed_labels
            if feature_view_obj.transformation_functions
            else feature_view_obj.labels
        )

        # Set training dataset schema after training dataset has been generated
        td_updated.schema = self.get_training_dataset_schema(
            feature_view=feature_view_obj, training_dataset_version=td_updated.version
        )

        # split df into features and labels df
        if td_updated.splits:
            for split in td_updated.splits:
                split_name = split.name
                split_df[split_name] = engine.get_instance().split_labels(
                    split_df[split_name], labels, dataframe_type
                )
            feature_dfs = []
            label_dfs = []
            for split in splits:
                feature_dfs.append(split_df[split][0])
                label_dfs.append(split_df[split][1])
            return td_updated, feature_dfs + label_dfs
        split_df = engine.get_instance().split_labels(split_df, labels, dataframe_type)
        return td_updated, split_df

    def _set_event_time(self, feature_view_obj, training_dataset_obj):
        event_time = feature_view_obj.query._left_feature_group.event_time
        if event_time:
            if training_dataset_obj.splits:
                for split in training_dataset_obj.splits:
                    if (
                        split.split_type == TrainingDatasetSplit.TIME_SERIES_SPLIT
                        and split.name == TrainingDatasetSplit.TRAIN
                        and not split.start_time
                    ):
                        split.start_time = self._get_start_time()
                    if (
                        split.split_type == TrainingDatasetSplit.TIME_SERIES_SPLIT
                        and split.name == TrainingDatasetSplit.TEST
                        and not split.end_time
                    ):
                        split.end_time = self._get_end_time()
            else:
                if not training_dataset_obj.event_start_time:
                    training_dataset_obj.event_start_time = self._get_start_time()
                if not training_dataset_obj.event_end_time:
                    training_dataset_obj.event_end_time = self._get_end_time()

    def _get_start_time(self):
        # minimum start time is 1 second
        return 1000

    def _get_end_time(self):
        # end time is current time
        return int(float(datetime.datetime.now().timestamp()) * 1000)

    def recreate_training_dataset(
        self,
        feature_view_obj,
        training_dataset_version,
        statistics_config,
        user_write_options,
        spine=None,
        transformation_context: dict[str, Any] = None,
    ):
        training_dataset_obj = self._get_training_dataset_metadata(
            feature_view_obj, training_dataset_version
        )

        if statistics_config is not None:
            # update statistics config if provided. This is currently the only way to update TD statistics config.
            # recreating a training dataset may result in different statistics if the FGs data in the FV query have been update.
            training_dataset_obj.statistics_config = statistics_config
            training_dataset_obj.update_statistics_config()

        td_job = self.compute_training_dataset(
            feature_view_obj,
            user_write_options,
            training_dataset_obj=training_dataset_obj,
            spine=spine,
            transformation_context=transformation_context,
        )
        # Set training dataset schema after training dataset has been generated
        training_dataset_obj.schema = self.get_training_dataset_schema(
            feature_view=feature_view_obj,
            training_dataset_version=training_dataset_obj.version,
        )
        return training_dataset_obj, td_job

    def _read_from_storage_connector(
        self,
        training_data_obj,
        splits,
        read_options,
        with_primary_keys,
        primary_keys,
        with_event_time,
        event_time,
        with_training_helper_columns,
        training_helper_columns,
        feature_view_features,
        dataframe_type,
    ):
        if splits:
            result = {}
            for split in splits:
                path = training_data_obj.location + "/" + str(split.name)
                result[split.name] = self._read_dir_from_storage_connector(
                    training_data_obj,
                    path,
                    read_options,
                    with_primary_keys,
                    primary_keys,
                    with_event_time,
                    event_time,
                    with_training_helper_columns,
                    training_helper_columns,
                    feature_view_features,
                    dataframe_type,
                )
            return result
        path = training_data_obj.location + "/" + training_data_obj.name
        return self._read_dir_from_storage_connector(
            training_data_obj,
            path,
            read_options,
            with_primary_keys,
            primary_keys,
            with_event_time,
            event_time,
            with_training_helper_columns,
            training_helper_columns,
            feature_view_features,
            dataframe_type,
        )

    def _cast_columns(self, data_format, df, schema):
        if data_format == "csv" or data_format == "tsv":
            if not schema:
                raise FeatureStoreException("Reading csv, tsv requires a schema.")
            return engine.get_instance().cast_columns(df, schema)
        return df

    def _read_dir_from_storage_connector(
        self,
        training_data_obj,
        path,
        read_options,
        with_primary_keys,
        primary_keys,
        with_event_time,
        event_time,
        with_training_helper_columns,
        training_helper_columns,
        feature_view_features,
        dataframe_type,
    ):
        try:
            df = training_data_obj.storage_connector.read(
                # always read from materialized dataset, not query object
                query=None,
                data_format=training_data_obj.data_format,
                options=read_options,
                path=path,
                dataframe_type=dataframe_type,
            )

            df = self._drop_helper_columns(
                df,
                feature_view_features,
                with_primary_keys,
                primary_keys,
                False,
                dataframe_type,
            )
            df = self._drop_helper_columns(
                df,
                feature_view_features,
                with_event_time,
                event_time,
                False,
                dataframe_type,
            )
            return self._drop_helper_columns(
                df,
                feature_view_features,
                with_training_helper_columns,
                training_helper_columns,
                True,
                dataframe_type,
            )

        except Exception as e:
            if isinstance(e, FileNotFoundError):
                raise FileNotFoundError(
                    f"Failed to read dataset from {path}."
                    " Check if path exists or recreate a training dataset."
                ) from e
            raise e

    def _drop_helper_columns(
        self,
        df,
        feature_view_features,
        with_columns,
        columns,
        training_helper,
        dataframe_type,
    ):
        if not with_columns:
            if (
                engine.get_type().startswith("spark")
                and dataframe_type.lower() == "spark"
            ):
                existing_cols = [field.name for field in df.schema.fields]
            else:
                existing_cols = df.columns
            # primary keys and event time are dropped only if they are in the query
            drop_cols = list(set(existing_cols).intersection(columns))
            # training helper is always in the query
            if not training_helper:
                drop_cols = list(set(drop_cols).difference(feature_view_features))
            if drop_cols:
                df = engine.get_instance().drop_columns(df, drop_cols)
        return df

    # This method is used by hsfs_utils to launch a job for python client
    def compute_training_dataset(
        self,
        feature_view_obj,
        user_write_options,
        training_dataset_obj=None,
        training_dataset_version=None,
        spine=None,
        primary_keys=False,
        event_time=False,
        training_helper_columns=False,
        transformation_context: dict[str, Any] = None,
    ):
        if training_dataset_obj:
            pass
        elif training_dataset_version:
            training_dataset_obj = self._get_training_dataset_metadata(
                feature_view_obj, training_dataset_version
            )
        else:
            raise ValueError("No training dataset object or version is provided")

        batch_query = self.get_batch_query(
            feature_view_obj,
            training_dataset_obj.event_start_time,
            training_dataset_obj.event_end_time,
            with_label=True,
            primary_keys=primary_keys,
            event_time=event_time,
            inference_helper_columns=False,
            training_helper_columns=training_helper_columns,
            training_dataset_version=training_dataset_obj.version,
            spine=spine,
        )

        # for spark job
        user_write_options["training_helper_columns"] = training_helper_columns
        user_write_options["primary_keys"] = primary_keys
        user_write_options["event_time"] = event_time

        td_job = engine.get_instance().write_training_dataset(
            training_dataset_obj,
            batch_query,
            user_write_options,
            self._OVERWRITE,
            feature_view_obj=feature_view_obj,
            transformation_context=transformation_context,
        )

        # Set training dataset schema after training dataset has been generated
        training_dataset_obj.schema = self.get_training_dataset_schema(
            feature_view=feature_view_obj,
            training_dataset_version=training_dataset_obj.version,
        )

        if engine.get_type().startswith("spark"):
            # if spark engine, read td and compute stats
            if training_dataset_obj.splits:
                td_df = {
                    split.name: self._training_dataset_engine.read(
                        training_dataset_obj, split.name, {}
                    )
                    for split in training_dataset_obj.splits
                }
            else:
                td_df = self._training_dataset_engine.read(
                    training_dataset_obj, None, {}
                )
            self.compute_training_dataset_statistics(
                feature_view_obj, training_dataset_obj, td_df
            )

        return td_job

    def compute_training_dataset_statistics(
        self, feature_view_obj, training_dataset_obj, td_df
    ):
        if training_dataset_obj.statistics_config.enabled:
            if training_dataset_obj.splits:
                if not isinstance(td_df, dict):
                    raise ValueError(
                        "Provided dataframes should be in dict format "
                        "'split': dataframe"
                    )
                return self._statistics_engine.compute_and_save_split_statistics(
                    training_dataset_obj,
                    feature_dataframes=td_df,
                    feature_view_obj=feature_view_obj,
                )
            return self._statistics_engine.compute_and_save_statistics(
                training_dataset_obj,
                feature_dataframe=td_df,
                feature_view_obj=feature_view_obj,
            )
        return None

    def _get_training_dataset_metadata(
        self, feature_view_obj: feature_view.FeatureView, training_dataset_version
    ):
        return self._feature_view_api.get_training_dataset_by_version(
            feature_view_obj.name, feature_view_obj.version, training_dataset_version
        )

    def _get_training_datasets_metadata(
        self, feature_view_obj: feature_view.FeatureView
    ):
        tds = self._feature_view_api.get_training_datasets(
            feature_view_obj.name, feature_view_obj.version
        )
        # schema needs to be set for writing training data or feature serving
        for td in tds:
            td.schema = feature_view_obj.get_training_dataset_schema(td.version)
        return tds

    def get_training_datasets(self, feature_view_obj):
        tds = self._get_training_datasets_metadata(feature_view_obj)
        # this is the only place we expose training dataset metadata
        # we return training dataset base classes with metadata only
        return [super(td.__class__, td) for td in tds]

    def _create_training_data_metadata(self, feature_view_obj, training_dataset_obj):
        return self._feature_view_api.create_training_dataset(
            feature_view_obj.name, feature_view_obj.version, training_dataset_obj
        )

    def delete_training_data(self, feature_view_obj, training_data_version=None):
        if training_data_version:
            self._feature_view_api.delete_training_data_version(
                feature_view_obj.name, feature_view_obj.version, training_data_version
            )
        else:
            self._feature_view_api.delete_training_data(
                feature_view_obj.name, feature_view_obj.version
            )

    def delete_training_dataset_only(
        self, feature_view_obj, training_data_version=None
    ):
        if training_data_version:
            self._feature_view_api.delete_training_dataset_only_version(
                feature_view_obj.name, feature_view_obj.version, training_data_version
            )
        else:
            self._feature_view_api.delete_training_dataset_only(
                feature_view_obj.name, feature_view_obj.version
            )

    def apply_transformations(
        self,
        execution_graph: list[list[TransformationFunction]],
        data: pd.DataFrame | pl.DataFrame | list[dict[str, Any]],
        online: bool | None = None,
        transformation_context: dict[str, Any] | list[dict[str, Any]] = None,
        request_parameters: dict[str, Any] | list[dict[str, Any]] = None,
        n_processes: int = None,
    ) -> list[dict[str, Any]] | pd.DataFrame | pl.DataFrame:
        """Apply transformations functions to the passed dataframe or list of dictionaries.

        Parameters:
            transformation_functions: List of transformation functions to apply.
            data: The dataframe or list of dictionaries to apply the transformations to.
            online: Apply the transformations for online or offline usecase. This parameter is applicable when a transformation function is defined using the `default` execution mode.
            transformation_context: Transformation context to be used when applying the transformations.
            request_parameters: Request parameters to be used when applying the transformations.
            n_processes: Number of processes to use for parallel execution of transformation functions.

        Returns:
            The updated dataframe or list of dictionaries with the transformations applied.
        """
        try:
            df = transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions(
                execution_graph=execution_graph,
                data=data,
                online=online,
                transformation_context=transformation_context,
                request_parameters=request_parameters,
                n_processes=n_processes,
            )
        except exceptions.TransformationFunctionException as e:
            raise FeatureStoreException(
                f"The following feature(s): {e.missing_features}, specified in the {e.transformation_type} transformation function '{e.transformation_function_name}' are not present in the dataframe."
                " Please verify that the correct feature names are used in the transformation function and that these features exist in the dataframe."
            ) from e
        return df

    def get_batch_data(
        self,
        feature_view_obj,
        start_time,
        end_time,
        training_dataset_version,
        execution_graph,
        read_options=None,
        spine=None,
        primary_keys=False,
        event_time=False,
        inference_helper_columns=False,
        dataframe_type="default",
        transformed=True,
        transformation_context: dict[str, Any] = None,
        logging_data: bool = False,
        n_processes: int = None,
    ):
        self._check_feature_group_accessibility(feature_view_obj)

        # check if primary_keys/event_time are ambiguous
        if primary_keys:
            self._get_primary_keys_from_query(feature_view_obj.query)
        if event_time:
            self._get_eventtimes_from_query(feature_view_obj.query)

        # Fetch batch data with primary key, event time and inference helper columns if logging metadata is required.
        # Columns fetched to create logging metadata is implicitly removed in the client before returning to the user.
        feature_dataframe = self.get_batch_query(
            feature_view_obj,
            start_time,
            end_time,
            with_label=False,
            primary_keys=primary_keys or logging_data,
            event_time=event_time or logging_data,
            inference_helper_columns=inference_helper_columns
            or not transformed
            or logging_data,
            training_helper_columns=False,
            training_dataset_version=training_dataset_version,
            spine=spine,
        ).read(read_options=read_options, dataframe_type=dataframe_type)
        if (execution_graph and transformed) or logging_data:
            try:
                transformed_dataframe = transformation_function_engine.TransformationFunctionEngine.apply_transformation_functions(
                    execution_graph=execution_graph,
                    data=feature_dataframe,
                    online=False,
                    transformation_context=transformation_context,
                    n_processes=n_processes,
                )
            except exceptions.TransformationFunctionException as e:
                raise FeatureStoreException(
                    f"The following feature(s): {e.missing_features}, specified in the {e.transformation_type} transformation function '{e.transformation_function_name}' are not present in the dataframe."
                    " Please verify that the correct feature names are used in the transformation function and that these features exist in the dataframe."
                ) from e
        else:
            transformed_dataframe = None

        batch_dataframe = (
            transformed_dataframe
            if (execution_graph and transformed)
            else feature_dataframe
        )

        if logging_data:
            batch_dataframe = engine.get_instance().extract_logging_metadata(
                untransformed_features=feature_dataframe,
                transformed_features=transformed_dataframe,
                feature_view=feature_view_obj,
                transformed=transformed,
                inference_helpers=inference_helper_columns,
                event_time=event_time,
                primary_key=primary_keys,
            )

        return batch_dataframe

    def transform_batch_data(self, features, transformation_functions):
        try:
            return self._transformation_function_engine.s(
                transformation_functions, dataset=features, inplace=False
            )
        except exceptions.TransformationFunctionException as e:
            raise exceptions.FeatureStoreException(
                f"The following feature(s): {e.missing_features}, specified in the {e.transformation_type} transformation function '{e.transformation_function_name}' are not present in the feature view. "
                " Please verify that the correct features are specified in the transformation function."
            ) from e

    def add_tag(
        self, feature_view_obj, name: str, value, training_dataset_version=None
    ):
        self._tags_api.add(
            feature_view_obj,
            name,
            value,
            training_dataset_version=training_dataset_version,
        )

    def delete_tag(self, feature_view_obj, name: str, training_dataset_version=None):
        self._tags_api.delete(
            feature_view_obj, name, training_dataset_version=training_dataset_version
        )

    def get_tag(self, feature_view_obj, name: str, training_dataset_version=None):
        tags = self._tags_api.get(
            feature_view_obj, name, training_dataset_version=training_dataset_version
        )
        if name in tags:
            return tags[name]
        return None

    def get_tags(self, feature_view_obj, training_dataset_version=None):
        return self._tags_api.get(
            feature_view_obj, training_dataset_version=training_dataset_version
        )

    def get_parent_feature_groups(self, feature_view_obj):
        """Get the parents of this feature view, based on explicit provenance.

        Parents are feature groups or external feature groups. These feature
        groups can be accessible, deleted or inaccessible.
        For deleted and inaccessible feature groups, only a minimal information is
        returned.

        Parameters:
            feature_view_obj: Metadata object of feature view.

        Returns:
            `Links`:  the feature groups used to generate this feature view or None
        """
        links = self._feature_view_api.get_parent_feature_groups(
            feature_view_obj.name, feature_view_obj.version
        )
        if not links.is_empty():
            return links
        return None

    def get_models_provenance(
        self, feature_view_obj, training_dataset_version: int | None = None
    ):
        """Get the generated models using this feature view, based on explicit provenance.

        These models can be accessible or inaccessible. Explicit
        provenance does not track deleted generated model links, so deleted
        will always be empty.
        For inaccessible models, only a minimal information is returned.

        Parameters:
            feature_view_obj: Filter generated models based on feature view (name, version).
            training_dataset_version: Filter generated models based on the used training dataset version.

        Returns:
            `Links`: the models generated using this feature group or None
        """
        links = self._feature_view_api.get_models_provenance(
            feature_view_obj.name,
            feature_view_obj.version,
            training_dataset_version=training_dataset_version,
        )
        if not links.is_empty():
            return links
        return None

    def _check_feature_group_accessibility(self, feature_view_obj):
        if engine.get_type() == "python":
            try:
                from hsfs.core import arrow_flight_client

                arrow_flight_client_imported = True
            except ImportError:
                arrow_flight_client_imported = False

            if (
                arrow_flight_client_imported
                and arrow_flight_client.get_instance().is_enabled()
            ):
                if not arrow_flight_client.supports(
                    feature_view_obj.query.featuregroups
                ):
                    raise NotImplementedError(
                        "Hopsworks Feature Query Service can only read from cached feature groups"
                        " and external feature groups on BigQuery and Snowflake."
                        " When using other external feature groups please use "
                        "`feature_view.create_training_data` instead. "
                        "If you are using spines, use a Spark Kernel."
                    )
            elif not feature_view_obj.query.is_cache_feature_group_only():
                raise NotImplementedError(
                    "Python kernel can only read from cached feature groups."
                    " When using external feature groups please use "
                    "`feature_view.create_training_data` instead. "
                    "If you are using spines, use a Spark Kernel."
                )

    def _get_feature_view_url(self, fv: feature_view.FeatureView):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/fs/"
            + str(fv.featurestore_id)
            + "/fv/"
            + str(fv.name)
            + "/version/"
            + str(fv.version)
        )
        return util.get_hostname_replaced_url(path)

    def _primary_keys_from_join(
        self, joins: list[Join], check_duplicate: bool, pk_names: set[str]
    ) -> set[str]:
        """Recursive function that extracts the primary keys from the join objects and returns them as a set."""
        for join in joins:
            sub_query = join.query
            join_prefix = join.prefix

            sub_query_selected_feature_names = [
                feature.name for feature in join.query._left_features
            ]
            sub_query_feature_group = sub_query._left_feature_group
            sub_query_pk_names = {
                feature.name
                for feature in sub_query_feature_group.features
                if feature.primary
            }

            sub_query_pk_names = {
                util.generate_fully_qualified_feature_name(
                    sub_query_feature_group, pk_name
                )
                if pk_name not in sub_query_selected_feature_names
                else (pk_name if not join_prefix else join_prefix + pk_name)
                for pk_name in sub_query_pk_names
            }

            if check_duplicate:
                for sub_query_pk_name in sub_query_pk_names:
                    self._check_if_exists_with_prefix(sub_query_pk_name, pk_names)

            pk_names.update(sub_query_pk_names)

            if sub_query._joins:
                sub_query_pks = self._primary_keys_from_join(
                    sub_query._joins, check_duplicate, pk_names
                )
                pk_names.update(sub_query_pks)
        return pk_names

    def _event_time_from_join(
        self, joins: list[Join], check_duplicate: bool, et_names: set[str]
    ) -> set[str]:
        for join in joins:
            sub_query = join.query
            join_prefix = join.prefix

            sub_query_selected_feature_names = [
                feature.name for feature in join.query._left_features
            ]
            sub_query_feature_group = sub_query._left_feature_group
            sub_query_event_time = sub_query_feature_group.event_time

            sub_query_event_time = (
                util.generate_fully_qualified_feature_name(
                    sub_query_feature_group, sub_query_event_time
                )
                if sub_query_event_time not in sub_query_selected_feature_names
                else (
                    join_prefix + sub_query_event_time
                    if join_prefix
                    else sub_query_event_time
                )
            )

            if check_duplicate:
                self._check_if_exists_with_prefix(sub_query_event_time, et_names)

            et_names.add(sub_query_event_time)

            if sub_query._joins:
                sub_query_ets = self._event_time_from_join(
                    sub_query._joins, check_duplicate, et_names
                )
                et_names.update(sub_query_ets)
        return et_names

    def _get_primary_keys_from_query(
        self, query: Query, check_duplicate: bool = True
    ) -> list[str]:
        """Function that checks the primary keys from the query object and returns them as a list.

        Parameters:
            fv_query_obj: Query object from which the primary keys are extracted.
            check_duplicate: Flag to check if the primary keys are duplicated in the query.
        """
        root_feature_group_selected_features_name = {
            feature.name for feature in query._left_features
        }

        root_feature_group = query._left_feature_group

        root_feature_group_primary_keys_names = {
            feature.name for feature in root_feature_group.features if feature.primary
        }

        pk_names = {
            util.generate_fully_qualified_feature_name(root_feature_group, pk_name)
            if pk_name not in root_feature_group_selected_features_name
            else pk_name
            for pk_name in root_feature_group_primary_keys_names
        }

        pk_names.update(
            self._primary_keys_from_join(query._joins, check_duplicate, pk_names)
        )

        return list(pk_names)

    def _get_eventtimes_from_query(
        self, query: Query, check_duplicate: bool = True
    ) -> list[str]:
        """Function that checks the event times from the query object and returns them as a list.

        Parameters:
            fv_query_obj: Query object from which the event times are extracted.
            check_duplicate: Flag to check if the event times are duplicated in the query.
        """
        root_feature_group_selected_features_name = {
            feature.name for feature in query._left_features
        }

        root_feature_group = query._left_feature_group

        root_feature_group_event_time = root_feature_group.event_time

        et_names = {
            util.generate_fully_qualified_feature_name(
                root_feature_group, root_feature_group_event_time
            )
            if root_feature_group_event_time
            not in root_feature_group_selected_features_name
            else root_feature_group_event_time
        }

        et_names.update(
            self._event_time_from_join(query._joins, check_duplicate, et_names)
        )

        return list(et_names)

    def _check_if_exists_with_prefix(self, f_name, f_set):
        if f_name in f_set:
            raise FeatureStoreException(
                f"Provided feature {f_name} is ambiguous and exists in more than one feature groups."
                "To avoid this error specify prefix in the join."
            )
        return f_name

    def get_logging_feature_from_dataframe(
        self,
        feature_view_obj: feature_view.FeatureView,
        dataframes: list[
            pd.DataFrame | pl.DataFrame | TypeVar("pyspark.sql.DataFrame")
        ] = None,
    ) -> list[feature.Feature]:
        """Function to extract features from a logging dataframe.

        Parameters:
            feature_view_obj: The feature view object for which logging features are to be extracted.
            dataframes: The dataframes from which logging features are to be extracted.

        Returns:
            `List[feature.Feature]`. List of features extracted from the logging feature view provided.
        """
        logging_features = []
        logging_feature_names = []
        label_features = [
            feature.name for feature in feature_view_obj.features if feature.label
        ]
        for df in dataframes:
            if df is not None and engine.get_instance().check_supported_dataframe(df):
                supported_df = engine.get_instance().convert_to_default_dataframe(df)
                features = engine.get_instance().parse_schema_feature_group(
                    supported_df
                )
                for feature in features:
                    # If the feature names in a label, then add the prefix to get the correct logging feature name.
                    if feature.name in label_features:
                        feature.name = (
                            constants.FEATURE_LOGGING.PREFIX_PREDICTIONS + feature.name
                        )
                    if feature.name not in logging_feature_names:
                        logging_feature_names.append(feature.name)
                        logging_features.append(feature)
        return logging_features

    def enable_feature_logging(
        self, fv, extra_log_columns: feature.Feature | dict[str, Any] = None
    ):
        """Function to enable feature logging for a feature view. This function creates logging feature groups for the feature view.

        Parameters:
            fv: Feature view object to enable feature logging for.
            extra_log_columns: List of features to be logged.

        Returns:
            `FeatureView`. Feature view object with feature logging enabled.
        """
        logging_features = (
            [
                feature.Feature.from_response_json(feat)
                if isinstance(feat, dict)
                else feat
                for feat in extra_log_columns
            ]
            if extra_log_columns
            else []
        )

        feature_logging = FeatureLogging(extra_logging_columns=logging_features)
        self._feature_view_api.enable_feature_logging(
            fv.name, fv.version, feature_logging
        )
        fv.logging_enabled = True
        return fv

    def get_feature_logging(self, fv):
        return self._feature_view_api.get_feature_logging(fv.name, fv.version)

    def _get_logging_fg(self, fv, transformed):
        feature_logging = self.get_feature_logging(fv)
        if feature_logging:
            return feature_logging.get_feature_group(transformed)
        return feature_logging

    def log_features(
        self,
        fv: feature_view.FeatureView,
        feature_logging: FeatureLogging,
        logs: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame") = None,
        untransformed_features: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame") = None,
        transformed_features: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame") = None,
        predictions: pd.DataFrame
        | pl.DataFrame
        | list[dict]
        | list[list]
        | np.ndarray
        | None = None,
        inference_helper_columns: pd.DataFrame
        | pl.DataFrame
        | list[dict]
        | list[list]
        | np.ndarray
        | None = None,
        request_parameters: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | None = None,
        event_time: pd.DataFrame
        | pl.DataFrame
        | list[dict]
        | list[list]
        | np.ndarray
        | None = None,
        serving_keys: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | None = None,
        extra_logging_features: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | None = None,
        request_id: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | list[dict]
        | np.ndarray
        | None = None,
        write_options: dict[str, Any] | None = None,
        training_dataset_version: int | None = None,
        hsml_model=None,
        model_name: str | None = None,
        model_version: int | None = None,
        logger: FeatureLogger = None,
    ):
        """Function that collects all the logging data and writes it to the logging feature groups.

        The functions collects the data as a list of dictionaries or a dataframe.
        - In the online inference, the data is collected as a list of dictionaries and logged asynchronously using the passed feature logger.
        - In the batch inference, the data is collected as a dataframe and written directly to the

        """
        if (
            logs is None
            and untransformed_features is None
            and transformed_features is None
            and predictions is None
            and inference_helper_columns is None
            and request_parameters is None
            and event_time is None
            and serving_keys is None
            and request_id is None
            and extra_logging_features is None
        ):
            return None

        default_write_options = {
            "start_offline_materialization": False,
        }
        if write_options:
            default_write_options.update(write_options)
        results = []

        # FSTORE-1871 combines the untransformed and transformed logging feature groups.
        # Transformed and untransformed logging features groups are retrived here to maintain backwards compatibility.
        if logger:
            logger.log(
                **{
                    key: (
                        self._get_feature_logging_data(
                            fv=fv,
                            logging_feature_group=fg,
                            logging_data=logs,
                            untransformed_features=untransformed_features,
                            transformed_features=transformed_features,
                            predictions=predictions,
                            helper_columns=inference_helper_columns,
                            request_parameters=request_parameters,
                            event_time=event_time,
                            serving_keys=serving_keys,
                            request_id=request_id,
                            extra_logging_features=extra_logging_features,
                            training_dataset_version=training_dataset_version,
                            hsml_model=hsml_model,
                            model_name=model_name,
                            model_version=model_version,
                            return_list=True,
                        )
                        if fg
                        else None
                    )
                    for key, fg in [
                        (
                            constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES,
                            feature_logging.untransformed_features,
                        ),
                        (
                            constants.FEATURE_LOGGING.TRANSFORMED_FEATURES,
                            feature_logging.transformed_features,
                        ),
                    ]
                }
            )

        else:
            for fg in [
                feature_logging.untransformed_features,
                feature_logging.transformed_features,
            ]:
                if fg:
                    logging_df = self._get_feature_logging_data(
                        fv=fv,
                        logging_feature_group=fg,
                        logging_data=logs,
                        untransformed_features=untransformed_features,
                        transformed_features=transformed_features,
                        predictions=predictions,
                        helper_columns=inference_helper_columns,
                        request_parameters=request_parameters,
                        event_time=event_time,
                        serving_keys=serving_keys,
                        request_id=request_id,
                        extra_logging_features=extra_logging_features,
                        training_dataset_version=training_dataset_version,
                        hsml_model=hsml_model,
                        return_list=False,
                        model_name=model_name,
                        model_version=model_version,
                    )
                    # Schema validation is disabled for logging feature groups to always allow logging data to be ingested.
                    results.append(
                        fg.insert(
                            logging_df,
                            write_options=default_write_options,
                            validation_options={"schema_validation": False},
                        )
                    )
        return results

    def _get_feature_logging_data(
        self,
        fv: feature_view.FeatureView,
        logging_feature_group: feature_group.FeatureGroup,
        logging_data: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        untransformed_features: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        transformed_features: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        predictions: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        helper_columns: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        request_parameters: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        event_time: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        serving_keys: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        request_id: str
        | pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        extra_logging_features: pd.DataFrame
        | pl.DataFrame
        | list[list]
        | np.ndarray
        | TypeVar("pyspark.sql.DataFrame")
        | None = None,
        training_dataset_version: int | None = None,
        hsml_model: str | None = None,
        model_name: str | None = None,
        model_version: int | None = None,
        return_list: bool = False,
    ):
        """Function that collects all the logging data and returns it as a dataframe or a list of dictionaries.

        The function uses the feature view schema to find the expected columns for each type of logging data and uses that to extract the correct columns
        from the provided dataframes and create dataframes/list of dictionaries with the correct schema if the data is provided as a list.
        It also check if the hopsworks logging metadata is present and uses it to fill in any missing logging data.
        """
        # Check if logging metadata is provided in the logging data object.
        # If provided, use the metadata to fill in any missing logging data.
        # User provided logging data takes precedence over the metadata.
        logging_meta_data: LoggingMetaData = getattr(
            logging_data, "hopsworks_logging_metadata", None
        )
        if logging_meta_data:
            # Setting logging data to None since all parameters required for logging are already in the metadata object.
            logging_data = (
                None if isinstance(logging_data, (list, np.ndarray)) else logging_data
            )

            transformed_features = (
                logging_meta_data.transformed_features
                if transformed_features is None
                else transformed_features
            )
            untransformed_features = (
                logging_meta_data.untransformed_features
                if untransformed_features is None
                else untransformed_features
            )
            serving_keys = (
                logging_meta_data.serving_keys if serving_keys is None else serving_keys
            )
            helper_columns = (
                logging_meta_data.inference_helper
                if helper_columns is None
                else helper_columns
            )
            request_parameters = (
                logging_meta_data.request_parameters
                if request_parameters is None
                else request_parameters
            )
            event_time = (
                logging_meta_data.event_time if event_time is None else event_time
            )

        logging_feature_group_features = list(logging_feature_group.features)
        logging_feature_group_feature_names = [
            feature.name for feature in logging_feature_group_features
        ]
        logging_features = [
            feature_name
            for feature_name in logging_feature_group_feature_names
            if feature_name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
        ]
        model_name = (
            model_name if model_name else hsml_model.name if hsml_model else None
        )
        model_version = (
            model_version
            if model_version
            else hsml_model.version
            if hsml_model
            else None
        )

        if return_list:
            logging_data, additional_logging_features, missing_logging_features = (
                engine.get_instance().get_feature_logging_list(
                    logging_data=logging_data,
                    logging_feature_group_features=logging_feature_group_features,
                    logging_feature_group_feature_names=logging_feature_group_feature_names,
                    logging_features=logging_features,
                    transformed_features=(
                        transformed_features,
                        fv._transformed_feature_names,
                        constants.FEATURE_LOGGING.TRANSFORMED_FEATURES,
                    ),
                    untransformed_features=(
                        untransformed_features,
                        fv._untransformed_feature_names,
                        constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES,
                    ),
                    predictions=(
                        predictions,
                        list(fv._label_column_names),
                        constants.FEATURE_LOGGING.PREDICTIONS,
                    ),
                    serving_keys=(
                        serving_keys,
                        fv._required_serving_key_names,
                        constants.FEATURE_LOGGING.SERVING_KEYS,
                    ),
                    helper_columns=(
                        helper_columns,
                        fv.inference_helper_columns,
                        constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS,
                    ),
                    request_parameters=(
                        request_parameters,
                        fv.request_parameters,
                        constants.FEATURE_LOGGING.REQUEST_PARAMETERS,
                    ),
                    event_time=(
                        event_time,
                        [fv._root_feature_group_event_time_column_name],
                        constants.FEATURE_LOGGING.EVENT_TIME,
                    ),
                    request_id=(
                        [request_id] if isinstance(request_id, str) else request_id,
                        [constants.FEATURE_LOGGING.REQUEST_ID_COLUMN_NAME],
                        constants.FEATURE_LOGGING.REQUEST_ID,
                    ),
                    extra_logging_features=(
                        extra_logging_features,
                        fv._extra_logging_column_names,
                        constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES,
                    ),
                    td_col_name=constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME,
                    time_col_name=constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME,
                    model_col_name=constants.FEATURE_LOGGING.MODEL_COLUMN_NAME,
                    training_dataset_version=training_dataset_version,
                    model_name=model_name,
                    model_version=model_version,
                )
            )
        else:
            logging_data, additional_logging_features, missing_logging_features = (
                engine.get_instance().get_feature_logging_df(
                    logging_data=logging_data,
                    logging_feature_group_features=logging_feature_group_features,
                    logging_feature_group_feature_names=logging_feature_group_feature_names,
                    logging_features=logging_features,
                    transformed_features=(
                        transformed_features,
                        fv._transformed_feature_names,
                        constants.FEATURE_LOGGING.TRANSFORMED_FEATURES,
                    ),
                    untransformed_features=(
                        untransformed_features,
                        fv._untransformed_feature_names,
                        constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES,
                    ),
                    predictions=(
                        predictions,
                        list(fv._label_column_names),
                        constants.FEATURE_LOGGING.PREDICTIONS,
                    ),
                    serving_keys=(
                        serving_keys,
                        fv._required_serving_key_names,
                        constants.FEATURE_LOGGING.SERVING_KEYS,
                    ),
                    helper_columns=(
                        helper_columns,
                        fv.inference_helper_columns,
                        constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS,
                    ),
                    request_parameters=(
                        request_parameters,
                        fv.request_parameters,
                        constants.FEATURE_LOGGING.REQUEST_PARAMETERS,
                    ),
                    event_time=(
                        event_time,
                        [fv._root_feature_group_event_time_column_name],
                        constants.FEATURE_LOGGING.EVENT_TIME,
                    ),
                    request_id=(
                        [request_id] if isinstance(request_id, str) else request_id,
                        [constants.FEATURE_LOGGING.REQUEST_ID_COLUMN_NAME],
                        constants.FEATURE_LOGGING.REQUEST_ID,
                    ),
                    extra_logging_features=(
                        extra_logging_features,
                        fv._extra_logging_column_names,
                        constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES,
                    ),
                    td_col_name=constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME,
                    time_col_name=constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME,
                    model_col_name=constants.FEATURE_LOGGING.MODEL_COLUMN_NAME,
                    training_dataset_version=training_dataset_version,
                    model_name=model_name,
                    model_version=model_version,
                )
            )

        if additional_logging_features:
            _logger.info(
                f"The following columns : `{'`, `'.join(sorted(additional_logging_features))}` are additional columns in the logged dataframe and is not present in the logging feature groups. They will be ignored."
            )
        if missing_logging_features:
            _logger.info(
                f"The following columns : `{'`, `'.join(sorted(missing_logging_features))}` are missing in the logged dataframe. Setting them to None."
            )

        return logging_data

    def read_feature_logs(
        self,
        fv,
        start_time: str | int | datetime | datetime.date | None = None,
        end_time: str | int | datetime | datetime.date | None = None,
        filter: Filter | Logic | None = None,
        transformed: bool | None = False,
        training_dataset_version=None,
        hsml_model=None,
        model_name: str | None = None,
        model_version: int | None = None,
    ):
        fg = self._get_logging_fg(fv, transformed)
        fv_feat_name_map = self._get_fv_feature_name_map(fv)
        query = fg.select_all()
        if start_time:
            query = query.filter(
                fg.get_feature(constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME)
                >= start_time
            )
        if end_time:
            query = query.filter(
                fg.get_feature(constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME)
                <= end_time
            )
        if training_dataset_version:
            query = query.filter(
                fg.get_feature(
                    constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME
                )
                == training_dataset_version
            )
        if hsml_model:
            query = query.filter(
                (
                    fg.get_feature(constants.FEATURE_LOGGING.MODEL_COLUMN_NAME)
                    == hsml_model.name
                )
                and (
                    fg.get_feature(constants.FEATURE_LOGGING.MODEL_VERSION_COLUMN_NAME)
                    == hsml_model.version
                )
            )
        if model_name:
            query = query.filter(
                fg.get_feature(constants.FEATURE_LOGGING.MODEL_COLUMN_NAME)
                == model_name
            )
        if model_version:
            query = query.filter(
                fg.get_feature(constants.FEATURE_LOGGING.MODEL_VERSION_COLUMN_NAME)
                == model_version
            )
        if filter:
            query = query.filter(
                self._convert_to_log_fg_filter(fg, fv, filter, fv_feat_name_map)
            )
        return engine.get_instance().read_feature_log(
            query, constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME
        )

    @staticmethod
    def get_hsml_model_value(hsml_model, model_name=None, model_version=None):
        if hsml_model:
            return f"{hsml_model.name}_{hsml_model.version}"
        if model_name and model_version:
            return f"{model_name}_{model_version}"
        return None

    def _convert_to_log_fg_filter(self, fg, fv, filter, fv_feat_name_map):
        if filter is None:
            return None

        if isinstance(filter, Logic):
            return Logic(
                filter.type,
                left_f=self._convert_to_log_fg_filter(fv, filter.left_f),
                right_f=self._convert_to_log_fg_filter(fv, filter.right_f),
                left_l=self._convert_to_log_fg_filter(fv, filter.left_l),
                right_l=self._convert_to_log_fg_filter(fv, filter.right_l),
            )
        if isinstance(filter, Filter):
            fv_feature_name = fv_feat_name_map.get(
                f"{filter.feature.feature_group_id}_{filter.feature.name}"
            )
            if fv_feature_name is None:
                raise FeatureStoreException(
                    "Filter feature {filter.feature.name} does not exist in feature view feature."
                )
            return Filter(
                fg.get_feature(filter.feature.name),
                filter.condition,
                filter.value,
            )
        raise FeatureStoreException("Accept only Filter or Logic")

    def _get_fv_feature_name_map(self, fv) -> dict[str, str]:
        result_dict = {}
        for td_feature in fv.features:
            fg_feature_key = (
                f"{td_feature.feature_group.id}_{td_feature.feature_group_feature_name}"
            )
            result_dict[fg_feature_key] = td_feature.name
        return result_dict

    def get_log_timeline(
        self,
        fv,
        wallclock_time: str | int | datetime | datetime.date | None = None,
        limit: int | None = None,
        transformed: bool | None = False,
    ) -> dict[str, dict[str, str]]:
        fg = self._get_logging_fg(fv, transformed)
        if fg:
            return fg.commit_details(wallclock_time=wallclock_time, limit=limit)
        return {}

    def pause_logging(self, fv):
        self._feature_view_api.pause_feature_logging(fv.name, fv.version)

    def resume_logging(self, fv):
        self._feature_view_api.resume_feature_logging(fv.name, fv.version)

    def materialize_feature_logs(self, fv, wait, transform):
        # FSTORE-1871 combines the untransformed and transformed logging feature groups.
        # Here we are checking are fetching both transformed and untransformed logging feature groups to maintain backwards compatibility.
        if transform is None:
            feature_logging = self.get_feature_logging(fv)
            logging_feature_groups = [
                feature_logging.untransformed_features,
                feature_logging.transformed_features,
            ]
            jobs = [fg.materialization_job for fg in logging_feature_groups if fg]
        else:
            jobs = [self._get_logging_fg(fv, transform).materialization_job]
        for job in jobs:
            job.run(await_termination=False)
        if wait:
            for job in jobs:
                job._wait_for_job(wait)
        return jobs

    def delete_feature_logs(self, fv, feature_logging, transformed):
        self._feature_view_api.delete_feature_logs(fv.name, fv.version, transformed)
        feature_logging.update(self.get_feature_logging(fv))
