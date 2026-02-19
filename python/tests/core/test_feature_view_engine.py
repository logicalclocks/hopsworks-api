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

from unittest.mock import MagicMock

import pandas as pd
import pytest
from hopsworks_common import constants
from hopsworks_common.core.type_systems import create_extended_type
from hsfs import (
    engine,
    feature,
    feature_group,
    feature_view,
    split_statistics,
    training_dataset,
)
from hsfs.client.exceptions import FeatureStoreException
from hsfs.constructor import fs_query
from hsfs.constructor.query import Query
from hsfs.core import arrow_flight_client, feature_view_engine
from hsfs.core import data_source as ds
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.core.feature_logging import LoggingMetaData
from hsfs.hopsworks_udf import udf
from hsfs.serving_key import ServingKey
from hsfs.storage_connector import BigQueryConnector, StorageConnector
from hsfs.training_dataset_feature import TrainingDatasetFeature
from hsfs.transformation_function import TransformationFunction, TransformationType


engine.init("python")
fg1 = feature_group.FeatureGroup(
    name="test1",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[feature.Feature("id"), feature.Feature("label")],
    id=11,
    stream=False,
)

fg2 = feature_group.FeatureGroup(
    name="test2",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[feature.Feature("id"), feature.Feature("label")],
    id=12,
    stream=False,
)

fg3 = feature_group.FeatureGroup(
    name="test2",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[feature.Feature("id"), feature.Feature("label")],
    id=13,
    stream=False,
)

fg4 = feature_group.FeatureGroup(
    name="test4",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[feature.Feature("id4"), feature.Feature("label4")],
    id=14,
    stream=False,
)

fg5 = feature_group.FeatureGroup(
    name="test5",
    version=1,
    featurestore_id=99,
    primary_key=["primary_key"],
    event_time="event_time",
    partition_key=[],
    features=[
        feature.Feature("primary_key", primary=True, type="bigint"),
        feature.Feature("event_time", type="timestamp"),
        feature.Feature("feature_1", type="float"),
        feature.Feature("feature_2", type="float"),
    ],
    id=11,
    stream=False,
    featurestore_name="test_fs",
)

fg6 = feature_group.FeatureGroup(
    name="test6",
    version=1,
    featurestore_id=99,
    primary_key=["primary_key"],
    event_time="event_time",
    partition_key=[],
    features=[
        feature.Feature("primary_key", primary=True, type="bigint"),
        feature.Feature("event_time", type="timestamp"),
        feature.Feature("feature_1", type="float"),
        feature.Feature("feature_2", type="float"),
    ],
    id=11,
    stream=False,
    featurestore_name="test_fs",
)

fg1._feature_store_name = "test_fs1"
fg2._feature_store_name = "test_fs2"
fg3._feature_store_name = "test_fs3"
fg4._feature_store_name = "test_fs4"

query = fg1.select_all()


class TestFeatureViewEngine:
    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_view_url = "test_url"

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value=feature_view_url,
        )
        mock_print = mocker.patch("builtins.print")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name", query=query, featurestore_id=feature_store_id
        )

        # Act
        fv_engine.save(fv)

        # Assert
        assert mock_fv_api.return_value.post.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature view created successfully, explore it at \n{feature_view_url}"
        )

    def test_save_time_travel_query(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_view_url = "test_url"

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value=feature_view_url,
        )
        mock_print = mocker.patch("builtins.print")
        mock_warning = mocker.patch("warnings.warn")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query.as_of(1000000000),
            featurestore_id=feature_store_id,
        )

        # Act
        fv_engine.save(fv)

        # Assert
        assert mock_fv_api.return_value.post.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature view created successfully, explore it at \n{feature_view_url}"
        )
        assert mock_warning.call_args[0][0] == (
            "`as_of` argument in the `Query` will be ignored because"
            " feature view does not support time travel query."
        )

    def test_save_time_travel_sub_query(self, mocker):
        # Arrange
        feature_store_id = 99
        feature_view_url = "test_url"

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value=feature_view_url,
        )
        mock_print = mocker.patch("builtins.print")
        mock_warning = mocker.patch("warnings.warn")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_all().join(
                fg2.select_all().as_of("20221010"), prefix="fg2"
            ),
            featurestore_id=feature_store_id,
        )

        # Act
        fv_engine.save(fv)

        # Assert
        assert mock_fv_api.return_value.post.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature view created successfully, explore it at \n{feature_view_url}"
        )
        assert mock_warning.call_args[0][0] == (
            "`as_of` argument in the `Query` will be ignored because"
            " feature view does not support time travel query."
        )

    def template_save_label_success(self, mocker, _query, label, label_fg_id):
        # Arrange
        feature_store_id = 99
        feature_view_url = "test_url"

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value=feature_view_url,
        )
        mock_print = mocker.patch("builtins.print")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )
        fv = feature_view.FeatureView(
            name="fv_name",
            query=_query,
            featurestore_id=feature_store_id,
            labels=[label],
        )
        # Act
        fv_engine.save(fv)

        # Assert
        assert len(fv._features) == 1
        assert (
            fv._features[0].name == "label"
            and fv._features[0].label
            and fv._features[0].feature_group.id == label_fg_id
        )
        assert mock_fv_api.return_value.post.call_count == 1
        assert mock_print.call_count == 1
        assert (
            mock_print.call_args[0][0]
            == f"Feature view created successfully, explore it at \n{feature_view_url}"
        )

    def template_save_label_fail(self, mocker, _query, label, msg):
        # Arrange
        feature_store_id = 99
        feature_view_url = "test_url"

        mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )
        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_feature_view_url",
            return_value=feature_view_url,
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )
        fv = feature_view.FeatureView(
            name="fv_name",
            query=_query,
            featurestore_id=feature_store_id,
            labels=[label],
        )
        # Act
        with pytest.raises(FeatureStoreException) as e_info:
            fv_engine.save(fv)

        # Assert
        assert str(e_info.value) == msg
        assert mock_fv_api.return_value.post.call_count == 0

    def test_save_label_unique_col(self, mocker):
        _query = fg1.select_all().join(fg4.select_all())
        self.template_save_label_success(mocker, _query, "label", fg1.id)

    def test_save_label_selected_in_head_query_1(self, mocker):
        _query = fg1.select_all().join(fg2.select_all(), prefix="fg2_")
        self.template_save_label_success(mocker, _query, "label", fg1.id)

    def test_save_label_selected_in_head_query_2(self, mocker):
        _query = fg1.select_all().join(fg2.select_all(), prefix="fg2_")
        self.template_save_label_success(mocker, _query, "fg2_label", fg2.id)

    def test_save_multiple_label_selected_1(self, mocker):
        _query = (
            fg1.select_except(["label"])
            .join(fg2.select_all(), prefix="fg2_")
            .join(fg3.select_all(), prefix="fg3_")
        )
        self.template_save_label_fail(
            mocker,
            _query,
            "label",
            Query.ERROR_MESSAGE_FEATURE_AMBIGUOUS.format("label"),
        )

    def test_save_multiple_label_selected_2(self, mocker):
        _query = (
            fg1.select_except(["label"])
            .join(fg2.select_all(), prefix="fg2_")
            .join(fg3.select_all(), prefix="fg3_")
        )
        self.template_save_label_success(mocker, _query, "fg2_label", fg2.id)

    def test_save_multiple_label_selected_3(self, mocker):
        _query = (
            fg1.select_except(["label"])
            .join(fg2.select_all(), prefix="fg2_")
            .join(fg3.select_all(), prefix="fg3_")
        )
        print(fg2["label"])
        self.template_save_label_success(mocker, _query, "fg3_label", fg3.id)

    def test_save_label_selected_in_join_only_1(self, mocker):
        _query = fg1.select_except(["label"]).join(fg2.select_all(), prefix="fg2_")
        self.template_save_label_success(mocker, _query, "label", fg2.id)

    def test_save_label_selected_in_join_only_2(self, mocker):
        _query = fg1.select_except(["label"]).join(fg2.select_all(), prefix="fg2_")
        self.template_save_label_success(mocker, _query, "fg2_label", fg2.id)

    def test_save_label_selected_in_join_only_3(self, mocker):
        _query = fg1.select_except(["label"]).join(fg2.select_all(), prefix="fg2_")
        self.template_save_label_fail(
            mocker,
            _query,
            "none",
            Query.ERROR_MESSAGE_FEATURE_NOT_FOUND.format("none"),
        )

    def test_save_label_self_join_1(self, mocker):
        _query = fg1.select_all().join(fg1.select_all(), prefix="fg1_")
        self.template_save_label_success(mocker, _query, "label", fg1.id)

    def test_save_label_self_join_2(self, mocker):
        _query = fg1.select_all().join(fg1.select_all(), prefix="fg1_")
        self.template_save_label_success(mocker, _query, "fg1_label", fg1.id)

    def test_get_name(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )
        fv1 = feature_view.FeatureView(
            name="fv_name",
            version=2,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_api.return_value.get_by_name.return_value = [fv, fv1]

        # Act
        result = fv_engine.get(name="test")

        # Assert
        assert mock_fv_api.return_value.get_by_name_version.call_count == 0
        assert mock_fv_api.return_value.get_by_name.call_count == 1
        assert len(result) == 2

    def test_get_name_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_api.return_value.get_by_name.return_value = fv

        # Act
        fv_engine.get(name="test", version=1)

        # Assert
        assert mock_fv_api.return_value.get_by_name_version.call_count == 1
        assert mock_fv_api.return_value.get_by_name.call_count == 0

    def test_delete_name(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.delete(name="test")

        # Assert
        assert mock_fv_api.return_value.delete_by_name_version.call_count == 0
        assert mock_fv_api.return_value.delete_by_name.call_count == 1

    def test_delete_name_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.delete(name="test", version=1)

        # Assert
        assert mock_fv_api.return_value.delete_by_name_version.call_count == 1
        assert mock_fv_api.return_value.delete_by_name.call_count == 0

    def test_get_batch_query(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.get_batch_query(
            feature_view_obj=fv,
            start_time=1000000000,
            end_time=2000000000,
            with_label=False,
        )

        # Assert
        assert mock_fv_api.return_value.get_batch_query.call_count == 1

    def test_get_batch_query_string(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_qc_api = mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi"
        )
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )
        _fs_query = fs_query.FsQuery(
            query="query",
            on_demand_feature_groups=None,
            hudi_cached_feature_groups=None,
            pit_query=None,
        )
        mock_qc_api.return_value.construct_query.return_value = _fs_query

        # Act
        result = fv_engine.get_batch_query_string(
            feature_view_obj=fv, start_time=1000000000, end_time=2000000000
        )

        # Assert
        assert result == "query"
        assert mock_fv_api.return_value.get_batch_query.call_count == 1
        assert mock_qc_api.return_value.construct_query.call_count == 1

    def test_get_batch_query_string_pit_query(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_qc_api = mocker.patch(
            "hsfs.core.query_constructor_api.QueryConstructorApi"
        )
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )
        _fs_query = fs_query.FsQuery(
            query="query",
            on_demand_feature_groups=None,
            hudi_cached_feature_groups=None,
            pit_query="pit_query",
        )

        mock_qc_api.return_value.construct_query.return_value = _fs_query

        # Act
        result = fv_engine.get_batch_query_string(
            feature_view_obj=fv, start_time=1000000000, end_time=2000000000
        )

        # Assert
        assert result == "pit_query"
        assert mock_fv_api.return_value.get_batch_query.call_count == 1
        assert mock_qc_api.return_value.construct_query.call_count == 1

    def test_create_training_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mock_fv_engine_compute_training_dataset = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.create_training_dataset(
            feature_view_obj=None, training_dataset_obj=None, user_write_options={}
        )

        # Assert
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_compute_training_dataset.call_count == 1

    def test_get_training_data(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_create_training_data_metadata.return_value = td

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=False),
        ]

        mock_fv_engine_create_training_data_metadata.return_value.splits = {}

        # Act
        fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

        assert len(td.schema) == len(fv.schema)
        for td_feature, expected_td_feature in zip(td.schema, fv.schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_get_training_data_transformations(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_create_training_data_metadata.return_value = td

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id"),
                feature.Feature("feature1"),
                feature.Feature("feature2"),
                feature.Feature("label1"),
                feature.Feature("label2"),
            ],
            id=14,
            stream=False,
        )

        fg._feature_store_name = "test_transformations_fs"

        @udf(return_type=[int, int], drop=["feature1"])
        def transform_feature_drop(feature1):
            return pd.DataFrame({"a": feature1 + 1, "b": feature1 + 2})

        @udf(return_type=[int])
        def transform_feature_no_drop(feature2):
            return feature2 + 2

        @udf(return_type=int, drop=["label1"])
        def transform_labels_drop(label1):
            return label1 + 2

        @udf(return_type=[int, int])
        def transform_labels_no_drop(label2):
            return pd.DataFrame({"a": label2 + 1, "b": label2 + 2})

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            labels=["label1", "label2"],
            transformation_functions=[
                transform_feature_drop,
                transform_feature_no_drop,
                transform_labels_drop,
                transform_labels_no_drop,
            ],
        )

        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature1", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(name="label1", type="bigint", label=True),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
        ]

        mock_fv_engine_create_training_data_metadata.return_value.splits = {}

        # Act
        fv_engine.get_training_data(feature_view_obj=fv)

        expected_schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_0", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_1", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_no_drop_feature2_", type="bigint", label=False
            ),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
            TrainingDatasetFeature(
                name="transform_labels_drop_label1_", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_0", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_1", type="bigint", label=True
            ),
        ]

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

        assert len(expected_schema) == len(td.schema)
        for td_feature, expected_td_feature in zip(td.schema, expected_schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_get_training_data_td_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_get_training_dataset_metadata.return_value = td

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=False),
        ]

        mock_fv_engine_get_training_dataset_metadata.return_value.splits = {}

        # Act
        fv_engine.get_training_data(feature_view_obj=fv, training_dataset_version=1)

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 3
        assert mock_fv_engine_create_training_data_metadata.call_count == 0
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

        assert len(td.schema) == len(fv.schema)
        for td_feature, expected_td_feature in zip(td.schema, fv.schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_get_training_data_td_version_transformations(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_get_training_dataset_metadata.return_value = td

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id"),
                feature.Feature("feature1"),
                feature.Feature("feature2"),
                feature.Feature("label1"),
                feature.Feature("label2"),
            ],
            id=14,
            stream=False,
        )

        fg._feature_store_name = "test_transformations_fs"

        @udf(return_type=[int, int], drop=["feature1"])
        def transform_feature_drop(feature1):
            return pd.DataFrame({"a": feature1 + 1, "b": feature1 + 2})

        @udf(return_type=[int])
        def transform_feature_no_drop(feature2):
            return feature2 + 2

        @udf(return_type=int, drop=["label1"])
        def transform_labels_drop(label1):
            return label1 + 2

        @udf(return_type=[int, int])
        def transform_labels_no_drop(label2):
            return pd.DataFrame({"a": label2 + 1, "b": label2 + 2})

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            labels=["label1", "label2"],
            transformation_functions=[
                transform_feature_drop,
                transform_feature_no_drop,
                transform_labels_drop,
                transform_labels_no_drop,
            ],
        )

        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature1", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(name="label1", type="bigint", label=True),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
        ]

        mock_fv_engine_get_training_dataset_metadata.return_value.splits = {}

        # Act
        fv_engine.get_training_data(feature_view_obj=fv, training_dataset_version=1)

        expected_schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_0", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_1", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_no_drop_feature2_", type="bigint", label=False
            ),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
            TrainingDatasetFeature(
                name="transform_labels_drop_label1_", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_0", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_1", type="bigint", label=True
            ),
        ]

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 3
        assert mock_fv_engine_create_training_data_metadata.call_count == 0
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

        assert len(expected_schema) == len(td.schema)
        for td_feature, expected_td_feature in zip(td.schema, expected_schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_get_training_data_type_in_memory(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")

        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.training_dataset_type = training_dataset.TrainingDataset.IN_MEMORY
        mock_fv_engine_create_training_data_metadata.return_value.IN_MEMORY = (
            training_dataset.TrainingDataset.IN_MEMORY
        )
        mock_fv_engine_create_training_data_metadata.return_value.splits = []

        # Act
        fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_get_training_data_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )

        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")

        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(
            name="ss",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        ss1 = split_statistics.SplitStatistics(
            name="ss1",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        splits = [ss, ss1]
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        fv_engine.get_training_data(feature_view_obj=fv, splits=splits)

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_check_splits_0(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(
            name="ss",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        splits = []
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.get_training_data(feature_view_obj=fv, splits=[ss])

        # Assert
        assert (
            str(e_info.value)
            == "Incorrect `get` method is used. Use `feature_view.get_training_data` instead."
        )
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_check_splits_2(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(
            name="ss",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        ss1 = split_statistics.SplitStatistics(
            name="ss1",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        splits = [ss, ss1]
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Incorrect `get` method is used. Use `feature_view.get_train_test_split` instead."
        )
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_get_training_data_check_splits_3(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine._set_event_time")
        mock_fv_engine_create_training_data_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._create_training_data_metadata"
        )
        mocker.patch("hsfs.engine.get_instance")
        mock_fv_engine_read_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_from_storage_connector"
        )
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        ss = split_statistics.SplitStatistics(
            name="ss",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        ss1 = split_statistics.SplitStatistics(
            name="ss1",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        ss2 = split_statistics.SplitStatistics(
            name="ss2",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        splits = [ss, ss1, ss2]
        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_fv_engine_create_training_data_metadata.return_value.splits = splits

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.get_training_data(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Incorrect `get` method is used. Use `feature_view.get_train_validation_test_split` instead."
        )
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 0
        assert mock_fv_engine_create_training_data_metadata.call_count == 1
        assert mock_fv_engine_read_from_storage_connector.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_recreate_training_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mock_fv_engine_compute_training_dataset = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset"
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_get_training_dataset_metadata.return_value = td

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )

        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=False),
        ]

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.recreate_training_dataset(
            feature_view_obj=fv,
            training_dataset_version=None,
            statistics_config=None,
            user_write_options={},
        )

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_fv_engine_compute_training_dataset.call_count == 1
        assert len(td.schema) == len(fv.schema)
        for td_feature, expected_td_feature in zip(td.schema, fv.schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_recreate_training_dataset_transformations(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mock_fv_engine_compute_training_dataset = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset"
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_get_training_dataset_metadata.return_value = td

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id"),
                feature.Feature("feature1"),
                feature.Feature("feature2"),
                feature.Feature("label1"),
                feature.Feature("label2"),
            ],
            id=14,
            stream=False,
        )

        @udf(return_type=[int, int], drop=["feature1"])
        def transform_feature_drop(feature1):
            return pd.DataFrame({"a": feature1 + 1, "b": feature1 + 2})

        @udf(return_type=[int])
        def transform_feature_no_drop(feature2):
            return feature2 + 2

        @udf(return_type=int, drop=["label1"])
        def transform_labels_drop(label1):
            return label1 + 2

        @udf(return_type=[int, int])
        def transform_labels_no_drop(label2):
            return pd.DataFrame({"a": label2 + 1, "b": label2 + 2})

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            labels=["label1", "label2"],
            transformation_functions=[
                transform_feature_drop,
                transform_feature_no_drop,
                transform_labels_drop,
                transform_labels_no_drop,
            ],
        )

        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature1", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(name="label1", type="bigint", label=True),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
        ]

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.recreate_training_dataset(
            feature_view_obj=fv,
            training_dataset_version=None,
            statistics_config=None,
            user_write_options={},
        )

        expected_schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_0", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_1", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_no_drop_feature2_", type="bigint", label=False
            ),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
            TrainingDatasetFeature(
                name="transform_labels_drop_label1_", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_0", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_1", type="bigint", label=True
            ),
        ]

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_fv_engine_compute_training_dataset.call_count == 1
        assert len(expected_schema) == len(td.schema)
        for td_feature, expected_td_feature in zip(td.schema, expected_schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_read_from_storage_connector(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_read_dir_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_dir_from_storage_connector"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine._read_from_storage_connector(
            training_data_obj=td,
            splits=None,
            read_options=None,
            with_primary_keys=None,
            primary_keys=None,
            with_event_time=None,
            event_time=None,
            with_training_helper_columns=None,
            training_helper_columns=None,
            feature_view_features=[],
            dataframe_type="default",
        )

        # Assert
        assert mock_fv_engine_read_dir_from_storage_connector.call_count == 1
        assert (
            mock_fv_engine_read_dir_from_storage_connector.call_args[0][1]
            == "location/test"
        )

    def test_read_from_storage_connector_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_read_dir_from_storage_connector = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._read_dir_from_storage_connector"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )
        ss = split_statistics.SplitStatistics(
            name="ss",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        ss1 = split_statistics.SplitStatistics(
            name="ss1",
            feature_descriptive_statistics=[FeatureDescriptiveStatistics("amount")],
        )
        splits = [ss, ss1]

        # Act
        fv_engine._read_from_storage_connector(
            training_data_obj=td,
            splits=splits,
            read_options=None,
            with_primary_keys=None,
            primary_keys=None,
            with_event_time=None,
            event_time=None,
            with_training_helper_columns=None,
            training_helper_columns=None,
            feature_view_features=[],
            dataframe_type="default",
        )

        # Assert
        assert mock_fv_engine_read_dir_from_storage_connector.call_count == 2
        assert (
            mock_fv_engine_read_dir_from_storage_connector.mock_calls[0][1][1]
            == "location/ss"
        )
        assert (
            mock_fv_engine_read_dir_from_storage_connector.mock_calls[1][1][1]
            == "location/ss1"
        )

    def test_read_dir_from_storage_connector(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_drop_helper_columns = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._drop_helper_columns"
        )
        mock_sc_read = mocker.patch("hsfs.storage_connector.StorageConnector.read")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine._read_dir_from_storage_connector(
            training_data_obj=td,
            path="test",
            read_options=None,
            with_primary_keys=False,
            primary_keys=[],
            with_event_time=False,
            event_time=[],
            with_training_helper_columns=False,
            training_helper_columns=[],
            feature_view_features=[],
            dataframe_type="default",
        )

        # Assert
        assert mock_sc_read.call_count == 1
        assert mock_drop_helper_columns.call_count == 3

    def test_read_dir_from_storage_connector_file_not_found(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_sc_read = mocker.patch(
            "hsfs.storage_connector.StorageConnector.read",
            side_effect=FileNotFoundError(),
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        with pytest.raises(FileNotFoundError) as e_info:
            fv_engine._read_dir_from_storage_connector(
                training_data_obj=td,
                path="test",
                read_options=None,
                with_primary_keys=None,
                primary_keys=None,
                with_event_time=None,
                event_time=None,
                with_training_helper_columns=None,
                training_helper_columns=None,
                feature_view_features=[],
                dataframe_type="default",
            )

        # Assert
        assert (
            str(e_info.value)
            == "Failed to read dataset from test. Check if path exists or recreate a training dataset."
        )
        assert mock_sc_read.call_count == 1

    def test_compute_training_dataset(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mock_td_engine = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.compute_training_dataset(
                feature_view_obj=None,
                user_write_options={},
                training_dataset_obj=None,
                training_dataset_version=None,
            )

        # Assert
        assert str(e_info.value) == "No training dataset object or version is provided"
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 0
        assert mock_td_engine.return_value.read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_compute_training_dataset_td(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mock_td_engine = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )
        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=False),
        ]

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=fv,
            user_write_options={},
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 1
        assert mock_td_engine.return_value.read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0
        assert len(td.schema) == len(fv.schema)
        for td_feature, expected_td_feature in zip(td.schema, fv.schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_compute_training_dataset_td_transformations(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mock_td_engine = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fg = feature_group.FeatureGroup(
            name="test",
            version=1,
            featurestore_id=99,
            primary_key=["id"],
            partition_key=[],
            features=[
                feature.Feature("id"),
                feature.Feature("feature1"),
                feature.Feature("feature2"),
                feature.Feature("label1"),
                feature.Feature("label2"),
            ],
            id=14,
            stream=False,
        )

        @udf(return_type=[int, int], drop=["feature1"])
        def transform_feature_drop(feature1):
            return pd.DataFrame({"a": feature1 + 1, "b": feature1 + 2})

        @udf(return_type=[int])
        def transform_feature_no_drop(feature2):
            return feature2 + 2

        @udf(return_type=int, drop=["label1"])
        def transform_labels_drop(label1):
            return label1 + 2

        @udf(return_type=[int, int])
        def transform_labels_no_drop(label2):
            return pd.DataFrame({"a": label2 + 1, "b": label2 + 2})

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            labels=["label1", "label2"],
            transformation_functions=[
                transform_feature_drop,
                transform_feature_no_drop,
                transform_labels_drop,
                transform_labels_no_drop,
            ],
        )
        # Setting feature view schema
        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature1", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(name="label1", type="bigint", label=True),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
        ]

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=fv,
            user_write_options={},
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        expected_schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="feature2", type="bigint", label=False),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_0", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_drop_feature1_1", type="bigint", label=False
            ),
            TrainingDatasetFeature(
                name="transform_feature_no_drop_feature2_", type="bigint", label=False
            ),
            TrainingDatasetFeature(name="label2", type="bigint", label=True),
            TrainingDatasetFeature(
                name="transform_labels_drop_label1_", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_0", type="bigint", label=True
            ),
            TrainingDatasetFeature(
                name="transform_labels_no_drop_label2_1", type="bigint", label=True
            ),
        ]

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 1
        assert mock_td_engine.return_value.read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0
        assert len(expected_schema) == len(td.schema)
        for td_feature, expected_td_feature in zip(td.schema, expected_schema):
            assert td_feature.name == expected_td_feature.name
            assert td_feature.type == expected_td_feature.type
            assert td_feature.label == expected_td_feature.label

    def test_compute_training_dataset_td_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mock_td_engine = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        mock_fv_engine_get_training_dataset_metadata.return_value = td

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=fv,
            user_write_options={},
            training_dataset_obj=None,
            training_dataset_version=1,
        )

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 2
        assert mock_td_engine.return_value.read.call_count == 0
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 0

    def test_compute_training_dataset_td_spark_type_split(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mock_td_engine = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=fv,
            user_write_options={},
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 1
        assert mock_td_engine.return_value.read.call_count == 1
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_compute_training_dataset_td_spark_type(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_fv_engine_get_training_dataset_metadata = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch("hsfs.engine.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="spark")
        mock_td_engine = mocker.patch(
            "hsfs.core.training_dataset_engine.TrainingDatasetEngine"
        )
        mock_fv_engine_compute_training_dataset_statistics = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.compute_training_dataset_statistics"
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"1": 1, "2": 2},
        )

        # Act
        fv_engine.compute_training_dataset(
            feature_view_obj=fv,
            user_write_options={},
            training_dataset_obj=td,
            training_dataset_version=None,
        )

        # Assert
        assert mock_fv_engine_get_training_dataset_metadata.call_count == 1
        assert mock_td_engine.return_value.read.call_count == 2
        assert mock_fv_engine_compute_training_dataset_statistics.call_count == 1

    def test_compute_training_dataset_statistics_default(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df=None
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_and_save_split_statistics.call_count == 0
        )
        assert mock_s_engine.return_value.compute_and_save_statistics.call_count == 1

    def test_compute_training_dataset_statistics_enabled(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )
        td.statistics_config.enabled = True

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df=None
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_and_save_split_statistics.call_count == 0
        )
        assert mock_s_engine.return_value.compute_and_save_statistics.call_count == 1

    def test_compute_training_dataset_statistics_enabled_splits(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"1": 1, "2": 2},
        )
        td.statistics_config.enabled = True

        # Act
        with pytest.raises(ValueError) as e_info:
            fv_engine.compute_training_dataset_statistics(
                feature_view_obj=None,
                training_dataset_obj=td,
                td_df=None,
            )

        # Assert
        assert (
            str(e_info.value)
            == "Provided dataframes should be in dict format 'split': dataframe"
        )
        assert (
            mock_s_engine.return_value.compute_and_save_split_statistics.call_count == 0
        )
        assert mock_s_engine.return_value.compute_and_save_statistics.call_count == 0

    def test_compute_training_dataset_statistics_enabled_splits_td_df(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"1": 1, "2": 2},
        )
        td.statistics_config.enabled = True

        # Act
        fv_engine.compute_training_dataset_statistics(
            feature_view_obj=None, training_dataset_obj=td, td_df={}
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_and_save_split_statistics.call_count == 1
        )
        assert mock_s_engine.return_value.compute_and_save_statistics.call_count == 0

    def test_get_training_dataset_metadata_no_transformations(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )
        # Setting feature view schema
        mock_fv_api.return_value.get_training_dataset_by_version.return_value = td

        # Act
        result = fv_engine._get_training_dataset_metadata(
            feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_fv_api.return_value.get_training_dataset_by_version.call_count == 1
        assert result == td

    def test_create_training_data_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            location="location",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )
        fv.schema = "schema"
        fv.transformation_functions = None

        mock_fv_api.return_value.create_training_dataset.return_value = td

        # Act
        _ = fv_engine._create_training_data_metadata(
            feature_view_obj=fv, training_dataset_obj=None
        )

        # Assert
        assert mock_fv_api.return_value.create_training_dataset.call_count == 1

    def test_delete_training_data(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_data(feature_view_obj=fv, training_data_version=None)

        # Assert
        assert mock_fv_api.return_value.delete_training_data_version.call_count == 0
        assert mock_fv_api.return_value.delete_training_data.call_count == 1

    def test_delete_training_data_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_data(feature_view_obj=fv, training_data_version=1)

        # Assert
        assert mock_fv_api.return_value.delete_training_data_version.call_count == 1
        assert mock_fv_api.return_value.delete_training_data.call_count == 0

    def test_delete_training_dataset_only(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_dataset_only(
            feature_view_obj=fv, training_data_version=None
        )

        # Assert
        assert (
            mock_fv_api.return_value.delete_training_dataset_only_version.call_count
            == 0
        )
        assert mock_fv_api.return_value.delete_training_dataset_only.call_count == 1

    def test_delete_training_dataset_only_version(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_fv_api = mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        fv_engine.delete_training_dataset_only(
            feature_view_obj=fv, training_data_version=1
        )

        # Assert
        assert (
            mock_fv_api.return_value.delete_training_dataset_only_version.call_count
            == 1
        )
        assert mock_fv_api.return_value.delete_training_dataset_only.call_count == 0

    def test_get_batch_data(self, mocker):
        # Arrange
        feature_store_id = 99

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf_value = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._check_feature_group_accessibility"
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine.get_batch_query")
        mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine._get_training_dataset_metadata"
        )
        tf_engine_patch = mocker.patch(
            "hsfs.core.transformation_function_engine.TransformationFunctionEngine"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.get_batch_data(
            feature_view_obj=None,
            start_time=None,
            end_time=None,
            training_dataset_version=None,
            transformation_functions=[tf_value],
            read_options=None,
        )

        # Assert
        assert (
            tf_engine_patch.apply_transformation_functions.call_args[1][
                "transformation_functions"
            ][0].hopsworks_udf.function_name
            == tf_value.hopsworks_udf.function_name
        )
        assert tf_engine_patch.apply_transformation_functions.call_count == 1

    def test_add_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.add_tag(
            feature_view_obj=None, name=None, value=None, training_dataset_version=None
        )

        # Assert
        assert mock_tags_api.return_value.add.call_count == 1

    def test_delete_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.delete_tag(
            feature_view_obj=None, name=None, training_dataset_version=None
        )

        # Assert
        assert mock_tags_api.return_value.delete.call_count == 1

    def test_get_tag(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.get_tag(
            feature_view_obj=None, name=None, training_dataset_version=None
        )

        # Assert
        assert mock_tags_api.return_value.get.call_count == 1

    def test_get_tags(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_tags_api = mocker.patch("hsfs.core.tags_api.TagsApi")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        # Act
        fv_engine.get_tags(feature_view_obj=None, training_dataset_version=None)

        # Assert
        assert mock_tags_api.return_value.get.call_count == 1

    def test_check_feature_group_accessibility(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.is_cache_feature_group_only.return_value = False

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 1

    def test_check_feature_group_accessibility_cache_feature_group(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.is_cache_feature_group_only.return_value = True

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 1

    def test_check_feature_group_accessibility_get_type_python(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hopsworks_common.client.get_instance")  # for arrow_flight_client
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch("hsfs.engine.get_type")
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.is_cache_feature_group_only.return_value = False
        mock_engine_get_type.return_value = "python"

        # Act
        with pytest.raises(NotImplementedError) as e_info:
            fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert (
            str(e_info.value)
            == "Python kernel can only read from cached feature groups. When using external feature groups please use `feature_view.create_training_data` instead. If you are using spines, use a Spark Kernel."
        )
        assert mock_engine_get_type.call_count == 1

    def test_check_feature_group_accessibility_arrow_flight(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hopsworks_common.client.get_instance")  # for arrow_flight_client
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        afc = arrow_flight_client.get_instance()
        afc._disabled_for_session = False
        afc._enabled_on_cluster = True

        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")
        connector = BigQueryConnector(0, "BigQueryConnector", 99)
        mock_external_feature_group = feature_group.ExternalFeatureGroup(
            primary_key="", data_source=ds.DataSource(storage_connector=connector)
        )
        mock_feature_group = MagicMock(spec=feature_group.FeatureGroup)
        mock_constructor_query.featuregroups = [
            mock_feature_group,
            mock_external_feature_group,
        ]

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        assert arrow_flight_client.get_instance().is_enabled()
        assert arrow_flight_client.supports(mock_constructor_query.featuregroups)

        # Act
        # All good if we don't get an exception
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

    def test_check_feature_group_accessibility_arrow_flight_unsupported(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hopsworks_common.client.get_instance")  # for arrow_flight_client
        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        afc = arrow_flight_client.get_instance()
        afc._is_enabled = True

        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        class FakeConnector(StorageConnector):
            def __init__(self):
                self._type = "Fake"

            def spark_options(self):
                pass

        connector = FakeConnector()
        mock_external_feature_group = feature_group.ExternalFeatureGroup(
            primary_key="", data_source=ds.DataSource(storage_connector=connector)
        )
        mock_feature_group = MagicMock(spec=feature_group.FeatureGroup)
        mock_constructor_query.featuregroups = [
            mock_feature_group,
            mock_external_feature_group,
        ]

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        # Act
        with pytest.raises(NotImplementedError):
            fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

    def test_check_feature_group_accessibility_cache_feature_group_get_type_python(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch(
            "hsfs.engine.get_type", return_value="python"
        )
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.is_cache_feature_group_only.return_value = True

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 1

    def test_check_feature_group_accessibility_cache_feature_group_get_type_hive(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_engine_get_type = mocker.patch(
            "hsfs.engine.get_type", return_value="python"
        )
        mock_constructor_query = mocker.patch("hsfs.constructor.query.Query")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=mock_constructor_query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_constructor_query.is_cache_feature_group_only.return_value = True
        mock_engine_get_type.return_value = "hive"

        # Act
        fv_engine._check_feature_group_accessibility(feature_view_obj=fv)

        # Assert
        assert mock_engine_get_type.call_count == 1

    def test_get_feature_view_url(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mock_client_get_instance = mocker.patch("hopsworks_common.client.get_instance")
        mock_util_get_hostname_replaced_url = mocker.patch(
            "hsfs.util.get_hostname_replaced_url"
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            query=query,
            featurestore_id=feature_store_id,
            labels=[],
        )

        mock_client_get_instance.return_value._project_id = 50

        # Act
        fv_engine._get_feature_view_url(fv=fv)

        # Assert
        assert mock_util_get_hostname_replaced_url.call_count == 1
        assert (
            mock_util_get_hostname_replaced_url.call_args[0][0]
            == "/p/50/fs/99/fv/fv_name/version/1"
        )

    def test_get_logging_feature_from_dataframe(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        engine = mocker.patch("hsfs.engine.get_instance", autospec=True).return_value
        mocker.patch("hsfs.engine.get_type", return_value="python")
        engine.check_supported_dataframe.return_value = True
        engine.parse_schema_feature_group.return_value = [
            feature.Feature(name="id", type="bigint"),
            feature.Feature(name="feature1", type="bigint"),
            feature.Feature(name="feature2", type="string"),
            feature.Feature(name="label", type="string"),
        ]

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
        )

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        df1 = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "feature1": [1, 2, 3],
            }
        )

        df2 = pd.DataFrame(
            {
                "id": [4, 5, 6],
                "feature2": ["a", "b", "c"],
                "label": [0, 1, 0],
            }
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
        ]

        # Act
        dataframe_logging_features = fv_engine.get_logging_feature_from_dataframe(
            fv, [df1, df2]
        )

        # Assert
        assert [feature.name for feature in dataframe_logging_features] == [
            "id",
            "feature1",
            "feature2",
            "predicted_label",
        ]

    def test_get_feature_logging_data(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocked_engine = mocker.Mock()
        mocker.patch("hsfs.engine.get_instance", return_value=mocked_engine)
        mocked_engine.get_feature_logging_df.return_value = (pd.DataFrame, None, None)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        @udf(return_type=int, drop=["rp_1", "rp_2"])
        def on_demand_feature(rp_1, rp_2) -> pd.Series:
            return rp_1 + rp_2

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
            inference_helper_columns=["inference_helper_1"],
            extra_logging_features=["extra_1", "extra_2"],
            transformation_functions=[on_demand_feature],
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
            TrainingDatasetFeature("primary_key", primary=True, type="bigint"),
            TrainingDatasetFeature("event_time", type="timestamp"),
            TrainingDatasetFeature("feature_1", type="double"),
            TrainingDatasetFeature("feature_2", type="double"),
            TrainingDatasetFeature("feature_3", type="int"),
            TrainingDatasetFeature("min_max_scaler_feature_3", type="double"),
            TrainingDatasetFeature("extra_1", type="string"),
            TrainingDatasetFeature("extra_2", type="int"),
            TrainingDatasetFeature("inference_helper_1", type="double"),
            TrainingDatasetFeature("on_demand_feature", type="double"),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        fv._request_parameters = ["rp_1", "rp_2"]
        fv._FeatureView__extra_logging_column_names = ["extra_1", "extra_2"]

        logging_data = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
                "feature_3": [7, 8, 9],
                "min_max_scaler_feature_3": [0.7, 0.8, 0.9],
                "extra_1": ["a", "b", "c"],
                "extra_2": [10, 11, 12],
                "inference_helper_1": [0.01, 0.02, 0.03],
            }
        )

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        # Prepare test data for various logging components
        untransformed_data = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
            }
        )

        transformed_data = pd.DataFrame(
            {"primary_key": [1, 2, 3], "min_max_scaler_feature_3": [0.7, 0.8, 0.9]}
        )

        predictions_data = pd.DataFrame({"label": [1, 0, 1]})

        helper_columns_data = pd.DataFrame({"inference_helper_1": [0.01, 0.02, 0.03]})

        request_parameters_data = pd.DataFrame(
            {"rp_1": [10, 20, 30], "rp_2": [5, 15, 25]}
        )

        event_time_data = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"])}
        )

        serving_keys_data = pd.DataFrame({"primary_key": [1, 2, 3]})

        extra_logging_data = pd.DataFrame(
            {"extra_1": ["a", "b", "c"], "extra_2": [10, 11, 12]}
        )

        request_id_data = "test_request_123"
        training_dataset_version = 5

        # Act
        _ = fv_engine._get_feature_logging_data(
            fv=fv,
            logging_feature_group=fg,
            logging_data=logging_data,
            untransformed_features=untransformed_data,
            transformed_features=transformed_data,
            predictions=predictions_data,
            helper_columns=helper_columns_data,
            request_parameters=request_parameters_data,
            event_time=event_time_data,
            serving_keys=serving_keys_data,
            request_id=request_id_data,
            extra_logging_features=extra_logging_data,
            training_dataset_version=training_dataset_version,
            hsml_model=mock_hsml_model,
            model_name="test_model",
            model_version=1,
            return_list=False,
        )

        # Assert
        assert mocked_engine.get_feature_logging_df.call_count == 1
        call_args = mocked_engine.get_feature_logging_df.call_args

        # Verify the main arguments
        assert call_args[1]["logging_data"] is logging_data
        assert len(call_args[1]["logging_feature_group_features"]) == len(fg.features)
        assert sorted(call_args[1]["logging_feature_group_feature_names"]) == sorted(
            [feat.name for feat in fg.features]
        )
        assert sorted(call_args[1]["logging_features"]) == sorted(
            [
                feat.name
                for feat in fg.features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ]
        )

        # Verify transformed_features tuple structure: (data, feature_names, constant)
        transformed_tuple = call_args[1]["transformed_features"]
        assert transformed_tuple[0] is transformed_data
        assert transformed_tuple[1] == fv._transformed_feature_names
        assert transformed_tuple[2] == constants.FEATURE_LOGGING.TRANSFORMED_FEATURES

        # Verify untransformed_features tuple structure
        untransformed_tuple = call_args[1]["untransformed_features"]
        assert untransformed_tuple[0] is untransformed_data
        assert untransformed_tuple[1] == fv._untransformed_feature_names
        assert (
            untransformed_tuple[2] == constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES
        )

        # Verify predictions tuple structure
        predictions_tuple = call_args[1]["predictions"]
        assert predictions_tuple[0] is predictions_data
        assert predictions_tuple[1] == list(fv._label_column_names)
        assert predictions_tuple[2] == constants.FEATURE_LOGGING.PREDICTIONS

        # Verify serving_keys tuple structure
        serving_keys_tuple = call_args[1]["serving_keys"]
        assert serving_keys_tuple[0] is serving_keys_data
        assert serving_keys_tuple[1] == fv._required_serving_key_names
        assert serving_keys_tuple[2] == constants.FEATURE_LOGGING.SERVING_KEYS

        # Verify helper_columns tuple structure
        helper_columns_tuple = call_args[1]["helper_columns"]
        assert helper_columns_tuple[0] is helper_columns_data
        assert helper_columns_tuple[1] == fv.inference_helper_columns
        assert (
            helper_columns_tuple[2]
            == constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS
        )

        # Verify request_parameters tuple structure
        request_parameters_tuple = call_args[1]["request_parameters"]
        assert request_parameters_tuple[0] is request_parameters_data
        assert request_parameters_tuple[1] == fv.request_parameters
        assert (
            request_parameters_tuple[2] == constants.FEATURE_LOGGING.REQUEST_PARAMETERS
        )

        # Verify event_time tuple structure
        event_time_tuple = call_args[1]["event_time"]
        assert event_time_tuple[0] is event_time_data
        assert event_time_tuple[1] == [fv._root_feature_group_event_time_column_name]
        assert event_time_tuple[2] == constants.FEATURE_LOGGING.EVENT_TIME

        # Verify request_id tuple structure (string should be wrapped in list)
        request_id_tuple = call_args[1]["request_id"]
        assert request_id_tuple[0] == [request_id_data]
        assert request_id_tuple[1] == [constants.FEATURE_LOGGING.REQUEST_ID_COLUMN_NAME]
        assert request_id_tuple[2] == constants.FEATURE_LOGGING.REQUEST_ID

        # Verify extra_logging_features tuple structure
        extra_logging_tuple = call_args[1]["extra_logging_features"]
        assert extra_logging_tuple[0] is extra_logging_data
        assert extra_logging_tuple[1] == fv._extra_logging_column_names
        assert (
            extra_logging_tuple[2] == constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES
        )

        # Verify metadata column names
        assert (
            call_args[1]["td_col_name"]
            == constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME
        )
        assert (
            call_args[1]["time_col_name"]
            == constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME
        )
        assert (
            call_args[1]["model_col_name"]
            == constants.FEATURE_LOGGING.MODEL_COLUMN_NAME
        )

        # Verify other parameters
        assert call_args[1]["training_dataset_version"] == training_dataset_version
        assert call_args[1]["model_name"] == "test_model"
        assert call_args[1]["model_version"] == 1

    def test_get_feature_logging_data_logging_metadata(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocked_engine = mocker.Mock()
        mocker.patch("hsfs.engine.get_instance", return_value=mocked_engine)
        mocked_engine.get_feature_logging_df.return_value = (pd.DataFrame, None, None)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        @udf(return_type=int, drop=["rp_1", "rp_2"])
        def on_demand_feature(rp_1, rp_2) -> pd.Series:
            return rp_1 + rp_2

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
            inference_helper_columns=["inference_helper_1"],
            extra_logging_features=["extra_1", "extra_2"],
            transformation_functions=[on_demand_feature],
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
            TrainingDatasetFeature("primary_key", primary=True, type="bigint"),
            TrainingDatasetFeature("event_time", type="timestamp"),
            TrainingDatasetFeature("feature_1", type="double"),
            TrainingDatasetFeature("feature_2", type="double"),
            TrainingDatasetFeature("feature_3", type="int"),
            TrainingDatasetFeature("min_max_scaler_feature_3", type="double"),
            TrainingDatasetFeature("extra_1", type="string"),
            TrainingDatasetFeature("extra_2", type="int"),
            TrainingDatasetFeature("inference_helper_1", type="double"),
            TrainingDatasetFeature("on_demand_feature", type="double"),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        fv._request_parameters = ["rp_1", "rp_2"]
        fv._FeatureView__extra_logging_column_names = ["extra_1", "extra_2"]

        logging_data = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
                "feature_3": [7, 8, 9],
                "min_max_scaler_feature_3": [0.7, 0.8, 0.9],
                "extra_1": ["a", "b", "c"],
                "extra_2": [10, 11, 12],
                "inference_helper_1": [0.01, 0.02, 0.03],
            }
        )

        logging_metadata = LoggingMetaData()

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        # Prepare test data for various logging components
        logging_metadata.untransformed_features = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
            }
        )

        logging_metadata.transformed_features = pd.DataFrame(
            {"primary_key": [1, 2, 3], "min_max_scaler_feature_3": [0.7, 0.8, 0.9]}
        )

        predictions_data = pd.DataFrame({"label": [1, 0, 1]})

        logging_metadata.inference_helper = pd.DataFrame(
            {"inference_helper_1": [0.01, 0.02, 0.03]}
        )

        logging_metadata.request_parameters = pd.DataFrame(
            {"rp_1": [10, 20, 30], "rp_2": [5, 15, 25]}
        )

        logging_metadata.event_time = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"])}
        )

        logging_metadata.serving_keys = pd.DataFrame({"primary_key": [1, 2, 3]})

        extra_logging_data = pd.DataFrame(
            {"extra_1": ["a", "b", "c"], "extra_2": [10, 11, 12]}
        )

        request_id_data = "test_request_123"
        training_dataset_version = 5
        logging_data.hopsworks_logging_metadata = logging_metadata

        # Act
        _ = fv_engine._get_feature_logging_data(
            fv=fv,
            logging_feature_group=fg,
            logging_data=logging_data,
            predictions=predictions_data,
            request_id=request_id_data,
            extra_logging_features=extra_logging_data,
            training_dataset_version=training_dataset_version,
            hsml_model=mock_hsml_model,
            model_name="test_model",
            model_version=1,
            return_list=False,
        )

        # Assert
        assert mocked_engine.get_feature_logging_df.call_count == 1
        call_args = mocked_engine.get_feature_logging_df.call_args

        # Verify the main arguments
        assert call_args[1]["logging_data"] is logging_data
        assert len(call_args[1]["logging_feature_group_features"]) == len(fg.features)
        assert sorted(call_args[1]["logging_feature_group_feature_names"]) == sorted(
            [feat.name for feat in fg.features]
        )
        assert sorted(call_args[1]["logging_features"]) == sorted(
            [
                feat.name
                for feat in fg.features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ]
        )

        # Verify transformed_features tuple structure: (data, feature_names, constant)
        transformed_tuple = call_args[1]["transformed_features"]
        assert transformed_tuple[0] is logging_metadata.transformed_features
        assert transformed_tuple[1] == fv._transformed_feature_names
        assert transformed_tuple[2] == constants.FEATURE_LOGGING.TRANSFORMED_FEATURES

        # Verify untransformed_features tuple structure
        untransformed_tuple = call_args[1]["untransformed_features"]
        assert untransformed_tuple[0] is logging_metadata.untransformed_features
        assert untransformed_tuple[1] == fv._untransformed_feature_names
        assert (
            untransformed_tuple[2] == constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES
        )

        # Verify predictions tuple structure
        predictions_tuple = call_args[1]["predictions"]
        assert predictions_tuple[0] is predictions_data
        assert predictions_tuple[1] == list(fv._label_column_names)
        assert predictions_tuple[2] == constants.FEATURE_LOGGING.PREDICTIONS

        # Verify serving_keys tuple structure
        serving_keys_tuple = call_args[1]["serving_keys"]
        assert serving_keys_tuple[0] is logging_metadata.serving_keys
        assert serving_keys_tuple[1] == fv._required_serving_key_names
        assert serving_keys_tuple[2] == constants.FEATURE_LOGGING.SERVING_KEYS

        # Verify helper_columns tuple structure
        helper_columns_tuple = call_args[1]["helper_columns"]
        assert helper_columns_tuple[0] is logging_metadata.inference_helper
        assert helper_columns_tuple[1] == fv.inference_helper_columns
        assert (
            helper_columns_tuple[2]
            == constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS
        )

        # Verify request_parameters tuple structure
        request_parameters_tuple = call_args[1]["request_parameters"]
        assert request_parameters_tuple[0] is logging_metadata.request_parameters
        assert request_parameters_tuple[1] == fv.request_parameters
        assert (
            request_parameters_tuple[2] == constants.FEATURE_LOGGING.REQUEST_PARAMETERS
        )

        # Verify event_time tuple structure
        event_time_tuple = call_args[1]["event_time"]
        assert event_time_tuple[0] is logging_metadata.event_time
        assert event_time_tuple[1] == [fv._root_feature_group_event_time_column_name]
        assert event_time_tuple[2] == constants.FEATURE_LOGGING.EVENT_TIME

        # Verify request_id tuple structure (string should be wrapped in list)
        request_id_tuple = call_args[1]["request_id"]
        assert request_id_tuple[0] == [request_id_data]
        assert request_id_tuple[1] == [constants.FEATURE_LOGGING.REQUEST_ID_COLUMN_NAME]
        assert request_id_tuple[2] == constants.FEATURE_LOGGING.REQUEST_ID

        # Verify extra_logging_features tuple structure
        extra_logging_tuple = call_args[1]["extra_logging_features"]
        assert extra_logging_tuple[0] is extra_logging_data
        assert extra_logging_tuple[1] == fv._extra_logging_column_names
        assert (
            extra_logging_tuple[2] == constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES
        )

        # Verify metadata column names
        assert (
            call_args[1]["td_col_name"]
            == constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME
        )
        assert (
            call_args[1]["time_col_name"]
            == constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME
        )
        assert (
            call_args[1]["model_col_name"]
            == constants.FEATURE_LOGGING.MODEL_COLUMN_NAME
        )

        # Verify other parameters
        assert call_args[1]["training_dataset_version"] == training_dataset_version
        assert call_args[1]["model_name"] == "test_model"
        assert call_args[1]["model_version"] == 1

    def test_get_feature_logging_data_logging_override(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocked_engine = mocker.Mock()
        mocker.patch("hsfs.engine.get_instance", return_value=mocked_engine)
        mocked_engine.get_feature_logging_df.return_value = (pd.DataFrame, None, None)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        @udf(return_type=int, drop=["rp_1", "rp_2"])
        def on_demand_feature(rp_1, rp_2) -> pd.Series:
            return rp_1 + rp_2

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
            inference_helper_columns=["inference_helper_1"],
            extra_logging_features=["extra_1", "extra_2"],
            transformation_functions=[on_demand_feature],
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
            TrainingDatasetFeature("primary_key", primary=True, type="bigint"),
            TrainingDatasetFeature("event_time", type="timestamp"),
            TrainingDatasetFeature("feature_1", type="double"),
            TrainingDatasetFeature("feature_2", type="double"),
            TrainingDatasetFeature("feature_3", type="int"),
            TrainingDatasetFeature("min_max_scaler_feature_3", type="double"),
            TrainingDatasetFeature("extra_1", type="string"),
            TrainingDatasetFeature("extra_2", type="int"),
            TrainingDatasetFeature("inference_helper_1", type="double"),
            TrainingDatasetFeature("on_demand_feature", type="double"),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        fv._request_parameters = ["rp_1", "rp_2"]
        fv._FeatureView__extra_logging_column_names = ["extra_1", "extra_2"]

        logging_data = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
                "feature_3": [7, 8, 9],
                "min_max_scaler_feature_3": [0.7, 0.8, 0.9],
                "extra_1": ["a", "b", "c"],
                "extra_2": [10, 11, 12],
                "inference_helper_1": [0.01, 0.02, 0.03],
            }
        )

        logging_metadata = LoggingMetaData()

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        # Prepare test data for various logging components
        logging_metadata.untransformed_features = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
            }
        )

        untransformed_features = pd.DataFrame(
            {
                "primary_key": [11, 22, 33],
                "feature_1": [1.1, 2.2, 3.3],
                "feature_2": [1.4, 2.5, 3.6],
            }
        )

        logging_metadata.transformed_features = pd.DataFrame(
            {"primary_key": [1, 2, 3], "min_max_scaler_feature_3": [0.7, 0.8, 0.9]}
        )

        transformed_features = pd.DataFrame(
            {"primary_key": [11, 12, 13], "min_max_scaler_feature_3": [1.7, 2.8, 3.9]}
        )

        predictions_data = pd.DataFrame({"label": [1, 0, 1]})

        logging_metadata.inference_helper = pd.DataFrame(
            {"inference_helper_1": [0.01, 0.02, 0.03]}
        )

        inference_helper = pd.DataFrame({"inference_helper_1": [1.01, 2.02, 3.03]})

        logging_metadata.request_parameters = pd.DataFrame(
            {"rp_1": [10, 20, 30], "rp_2": [5, 15, 25]}
        )

        request_parameters = pd.DataFrame(
            {"rp_1": [100, 200, 300], "rp_2": [50, 150, 250]}
        )

        logging_metadata.event_time = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"])}
        )

        event_time = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-02-01", "2025-03-02", "2025-04-03"])}
        )

        logging_metadata.serving_keys = pd.DataFrame({"primary_key": [1, 2, 3]})

        serving_keys = pd.DataFrame({"primary_key": [1, 2, 3]})

        extra_logging_data = pd.DataFrame(
            {"extra_1": ["a", "b", "c"], "extra_2": [10, 11, 12]}
        )

        request_id_data = "test_request_123"
        training_dataset_version = 5
        logging_data.hopsworks_logging_metadata = logging_metadata

        # Act
        _ = fv_engine._get_feature_logging_data(
            fv=fv,
            logging_feature_group=fg,
            transformed_features=transformed_features,
            untransformed_features=untransformed_features,
            logging_data=logging_data,
            predictions=predictions_data,
            helper_columns=inference_helper,
            request_parameters=request_parameters,
            event_time=event_time,
            serving_keys=serving_keys,
            request_id=request_id_data,
            extra_logging_features=extra_logging_data,
            training_dataset_version=training_dataset_version,
            hsml_model=mock_hsml_model,
            model_name="test_model",
            model_version=1,
            return_list=False,
        )

        # Assert
        assert mocked_engine.get_feature_logging_df.call_count == 1
        call_args = mocked_engine.get_feature_logging_df.call_args

        # Verify the main arguments
        assert call_args[1]["logging_data"] is logging_data
        assert len(call_args[1]["logging_feature_group_features"]) == len(fg.features)
        assert sorted(call_args[1]["logging_feature_group_feature_names"]) == sorted(
            [feat.name for feat in fg.features]
        )
        assert sorted(call_args[1]["logging_features"]) == sorted(
            [
                feat.name
                for feat in fg.features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ]
        )

        # Verify transformed_features tuple structure: (data, feature_names, constant)
        transformed_tuple = call_args[1]["transformed_features"]
        assert transformed_tuple[0] is not logging_metadata.transformed_features
        assert transformed_tuple[0] is transformed_features
        assert transformed_tuple[1] == fv._transformed_feature_names
        assert transformed_tuple[2] == constants.FEATURE_LOGGING.TRANSFORMED_FEATURES

        # Verify untransformed_features tuple structure
        untransformed_tuple = call_args[1]["untransformed_features"]
        assert untransformed_tuple[0] is not logging_metadata.untransformed_features
        assert untransformed_tuple[0] is untransformed_features
        assert untransformed_tuple[1] == fv._untransformed_feature_names
        assert (
            untransformed_tuple[2] == constants.FEATURE_LOGGING.UNTRANSFORMED_FEATURES
        )

        # Verify predictions tuple structure
        predictions_tuple = call_args[1]["predictions"]
        assert predictions_tuple[0] is predictions_data
        assert predictions_tuple[1] == list(fv._label_column_names)
        assert predictions_tuple[2] == constants.FEATURE_LOGGING.PREDICTIONS

        # Verify serving_keys tuple structure
        serving_keys_tuple = call_args[1]["serving_keys"]
        assert serving_keys_tuple[0] is not logging_metadata.serving_keys
        assert serving_keys_tuple[0] is serving_keys
        assert serving_keys_tuple[1] == fv._required_serving_key_names
        assert serving_keys_tuple[2] == constants.FEATURE_LOGGING.SERVING_KEYS

        # Verify helper_columns tuple structure
        helper_columns_tuple = call_args[1]["helper_columns"]
        assert helper_columns_tuple[0] is not logging_metadata.inference_helper
        assert helper_columns_tuple[0] is inference_helper
        assert helper_columns_tuple[1] == fv.inference_helper_columns
        assert (
            helper_columns_tuple[2]
            == constants.FEATURE_LOGGING.INFERENCE_HELPER_COLUMNS
        )

        # Verify request_parameters tuple structure
        request_parameters_tuple = call_args[1]["request_parameters"]
        assert request_parameters_tuple[0] is not logging_metadata.request_parameters
        assert request_parameters_tuple[0] is request_parameters
        assert request_parameters_tuple[1] == fv.request_parameters
        assert (
            request_parameters_tuple[2] == constants.FEATURE_LOGGING.REQUEST_PARAMETERS
        )

        # Verify event_time tuple structure
        event_time_tuple = call_args[1]["event_time"]
        assert event_time_tuple[0] is not logging_metadata.event_time
        assert event_time_tuple[0] is event_time
        assert event_time_tuple[1] == [fv._root_feature_group_event_time_column_name]
        assert event_time_tuple[2] == constants.FEATURE_LOGGING.EVENT_TIME

        # Verify request_id tuple structure (string should be wrapped in list)
        request_id_tuple = call_args[1]["request_id"]
        assert request_id_tuple[0] == [request_id_data]
        assert request_id_tuple[1] == [constants.FEATURE_LOGGING.REQUEST_ID_COLUMN_NAME]
        assert request_id_tuple[2] == constants.FEATURE_LOGGING.REQUEST_ID

        # Verify extra_logging_features tuple structure
        extra_logging_tuple = call_args[1]["extra_logging_features"]
        assert extra_logging_tuple[0] is extra_logging_data
        assert extra_logging_tuple[1] == fv._extra_logging_column_names
        assert (
            extra_logging_tuple[2] == constants.FEATURE_LOGGING.EXTRA_LOGGING_FEATURES
        )

        # Verify metadata column names
        assert (
            call_args[1]["td_col_name"]
            == constants.FEATURE_LOGGING.TRAINING_DATASET_VERSION_COLUMN_NAME
        )
        assert (
            call_args[1]["time_col_name"]
            == constants.FEATURE_LOGGING.LOG_TIME_COLUMN_NAME
        )
        assert (
            call_args[1]["model_col_name"]
            == constants.FEATURE_LOGGING.MODEL_COLUMN_NAME
        )

        # Verify other parameters
        assert call_args[1]["training_dataset_version"] == training_dataset_version
        assert call_args[1]["model_name"] == "test_model"
        assert call_args[1]["model_version"] == 1

    def test_get_feature_logging_data_return_list(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocked_engine = mocker.Mock()
        mocker.patch("hsfs.engine.get_instance", return_value=mocked_engine)
        mocked_engine.get_feature_logging_list.return_value = (pd.DataFrame, None, None)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        @udf(return_type=int, drop=["rp_1", "rp_2"])
        def on_demand_feature(rp_1, rp_2) -> pd.Series:
            return rp_1 + rp_2

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
            inference_helper_columns=["inference_helper_1"],
            extra_logging_features=["extra_1", "extra_2"],
            transformation_functions=[on_demand_feature],
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
            TrainingDatasetFeature("primary_key", primary=True, type="bigint"),
            TrainingDatasetFeature("event_time", type="timestamp"),
            TrainingDatasetFeature("feature_1", type="double"),
            TrainingDatasetFeature("feature_2", type="double"),
            TrainingDatasetFeature("feature_3", type="int"),
            TrainingDatasetFeature("min_max_scaler_feature_3", type="double"),
            TrainingDatasetFeature("extra_1", type="string"),
            TrainingDatasetFeature("extra_2", type="int"),
            TrainingDatasetFeature("inference_helper_1", type="double"),
            TrainingDatasetFeature("on_demand_feature", type="double"),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        fv._request_parameters = ["rp_1", "rp_2"]
        fv._FeatureView__extra_logging_column_names = ["extra_1", "extra_2"]

        logging_data = [
            {"primary_key": 1, "feature_1": 0.1, "feature_2": 0.4},
            {"primary_key": 2, "feature_1": 0.2, "feature_2": 0.5},
            {"primary_key": 3, "feature_1": 0.3, "feature_2": 0.6},
        ]

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        training_dataset_version = 3

        # Act
        _ = fv_engine._get_feature_logging_data(
            fv=fv,
            logging_feature_group=fg,
            logging_data=logging_data,
            untransformed_features=None,
            transformed_features=None,
            predictions=None,
            helper_columns=None,
            request_parameters=None,
            event_time=None,
            serving_keys=None,
            request_id=None,
            extra_logging_features=None,
            training_dataset_version=training_dataset_version,
            hsml_model=mock_hsml_model,
            model_name=None,
            model_version=None,
            return_list=True,
        )

        # Assert
        assert mocked_engine.get_feature_logging_list.call_count == 1
        assert (
            mocked_engine.get_feature_logging_df.call_count == 0
        )  # Should not be called when return_list=True

        call_args = mocked_engine.get_feature_logging_list.call_args

        # Verify the main arguments for list version
        assert call_args[1]["logging_data"] is logging_data
        assert len(call_args[1]["logging_feature_group_features"]) == len(fg.features)
        assert sorted(call_args[1]["logging_feature_group_feature_names"]) == sorted(
            [feat.name for feat in fg.features]
        )
        assert sorted(call_args[1]["logging_features"]) == sorted(
            [
                feat.name
                for feat in fg.features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ]
        )
        assert call_args[1]["training_dataset_version"] == training_dataset_version

    def test_get_feature_logging_data_return_list_logging_meta_data(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocked_engine = mocker.Mock()
        mocker.patch("hsfs.engine.get_instance", return_value=mocked_engine)
        mocked_engine.get_feature_logging_list.return_value = (pd.DataFrame, None, None)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        @udf(return_type=int, drop=["rp_1", "rp_2"])
        def on_demand_feature(rp_1, rp_2) -> pd.Series:
            return rp_1 + rp_2

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
            inference_helper_columns=["inference_helper_1"],
            extra_logging_features=["extra_1", "extra_2"],
            transformation_functions=[on_demand_feature],
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
            TrainingDatasetFeature("primary_key", primary=True, type="bigint"),
            TrainingDatasetFeature("event_time", type="timestamp"),
            TrainingDatasetFeature("feature_1", type="double"),
            TrainingDatasetFeature("feature_2", type="double"),
            TrainingDatasetFeature("feature_3", type="int"),
            TrainingDatasetFeature("min_max_scaler_feature_3", type="double"),
            TrainingDatasetFeature("extra_1", type="string"),
            TrainingDatasetFeature("extra_2", type="int"),
            TrainingDatasetFeature("inference_helper_1", type="double"),
            TrainingDatasetFeature("on_demand_feature", type="double"),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        fv._request_parameters = ["rp_1", "rp_2"]
        fv._FeatureView__extra_logging_column_names = ["extra_1", "extra_2"]

        logging_data = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
                "feature_3": [7, 8, 9],
                "min_max_scaler_feature_3": [0.7, 0.8, 0.9],
                "extra_1": ["a", "b", "c"],
                "extra_2": [10, 11, 12],
                "inference_helper_1": [0.01, 0.02, 0.03],
            }
        ).to_dict(orient="records")

        logging_metadata = LoggingMetaData()

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        # Prepare test data for various logging components
        logging_metadata.untransformed_features = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
            }
        ).values.tolist()

        logging_metadata.transformed_features = pd.DataFrame(
            {"primary_key": [1, 2, 3], "min_max_scaler_feature_3": [0.7, 0.8, 0.9]}
        ).values.tolist()

        predictions_data = pd.DataFrame({"label": [1, 0, 1]}).values.tolist()

        logging_metadata.inference_helper = pd.DataFrame(
            {"inference_helper_1": [0.01, 0.02, 0.03]}
        ).values.tolist()

        logging_metadata.request_parameters = pd.DataFrame(
            {"rp_1": [10, 20, 30], "rp_2": [5, 15, 25]}
        ).values.tolist()

        logging_metadata.event_time = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"])}
        ).values.tolist()

        logging_metadata.serving_keys = pd.DataFrame(
            {"primary_key": [1, 2, 3]}
        ).values.tolist()

        extra_logging_data = pd.DataFrame(
            {"extra_1": ["a", "b", "c"], "extra_2": [10, 11, 12]}
        ).values.tolist()

        request_id_data = ["test_request_123", "test_request_456", "test_request_789"]

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        training_dataset_version = 3

        extended_list = create_extended_type(list)
        logging_data = extended_list(logging_data)
        logging_data.hopsworks_logging_metadata = logging_metadata

        # Act
        _ = fv_engine._get_feature_logging_data(
            fv=fv,
            logging_feature_group=fg,
            logging_data=logging_data,
            predictions=predictions_data,
            request_id=request_id_data,
            extra_logging_features=extra_logging_data,
            training_dataset_version=training_dataset_version,
            hsml_model=mock_hsml_model,
            model_name="test_model",
            model_version=1,
            return_list=True,
        )

        # Assert
        assert mocked_engine.get_feature_logging_list.call_count == 1
        assert (
            mocked_engine.get_feature_logging_df.call_count == 0
        )  # Should not be called when return_list=True

        call_args = mocked_engine.get_feature_logging_list.call_args

        # Verify the main arguments for list version
        assert (
            call_args[1]["logging_data"] is None
        )  # Should be None since all the data is in the metadata
        assert len(call_args[1]["logging_feature_group_features"]) == len(fg.features)
        assert sorted(call_args[1]["logging_feature_group_feature_names"]) == sorted(
            [feat.name for feat in fg.features]
        )
        assert sorted(call_args[1]["logging_features"]) == sorted(
            [
                feat.name
                for feat in fg.features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ]
        )
        assert call_args[1]["training_dataset_version"] == training_dataset_version
        assert (
            call_args[1]["untransformed_features"][0]
            is logging_metadata.untransformed_features
        )
        assert (
            call_args[1]["transformed_features"][0]
            is logging_metadata.transformed_features
        )
        assert call_args[1]["predictions"][0] is predictions_data
        assert call_args[1]["serving_keys"][0] is logging_metadata.serving_keys
        assert call_args[1]["helper_columns"][0] is logging_metadata.inference_helper
        assert (
            call_args[1]["request_parameters"][0] is logging_metadata.request_parameters
        )
        assert call_args[1]["event_time"][0] is logging_metadata.event_time
        assert call_args[1]["request_id"][0] == request_id_data
        assert call_args[1]["extra_logging_features"][0] is extra_logging_data
        assert call_args[1]["model_name"] == "test_model"
        assert call_args[1]["model_version"] == 1

    def test_get_feature_logging_data_return_list_logging_metadata_override(
        self, mocker
    ):
        # Arrange
        feature_store_id = 99

        mocker.patch("hsfs.core.feature_view_api.FeatureViewApi")
        mocked_engine = mocker.Mock()
        mocker.patch("hsfs.engine.get_instance", return_value=mocked_engine)
        mocked_engine.get_feature_logging_list.return_value = (pd.DataFrame, None, None)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=feature_store_id
        )

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=["primary_key"],
            event_time="event_time",
            partition_key=[],
            features=[
                feature.Feature("primary_key", primary=True, type="bigint"),
                feature.Feature("event_time", type="timestamp"),
                feature.Feature("feature_1", type="float"),
                feature.Feature("feature_2", type="float"),
                feature.Feature("inference_helper_1", type="float"),
            ],
            id=11,
            stream=False,
            featurestore_name="test_fs",
        )

        query = fg.select_all()

        @udf(return_type=int, drop=["rp_1", "rp_2"])
        def on_demand_feature(rp_1, rp_2) -> pd.Series:
            return rp_1 + rp_2

        fv = feature_view.FeatureView(
            name="fv_name",
            version=1,
            featurestore_id=feature_store_id,
            query=query,
            inference_helper_columns=["inference_helper_1"],
            extra_logging_features=["extra_1", "extra_2"],
            transformation_functions=[on_demand_feature],
        )

        fv.schema = [
            TrainingDatasetFeature(name="id", type="bigint", label=False),
            TrainingDatasetFeature(name="label", type="bigint", label=True),
            TrainingDatasetFeature("primary_key", primary=True, type="bigint"),
            TrainingDatasetFeature("event_time", type="timestamp"),
            TrainingDatasetFeature("feature_1", type="double"),
            TrainingDatasetFeature("feature_2", type="double"),
            TrainingDatasetFeature("feature_3", type="int"),
            TrainingDatasetFeature("min_max_scaler_feature_3", type="double"),
            TrainingDatasetFeature("extra_1", type="string"),
            TrainingDatasetFeature("extra_2", type="int"),
            TrainingDatasetFeature("inference_helper_1", type="double"),
            TrainingDatasetFeature("on_demand_feature", type="double"),
        ]

        fv._serving_keys = [
            ServingKey(feature_name="primary_key", join_index=0, feature_group=fg)
        ]

        fv._request_parameters = ["rp_1", "rp_2"]
        fv._FeatureView__extra_logging_column_names = ["extra_1", "extra_2"]

        logging_data = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
                "feature_3": [7, 8, 9],
                "min_max_scaler_feature_3": [0.7, 0.8, 0.9],
                "extra_1": ["a", "b", "c"],
                "extra_2": [10, 11, 12],
                "inference_helper_1": [0.01, 0.02, 0.03],
            }
        ).to_dict(orient="records")

        logging_metadata = LoggingMetaData()

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        # Prepare test data for various logging components
        logging_metadata.untransformed_features = pd.DataFrame(
            {
                "primary_key": [1, 2, 3],
                "feature_1": [0.1, 0.2, 0.3],
                "feature_2": [0.4, 0.5, 0.6],
            }
        ).values.tolist()

        untransformed_features = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
                "feature_1": [0.11, 0.21, 0.31],
                "feature_2": [0.41, 0.51, 0.61],
            }
        ).values.tolist()

        logging_metadata.transformed_features = pd.DataFrame(
            {"primary_key": [1, 2, 3], "min_max_scaler_feature_3": [0.7, 0.8, 0.9]}
        ).values.tolist()

        transformed_features = pd.DataFrame(
            {
                "primary_key": [11, 21, 31],
                "min_max_scaler_feature_3": [0.71, 0.81, 0.91],
            }
        ).values.tolist()

        predictions_data = pd.DataFrame({"label": [1, 0, 1]}).values.tolist()

        logging_metadata.inference_helper = pd.DataFrame(
            {"inference_helper_1": [0.01, 0.02, 0.03]}
        ).values.tolist()

        inference_helper = pd.DataFrame(
            {"inference_helper_1": [10.01, 10.02, 10.03]}
        ).values.tolist()

        logging_metadata.request_parameters = pd.DataFrame(
            {"rp_1": [10, 20, 30], "rp_2": [5, 15, 25]}
        ).values.tolist()

        request_parameters = pd.DataFrame(
            {"rp_1": [110, 210, 310], "rp_2": [51, 115, 215]}
        ).values.tolist()

        logging_metadata.event_time = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-03"])}
        ).values.tolist()

        event_time = pd.DataFrame(
            {"event_time": pd.to_datetime(["2025-12-02", "2025-11-02", "2025-10-03"])}
        ).values.tolist()

        logging_metadata.serving_keys = pd.DataFrame(
            {"primary_key": [1, 2, 3]}
        ).values.tolist()

        serving_keys = pd.DataFrame({"primary_key": [11, 21, 31]}).values.tolist()

        extra_logging_data = pd.DataFrame(
            {"extra_1": ["a", "b", "c"], "extra_2": [10, 11, 12]}
        ).values.tolist()

        request_id_data = ["test_request_123", "test_request_456", "test_request_789"]

        # Mock hsml model
        mock_hsml_model = mocker.Mock()
        mock_hsml_model.name = "test_model"
        mock_hsml_model.version = 1

        training_dataset_version = 3

        extended_list = create_extended_type(list)
        logging_data = extended_list(logging_data)
        logging_data.hopsworks_logging_metadata = logging_metadata

        # Act
        _ = fv_engine._get_feature_logging_data(
            fv=fv,
            logging_feature_group=fg,
            untransformed_features=untransformed_features,
            transformed_features=transformed_features,
            logging_data=logging_data,
            predictions=predictions_data,
            serving_keys=serving_keys,
            helper_columns=inference_helper,
            request_parameters=request_parameters,
            event_time=event_time,
            request_id=request_id_data,
            extra_logging_features=extra_logging_data,
            training_dataset_version=training_dataset_version,
            hsml_model=mock_hsml_model,
            model_name="test_model",
            model_version=1,
            return_list=True,
        )

        # Assert
        assert mocked_engine.get_feature_logging_list.call_count == 1
        assert (
            mocked_engine.get_feature_logging_df.call_count == 0
        )  # Should not be called when return_list=True

        call_args = mocked_engine.get_feature_logging_list.call_args

        # Verify the main arguments for list version
        assert (
            call_args[1]["logging_data"] is None
        )  # Should be None since all the data is in the metadata
        assert len(call_args[1]["logging_feature_group_features"]) == len(fg.features)
        assert sorted(call_args[1]["logging_feature_group_feature_names"]) == sorted(
            [feat.name for feat in fg.features]
        )
        assert sorted(call_args[1]["logging_features"]) == sorted(
            [
                feat.name
                for feat in fg.features
                if feat.name not in constants.FEATURE_LOGGING.LOGGING_METADATA_COLUMNS
            ]
        )
        assert call_args[1]["training_dataset_version"] == training_dataset_version
        assert (
            call_args[1]["untransformed_features"][0]
            is not logging_metadata.untransformed_features
        )
        assert call_args[1]["untransformed_features"][0] is untransformed_features
        assert (
            call_args[1]["transformed_features"][0]
            is not logging_metadata.transformed_features
        )
        assert call_args[1]["transformed_features"][0] is transformed_features
        assert call_args[1]["predictions"][0] is predictions_data
        assert call_args[1]["serving_keys"][0] is not logging_metadata.serving_keys
        assert call_args[1]["serving_keys"][0] is serving_keys
        assert (
            call_args[1]["helper_columns"][0] is not logging_metadata.inference_helper
        )
        assert call_args[1]["helper_columns"][0] is inference_helper
        assert (
            call_args[1]["request_parameters"][0]
            is not logging_metadata.request_parameters
        )
        assert call_args[1]["request_parameters"][0] is request_parameters
        assert call_args[1]["event_time"][0] is not logging_metadata.event_time
        assert call_args[1]["event_time"][0] is event_time
        assert call_args[1]["request_id"][0] == request_id_data
        assert call_args[1]["extra_logging_features"][0] is extra_logging_data
        assert call_args[1]["model_name"] == "test_model"
        assert call_args[1]["model_version"] == 1

    def test_get_primary_keys_from_query_all_fully_qualified(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        query = fg5.select_features().join(fg6.select_features())

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            labels=["label"],
        )

        # Act
        fqn_primary_keys = fv_engine._get_primary_keys_from_query(fv.query)

        # Assert
        assert {"test_fs_test5_1_primary_key", "test_fs_test6_1_primary_key"} == set(
            fqn_primary_keys
        )

    def test_get_primary_keys_from_query_some_fully_qualified(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        query = fg5.select_features().join(fg6.select_all())

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            labels=["label"],
        )

        # Act
        fqn_primary_keys = fv_engine._get_primary_keys_from_query(fv.query)

        # Assert
        assert {"test_fs_test5_1_primary_key", "primary_key"} == set(fqn_primary_keys)

    def test_get_event_time_from_query_all_fully_qualified(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        query = fg5.select_features().join(fg6.select_features())

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            labels=["label"],
        )

        # Act
        fqn_primary_keys = fv_engine._get_eventtimes_from_query(fv.query)

        # Assert
        assert {"test_fs_test5_1_event_time", "test_fs_test6_1_event_time"} == set(
            fqn_primary_keys
        )

    def test_get_event_time_from_query_some_fully_qualified(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        query = fg5.select_features().join(fg6.select_all())

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            labels=["label"],
        )

        # Act
        fqn_primary_keys = fv_engine._get_eventtimes_from_query(fv.query)

        # Assert
        assert {"test_fs_test5_1_event_time", "event_time"} == set(fqn_primary_keys)

    def test_get_training_dataset_schema_no_transformations(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        query = fg5.select_features().join(fg6.select_all())

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            labels=["label"],
        )

        fv.schema = [
            TrainingDatasetFeature(name="fg1_feature1", type="float"),
            TrainingDatasetFeature(name="fg1_feature2", type="int"),
            TrainingDatasetFeature(name="label", type="int", label=True),
            TrainingDatasetFeature(name="fg2_feature1", type="float"),
            TrainingDatasetFeature(name="fg2_feature2", type="int"),
        ]

        # Act
        schema = fv_engine.get_training_dataset_schema(fv)

        # Assert
        # If there are no transformation function training dataset schema == feature view schema
        assert schema == fv.features

    def test_get_training_dataset_schema_transformation_functions(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        query = fg5.select_features().join(fg6.select_all())

        @udf(int, drop="feature")
        def add_one(feature):
            return feature + 1

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[add_one("fg1_feature1")],
            labels=["label"],
        )

        fv.schema = [
            TrainingDatasetFeature(name="fg1_feature1", type="float"),
            TrainingDatasetFeature(name="fg1_feature2", type="int"),
            TrainingDatasetFeature(name="label", type="int", label=True),
            TrainingDatasetFeature(name="fg2_feature1", type="float"),
            TrainingDatasetFeature(name="fg2_feature2", type="int"),
        ]

        # Act
        schema = fv_engine.get_training_dataset_schema(fv)

        # Assert
        assert {feat.name for feat in schema} == {
            "add_one_fg1_feature1_",
            "fg1_feature2",
            "label",
            "fg2_feature1",
            "fg2_feature2",
        }

    def test_get_training_dataset_schema_transformation_functions_statistics(
        self, mocker
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv_engine = feature_view_engine.FeatureViewEngine(
            feature_store_id=99,
        )

        mocker.patch.object(fv_engine, "_get_training_dataset_metadata")

        query = fg5.select_features().join(fg6.select_all())

        from hsfs.builtin_transformations import one_hot_encoder

        fv = feature_view.FeatureView(
            name="fv_name",
            query=query,
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[one_hot_encoder("fg1_feature1")],
            labels=["label"],
        )

        fv.schema = [
            TrainingDatasetFeature(name="fg1_feature1", type="float"),
            TrainingDatasetFeature(name="fg1_feature2", type="int"),
            TrainingDatasetFeature(name="label", type="int", label=True),
            TrainingDatasetFeature(name="fg2_feature1", type="float"),
            TrainingDatasetFeature(name="fg2_feature2", type="int"),
        ]

        mock_one_hot_stats = one_hot_encoder("fg1_feature1")
        mock_one_hot_stats = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=mock_one_hot_stats,
            version=1,
            id=1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        mock_one_hot_stats.transformation_statistics = [
            FeatureDescriptiveStatistics(
                feature_name="fg1_feature1",
                extended_statistics={"unique_values": ["a", "b"]},
            )
        ]
        mocker.patch.object(
            fv_engine._transformation_function_engine,
            "get_ready_to_use_transformation_fns",
            return_value=[mock_one_hot_stats],
        )

        # Act
        schema = fv_engine.get_training_dataset_schema(fv, 1)

        # Assert
        assert {feat.name for feat in schema} == {
            "one_hot_encoder_fg1_feature1_0",
            "one_hot_encoder_fg1_feature1_1",
            "fg1_feature2",
            "label",
            "fg2_feature1",
            "fg2_feature2",
        }
