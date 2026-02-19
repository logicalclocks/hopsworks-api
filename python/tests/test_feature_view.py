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
import warnings

import pytest
from hopsworks_common import version
from hsfs import feature, feature_group, feature_view, training_dataset_feature
from hsfs.constructor import query
from hsfs.feature_store import FeatureStore
from hsfs.hopsworks_udf import udf
from hsfs.serving_key import ServingKey
from hsfs.transformation_function import TransformationType


fg1 = feature_group.FeatureGroup(
    name="test1",
    version=1,
    featurestore_id=99,
    primary_key=["primary_key"],
    event_time="event_time",
    partition_key=[],
    features=[
        feature.Feature("primary_key", primary=True, type="bigint"),
        feature.Feature("event_time", type="timestamp"),
        feature.Feature("fg1_feature", type="float"),
        feature.Feature("label", type="string"),
        feature.Feature("fg1_inference_helper", type="int"),
        feature.Feature("fg1_training_helper", type="int"),
    ],
    id=11,
    stream=False,
    featurestore_name="test_fs",
)

fg2 = feature_group.FeatureGroup(
    name="test2",
    version=1,
    featurestore_id=99,
    primary_key=["primary_key"],
    event_time="event_time",
    partition_key=[],
    features=[
        feature.Feature("primary_key", primary=True, type="bigint"),
        feature.Feature("event_time", type="timestamp"),
        feature.Feature("fg2_feature", type="float"),
        feature.Feature("fg2_inference_helper", type="int"),
        feature.Feature("fg2_training_helper", type="int"),
    ],
    id=11,
    stream=False,
    featurestore_name="test_fs",
)


class TestFeatureView:
    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch.object(
            FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi.get")
        json = backend_fixtures["feature_view"]["get"]["response"]
        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id == 11
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version == 1
        assert fv.description == "test_description"
        assert fv.labels == ["intt"]
        assert fv.transformation_functions == []
        assert len(fv.schema) == 2
        assert isinstance(fv.schema[0], training_dataset_feature.TrainingDatasetFeature)

    def test_from_response_json_basic_info(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["feature_view"]["get_basic_info"]["response"]

        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id is None
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version is None
        assert fv.description is None
        assert fv.labels == []
        assert fv.transformation_functions == []
        assert len(fv.schema) == 0
        assert fv.query._left_feature_group.deprecated is False

    def test_from_response_json_transformation_function(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch.object(
            FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi.get")
        json = backend_fixtures["feature_view"]["get_transformations"]["response"]
        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id == 11
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version == 1
        assert fv.description == "test_description"
        assert fv.labels == ["intt"]
        assert len(fv.transformation_functions) == 2
        assert (
            fv.transformation_functions[0].hopsworks_udf.function_name == "add_mean_fs"
        )
        assert (
            fv.transformation_functions[1].hopsworks_udf.function_name == "add_one_fs"
        )
        assert (
            fv.transformation_functions[0].hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_mean_fs(data1 : pd.Series, statistics=stats):\n    return data1 + statistics.data1.mean\n"
        )
        assert (
            fv.transformation_functions[1].hopsworks_udf._function_source
            == "\n@udf(float)\ndef add_one_fs(data1 : pd.Series):\n    return data1 + 1\n"
        )
        assert (
            fv.transformation_functions[0].transformation_type
            == TransformationType.MODEL_DEPENDENT
        )
        assert (
            fv.transformation_functions[1].transformation_type
            == TransformationType.MODEL_DEPENDENT
        )

        assert len(fv.schema) == 2
        assert isinstance(fv.schema[0], training_dataset_feature.TrainingDatasetFeature)

    def test_from_response_json_basic_info_deprecated(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine.get_type")
        mocker.patch("hopsworks_common.client.get_instance")
        json = backend_fixtures["feature_view"]["get_basic_info_deprecated"]["response"]

        # Act
        with warnings.catch_warnings(record=True) as warning_record:
            fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.name == "test_name"
        assert fv.id is None
        assert isinstance(fv.query, query.Query)
        assert fv.featurestore_id == 5
        assert fv.version is None
        assert fv.description is None
        assert fv.labels == []
        assert fv.transformation_functions == []
        assert len(fv.schema) == 0
        assert fv.query._left_feature_group.deprecated is True
        assert len(warning_record) == 1
        assert str(warning_record[0].message) == (
            f"Feature Group `{fv.query._left_feature_group.name}`, version `{fv.query._left_feature_group.version}` is deprecated"
        )

    def test_transformation_function_instances(self, mocker, backend_fixtures):
        # Arrange
        feature_store_id = 99
        mocked_connection = mocker.MagicMock()
        mocked_connection.backend_version = version.__version__
        mocked_connection = mocker.patch(
            "hopsworks_common.client.get_connection", return_value=mocked_connection
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["query"]["get"]["response"]

        # Act
        q = query.Query.from_response_json(json)

        @udf(int)
        def test(col1):
            return col1 + 1

        fv = feature_view.FeatureView(
            featurestore_id=feature_store_id,
            name="test_fv",
            version=1,
            query=q,
            transformation_functions=[test("data1"), test("data2")],
        )

        transformation_functions = fv.transformation_functions

        assert transformation_functions[0] != transformation_functions[1]

    def test_logging_enabled_instances(self, mocker, backend_fixtures):
        # Arrange
        mocked_connection = mocker.MagicMock()
        mocked_connection.backend_version = version.__version__
        mocked_connection = mocker.patch(
            "hopsworks_common.client.get_connection", return_value=mocked_connection
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        json = backend_fixtures["feature_view"]["get_feature_view_logging_enabled"][
            "response"
        ]

        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.logging_enabled is True

    def test_label_column_name(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features().join(fg2.select_features()),
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["fg1_inference_helpers", "fg2_inference_helpers"],
            training_helper_columns=["fg1_training_helper", "fg2_training_helper"],
            labels=["label"],
        )

        # Act
        mocker.patch.object(
            fv,
            "get_training_dataset_schema",
            return_value=[
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_feature", type="float"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_inference_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_training_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="label", type="int", label=True
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_feature", type="float"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_inference_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_training_helper", type="int"
                ),
            ],
        )

        # Assert
        assert fv._label_column_names == {"label"}

    def test_transformed_feature_name(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features().join(fg2.select_features()),
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["fg1_inference_helper", "fg2_inference_helper"],
            training_helper_columns=["fg1_training_helper", "fg2_training_helper"],
            labels=["label"],
        )

        mocker.patch.object(
            fv,
            "get_training_dataset_schema",
            return_value=[
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_feature", type="float"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_inference_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_training_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_transformed_features", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="label", type="int", label=True
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_feature", type="float"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_inference_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_training_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_transformed_features", type="int"
                ),
            ],
        )

        # Act
        transformed_features = fv._transformed_feature_names

        # Assert
        assert transformed_features == [
            "fg1_feature",
            "fg1_transformed_features",
            "fg2_feature",
            "fg2_transformed_features",
        ]

    def test_untransformed_feature_names(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features().join(fg2.select_features()),
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["fg1_inference_helper", "fg2_inference_helper"],
            training_helper_columns=["fg1_training_helper", "fg2_training_helper"],
            labels=["label"],
        )

        mocker.patch.object(
            fv,
            "get_training_dataset_schema",
            return_value=[
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_feature", type="float"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_inference_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg1_training_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="label", type="int", label=True
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_feature", type="float"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_inference_helper", type="int"
                ),
                training_dataset_feature.TrainingDatasetFeature(
                    name="fg2_training_helper", type="int"
                ),
            ],
        )

        # Act
        transformed_features = fv._transformed_feature_names

        # Assert
        assert transformed_features == ["fg1_feature", "fg2_feature"]

    def test_required_serving_key_names(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features().join(fg2.select_features()),
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["fg1_inference_helper", "fg2_inference_helper"],
            training_helper_columns=["fg1_training_helper", "fg2_training_helper"],
            labels=["label"],
        )

        fv._serving_keys = [
            ServingKey(
                feature_name="primary_key",
                join_index=0,
                feature_group=fg1,
                required=True,
            ),
            ServingKey(
                feature_name="primary_key",
                join_index=0,
                feature_group=fg2,
                required=False,
            ),
        ]

        # Act
        required_serving_keys = fv._required_serving_key_names

        # Assert
        assert required_serving_keys == ["primary_key"]

    def test_root_feature_group_event_time_column_name(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features().join(fg2.select_features()),
            featurestore_id=99,
            featurestore_name="test_fs",
            inference_helper_columns=["fg1_inference_helper", "fg2_inference_helper"],
            training_helper_columns=["fg1_training_helper", "fg2_training_helper"],
            labels=["label"],
        )

        # Act
        root_feature_group_event_time = fv._root_feature_group_event_time_column_name

        # Assert
        assert root_feature_group_event_time == "event_time"

    def test_delete_feature_view_force(self, mocker, backend_fixtures):
        # Arrange
        mock_engine = mocker.patch(
            "hsfs.core.feature_view_engine.FeatureViewEngine.delete"
        )
        mocker.patch("hsfs.engine.get_type", return_value="python")

        json = backend_fixtures["feature_view"]["get"]["response"]
        fv = feature_view.FeatureView.from_response_json(json)

        # Act: delete without force (default)
        fv.delete()
        # Act: delete with force=True
        fv.delete(force=True)

        # Assert
        # First call: force should be False (default)
        # Second call: force should be True
        assert mock_engine.call_count == 2
        call_args_list = mock_engine.call_args_list
        assert call_args_list[0][0][2] is False  # (name, version, force)
        assert call_args_list[1][0][2] is True


class TestFeatureViewExecuteOdts:
    def test_execute_odts_with_transformations(self, mocker):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(int)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        odts_list = [odt("fg1_feature")]

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        # Mock the property since it's computed from features
        mocker.patch.object(
            type(fv),
            "_on_demand_transformation_functions",
            new_callable=mocker.PropertyMock,
            return_value=odts_list,
        )

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline)
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_odts(data=df_test_data, online=False)

        mock_apply.assert_called_with(
            transformation_functions=odts_list,
            data=df_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online)
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_odts(data=dict_test_data, online=True)

        mock_apply.assert_called_with(
            transformation_functions=odts_list,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
        )
        assert result_dict == dict_test_data

    def test_execute_odts_with_transformation_context_and_request_parameters(
        self, mocker
    ):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(int)
        def add_context_value(feature, context):
            return feature + context["value"]

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_context_value,
            transformation_type=TransformationType.ON_DEMAND,
        )
        odts_list = [odt("fg1_feature")]

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        mocker.patch.object(
            type(fv),
            "_on_demand_transformation_functions",
            new_callable=mocker.PropertyMock,
            return_value=odts_list,
        )

        context = {"value": 10}
        request_params = {"param1": "value1"}

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline) and transformation_context
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_odts(
            data=df_test_data, online=False, transformation_context=context
        )

        mock_apply.assert_called_with(
            transformation_functions=odts_list,
            data=df_test_data,
            online=False,
            transformation_context=context,
            request_parameters=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online) and request_parameters
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_odts(
            data=dict_test_data, online=True, request_parameters=request_params
        )

        mock_apply.assert_called_with(
            transformation_functions=odts_list,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=request_params,
        )
        assert result_dict == dict_test_data

    def test_execute_odts_no_transformations(self, mocker, caplog):
        import logging

        import pandas as pd

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        # Mock empty list - no ODTs attached
        mocker.patch.object(
            type(fv),
            "_on_demand_transformation_functions",
            new_callable=mocker.PropertyMock,
            return_value=[],
        )

        test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
        )

        # Act
        with caplog.at_level(logging.INFO):
            result = fv.execute_odts(data=test_data, online=False)

        # Assert
        mock_apply.assert_not_called()
        assert (
            "No on-demand transformation functions attached to the feature view"
            in caplog.text
        )
        pd.testing.assert_frame_equal(result, test_data)

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_execute_odts_execution_modes(self, mocker, execution_mode):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(int, mode=execution_mode)
        def add_one(feature):
            return feature + 1

        odt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        odts_list = [odt("fg1_feature")]

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        mocker.patch.object(
            type(fv),
            "_on_demand_transformation_functions",
            new_callable=mocker.PropertyMock,
            return_value=odts_list,
        )

        if execution_mode == "default":
            online_test_data = {"fg1_feature": 1.0}
            offline_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        elif execution_mode == "python":
            online_test_data = offline_test_data = {"fg1_feature": 1.0}
        elif execution_mode == "pandas":
            online_test_data = offline_test_data = pd.DataFrame(
                {"fg1_feature": [1.0, 2.0, 3.0]}
            )

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Act - online
        fv.execute_odts(data=online_test_data, online=True)

        # Assert - online
        mock_apply.assert_called_with(
            transformation_functions=odts_list,
            data=online_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
        )

        # Act - offline
        fv.execute_odts(data=offline_test_data, online=False)

        # Assert - offline
        mock_apply.assert_called_with(
            transformation_functions=odts_list,
            data=offline_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )


class TestFeatureViewExecuteMdts:
    def test_execute_mdts_with_transformations(self, mocker):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[mdt("fg1_feature")],
        )

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline)
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_mdts(data=df_test_data, online=False)

        mock_apply.assert_called_with(
            transformation_functions=fv.transformation_functions,
            data=df_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online)
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_mdts(data=dict_test_data, online=True)

        mock_apply.assert_called_with(
            transformation_functions=fv.transformation_functions,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
        )
        assert result_dict == dict_test_data

    def test_execute_mdts_with_transformation_context_and_request_parameters(
        self, mocker
    ):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(int)
        def add_context_value(feature, context):
            return feature + context["value"]

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_context_value,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[mdt("fg1_feature")],
        )

        context = {"value": 10}
        request_params = {"param1": "value1"}

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline) and transformation_context
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_mdts(
            data=df_test_data, online=False, transformation_context=context
        )

        mock_apply.assert_called_with(
            transformation_functions=fv.transformation_functions,
            data=df_test_data,
            online=False,
            transformation_context=context,
            request_parameters=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online) and request_parameters
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_mdts(
            data=dict_test_data, online=True, request_parameters=request_params
        )

        mock_apply.assert_called_with(
            transformation_functions=fv.transformation_functions,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=request_params,
        )
        assert result_dict == dict_test_data

    def test_execute_mdts_with_statistics(self, mocker):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )
        from hsfs.transformation_statistics import TransformationStatistics

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        stats = TransformationStatistics("feature")

        @udf(float, mode="pandas")
        def normalize(feature, statistics=stats):
            return (feature - statistics.feature.mean) / statistics.feature.std_dev

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=normalize,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[mdt("fg1_feature")],
        )

        test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        expected_result = pd.DataFrame(
            {"fg1_feature": [1.0, 2.0, 3.0], "normalize_fg1_feature_": [-1.0, 0.0, 1.0]}
        )

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            return_value=expected_result,
        )

        # Act
        result = fv.execute_mdts(data=test_data, online=False)

        # Assert
        mock_apply.assert_called_once_with(
            transformation_functions=fv.transformation_functions,
            data=test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )
        assert result is expected_result

    def test_execute_mdts_no_transformations(self, mocker, caplog):
        import logging

        import pandas as pd

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[],
        )

        test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
        )

        # Act
        with caplog.at_level(logging.INFO):
            result = fv.execute_mdts(data=test_data, online=False)

        # Assert
        mock_apply.assert_not_called()
        assert (
            "No model dependent transformation functions attached to the feature view"
            in caplog.text
        )
        pd.testing.assert_frame_equal(result, test_data)

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_execute_mdts_execution_modes(self, mocker, execution_mode):
        import pandas as pd
        from hsfs.hopsworks_udf import udf
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        @udf(int, mode=execution_mode)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=10,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[mdt("fg1_feature")],
        )

        if execution_mode == "default":
            online_test_data = {"fg1_feature": 1.0}
            offline_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        elif execution_mode == "python":
            online_test_data = offline_test_data = {"fg1_feature": 1.0}
        elif execution_mode == "pandas":
            online_test_data = offline_test_data = pd.DataFrame(
                {"fg1_feature": [1.0, 2.0, 3.0]}
            )

        mock_apply = mocker.patch.object(
            fv._feature_view_engine,
            "apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Act - online
        fv.execute_mdts(data=online_test_data, online=True)

        # Assert - online
        mock_apply.assert_called_with(
            transformation_functions=fv.transformation_functions,
            data=online_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
        )

        # Act - offline
        fv.execute_mdts(data=offline_test_data, online=False)

        # Assert - offline
        mock_apply.assert_called_with(
            transformation_functions=fv.transformation_functions,
            data=offline_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
        )
