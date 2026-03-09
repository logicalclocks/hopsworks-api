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

import hopsworks_common
import pandas as pd
import pytest
from hopsworks_common.client import exceptions
from hopsworks_common.core.constants import HAS_POLARS
from hsfs import (
    engine,
    feature,
    feature_group,
    feature_view,
    training_dataset,
    transformation_function,
)
from hsfs.core import transformation_function_engine
from hsfs.engine import python, spark
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationType


if HAS_POLARS:
    import polars as pl


fg1 = feature_group.FeatureGroup(
    name="test1",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[
        feature.Feature("id"),
        feature.Feature("label"),
        feature.Feature("tf_name"),
    ],
    id=11,
    stream=False,
)

fg2 = feature_group.FeatureGroup(
    name="test2",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[feature.Feature("id"), feature.Feature("tf1_name")],
    id=12,
    stream=False,
)

fg3 = feature_group.FeatureGroup(
    name="test3",
    version=1,
    featurestore_id=99,
    primary_key=[],
    partition_key=[],
    features=[
        feature.Feature("id"),
        feature.Feature("tf_name"),
        feature.Feature("tf1_name"),
        feature.Feature("tf3_name"),
    ],
    id=12,
    stream=False,
)
engine.init("python")
query = fg1.select_all().join(fg2.select(["tf1_name"]), on=["id"])
query_self_join = fg1.select_all().join(fg1.select_all(), on=["id"], prefix="fg1_")
query_prefix = (
    fg1.select_all()
    .join(fg2.select(["tf1_name"]), on=["id"], prefix="second_")
    .join(fg3.select(["tf_name", "tf1_name"]), on=["id"], prefix="third_")
)


class TestTransformationFunctionEngine:
    @pytest.fixture(scope="class")
    def python_engine(self):
        return python.Engine()

    @pytest.fixture(scope="class")
    def spark_engine(self):
        spark_engine = spark.Engine()
        # Set shuffle partitions to 1 for testing purposes
        spark_engine._spark_session.conf.set("spark.sql.shuffle.partitions", "1")
        yield spark_engine

    def test_save(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction(col1):
            return col1 + 1

        tf = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        # Act
        tf_engine.save(transformation_fn_instance=tf)

        # Assert
        assert mock_tf_api.return_value.register_transformation_fn.call_count == 1

    def test_get_transformation_fn(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        @udf(float)
        def testFunction2(data2, statistics_data2):
            return data2 + 1

        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction2,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        transformations = [tf1, tf2]

        mock_tf_api.return_value.get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine.get_transformation_fn(name=None, version=None)

        # Assert
        assert mock_tf_api.return_value.get_transformation_fn.call_count == 1
        assert result == transformations

    def test_get_transformation_fns(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        @udf(float)
        def testFunction2(data2, statistics_data2):
            return data2 + 1

        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction2,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        transformations = [tf1, tf2]

        mock_tf_api.return_value.get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine.get_transformation_fns()

        # Assert
        assert mock_tf_api.return_value.get_transformation_fn.call_count == 1
        assert result == transformations

    def test_delete(self, mocker):
        # Arrange
        feature_store_id = 99

        mock_tf_api = mocker.patch(
            "hsfs.core.transformation_function_api.TransformationFunctionApi"
        )

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        # Act
        tf_engine.delete(transformation_function_instance=tf1)

        # Assert
        assert mock_tf_api.return_value.delete.call_count == 1

    def test_compute_transformation_fn_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hopsworks_common.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            id=10,
        )

        # Act
        tf_engine.compute_transformation_fn_statistics(
            training_dataset_obj=td,
            statistics_features=None,
            label_encoder_features=None,
            feature_dataframe=None,
            feature_view_obj=None,
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 1
        )

    def test_compute_and_set_feature_statistics_no_split(self, mocker):
        feature_store_id = 99
        mocker.patch("hopsworks_common.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

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

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={},
            id=10,
        )

        # Act
        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        dataset = pd.DataFrame()

        # Act
        tf_engine.compute_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 0
        )

    def test_compute_and_set_feature_statistics_train_test_split(self, mocker):
        feature_store_id = 99
        mocker.patch("hopsworks_common.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

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

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.8, "test": 0.2},
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        dataset = pd.DataFrame()

        # Act
        tf_engine.compute_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Assert
        assert (
            mock_s_engine.return_value.compute_transformation_fn_statistics.call_count
            == 0
        )

    def test_get_and_set_feature_statistics_no_statistics_required(self, mocker):
        feature_store_id = 99
        mocker.patch("hopsworks_common.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )

        @udf(int)
        def testFunction1(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

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

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.8, "test": 0.2},
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        # Act
        tf_engine.get_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_s_engine.return_value.get.call_count == 0

    def test_get_and_set_feature_statistics_statistics_required(self, mocker):
        feature_store_id = 99
        mocker.patch("hopsworks_common.client.get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id
        )
        from hsfs.transformation_statistics import TransformationStatistics

        stats = TransformationStatistics("col1")

        @udf(int)
        def testFunction1(col1, statistics=stats):
            return col1 + statistics.col1.mean

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=testFunction1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

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

        td = training_dataset.TrainingDataset(
            name="test",
            version=1,
            data_format="CSV",
            featurestore_id=99,
            splits={"train": 0.8, "test": 0.2},
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        # Act
        tf_engine.get_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_s_engine.return_value.get.call_count == 1

    def test_execute_udf_on_supported_dataframe(self, mocker, python_engine):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        add_one.output_column_names = ["col1"]

        # Act
        result = tf_engine.execute_udf(
            udf=add_one,
            data=pd.DataFrame(data={"col1": [1, 2, 3]}),
            online=False,
        )

        # Assert
        assert all(result == {"col1": [2, 3, 4]})

    def test_execute_udf_on_unsupported_type(self, mocker, python_engine):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            tf_engine.execute_udf(udf=add_one, data=1, online=False)

        # Assert
        assert (
            str(e_info.value)
            == "Dataframe type <class 'int'> not supported in the engine."
        )

    def test_execute_udf_on_dict(self, mocker, python_engine):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        add_one.output_column_names = [
            "col1"
        ]  # Output column names would already be set when transformation function is executed using TransformationFunctionEngine.

        data = {"col1": 1}

        # Act
        result = tf_engine.execute_udf(
            udf=add_one,
            data=data,
            online=False,
        )

        # Assert
        assert result == {"col1": 2}

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_apply_udf_on_dict_batch(self, mocker, execution_mode):
        # Arrange
        @udf(int, mode=execution_mode)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        add_one.output_column_names = [
            "col1"
        ]  # Mocking the output column names, this would be generated when a transformation function is created.

        # Act
        result = tf_engine.apply_udf_on_dict(
            udf=add_one, data={"col1": 1}, online=False
        )

        # Assert
        assert isinstance(result, dict)
        assert result == {"col1": 2}

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_apply_udf_on_dict_online(self, mocker, execution_mode):
        # Arrange
        @udf(int, mode=execution_mode)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        add_one.output_column_names = [
            "col1"
        ]  # Mocking the output column names, this would be generated when a transformation function is created.

        # Act
        result = tf_engine.apply_udf_on_dict(udf=add_one, data={"col1": 1}, online=True)

        # Assert
        assert isinstance(result, dict)
        assert result == {"col1": 2}

    def test_apply_transformation_functions_dataframe(self, mocker, python_engine):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dataset = pd.DataFrame(data={"col1": [1, 2, 3]})

        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                [tf1, tf2]
            ),
            data=dataset,
            online=False,
        )

        assert set(result.columns) == {"col1", "add_one_col1_", "add_two_col1_"}

    def test_apply_transformation_functions_dict(self, mocker, python_engine):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_type", return_value="python")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dataset = {"col1": 1}

        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                [tf1, tf2]
            ),
            data=dataset,
            online=False,
        )

        assert set(result.keys()) == {"col1", "add_one_col1_", "add_two_col1_"}

    def test_apply_transformation_functions_unsupported_dataframe(
        self, mocker, python_engine
    ):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            tf_engine.apply_transformation_functions(
                execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                    [tf1, tf2]
                ),
                data=1,
                online=False,
            )

        assert (
            str(e_info.value)
            == "Dataframe type <class 'int'> not supported in the engine."
        )

    def test_apply_transformation_functions_missing_features_dict(
        self, mocker, python_engine
    ):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col2):
            return col2 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dataset = {"col1": 1}
        with pytest.raises(exceptions.TransformationFunctionException) as e_info:
            tf_engine.apply_transformation_functions(
                execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                    [tf1, tf2]
                ),
                data=dataset,
                online=False,
            )

        assert (
            str(e_info.value)
            == "The following feature(s): `col2`, required for the transformation function 'add_two' are not available."
        )

    def test_apply_transformation_functions_dropped_features_dataframe(
        self, mocker, python_engine
    ):
        # Arrange
        @udf(int, drop=["col1"])
        def add_one(col1):
            return col1 + 1

        @udf(int, drop=["col2"])
        def add_two(col2):
            return col2 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dataset = pd.DataFrame(data={"col1": [1, 2, 3], "col2": [4, 5, 6]})

        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                [tf1, tf2]
            ),
            data=dataset,
            online=False,
        )

        assert set(result.columns) == {"add_one_col1_", "add_two_col2_"}

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_apply_transformation_functions_dropped_features_dict_batch(
        self, mocker, python_engine, execution_mode
    ):
        # Arrange
        @udf(int, drop=["col1"], mode=execution_mode)
        def add_one(col1):
            return col1 + 1

        @udf(int, drop=["col2"], mode=execution_mode)
        def add_two(col2):
            return col2 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dataset = {"col1": 1, "col2": 4}

        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                [tf1, tf2]
            ),
            data=dataset,
            online=False,
        )

        assert result == {"add_one_col1_": 2, "add_two_col2_": 6}

    @pytest.mark.parametrize("execution_mode", ["python", "pandas", "default"])
    def test_apply_transformation_functions_dropped_features_dict_online(
        self, mocker, execution_mode, python_engine
    ):
        # Arrange
        @udf(int, drop=["col1"], mode=execution_mode)
        def add_one(col1):
            return col1 + 1

        @udf(int, drop=["col2"], mode=execution_mode)
        def add_two(col2):
            return col2 + 2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dataset = {"col1": 1, "col2": 4}

        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                [tf1, tf2]
            ),
            data=dataset,
            online=True,
        )

        assert result == {"add_one_col1_": 2, "add_two_col2_": 6}

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_udf_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_udf_dict_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        data = {"tf_name": 1}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=True,
        )

        # Assert
        assert result["plus_one_tf_name_"] == 2

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_udf_dataframe_transformation_context_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def plus_one(col1, context):
            return col1 + context["test"] + 10

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
            transformation_context={"test": 10},
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 21
        assert result["plus_one_tf_name_"][1] == 22

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_udf_dict_transformation_context_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def plus_one(col1, context):
            return col1 + context["test"] + 10

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        data = {"tf_name": 1}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
            transformation_context={"test": 10},
        )

        # Assert
        assert result["plus_one_tf_name_"] == 21

    @pytest.mark.parametrize(
        "execution_mode, online", [("pandas", True), ("pandas", False)]
    )
    def test_apply_transformation_function_dataframe_multiple_output_udf_input_dataframe_return_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1"], mode=execution_mode)
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    @pytest.mark.parametrize(
        "execution_mode, online", [("pandas", True), ("pandas", False)]
    )
    def test_apply_transformation_function_dataframe_multiple_output_udf_input_dict_return_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1"], mode=execution_mode)
        def plus_two(col1):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col1 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        data = {"col1": 1, "col2": 10}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert list(result.keys()) == ["col2", "plus_two_col1_0", "plus_two_col1_1"]
        assert result["plus_two_col1_0"] == 2
        assert result["plus_two_col1_1"] == 3

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_output_udf_input_dataframe_return_tuple_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1"], mode=execution_mode)
        def plus_two(col1):
            return col1 + 1, col1 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(result.columns == ["col2", "plus_two_col1_0", "plus_two_col1_1"])
        assert len(result) == 2
        assert result["plus_two_col1_0"][0] == 2
        assert result["plus_two_col1_0"][1] == 3
        assert result["plus_two_col1_1"][0] == 3
        assert result["plus_two_col1_1"][1] == 4

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_output_udf_input_dict_return_tuple_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1"], mode=execution_mode)
        def plus_two(col1):
            return col1 + 1, col1 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        data = {"col1": 1, "col2": 10}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert list(result.keys()) == ["col2", "plus_two_col1_0", "plus_two_col1_1"]
        assert result["plus_two_col1_0"] == 2
        assert result["plus_two_col1_1"] == 3

    @pytest.mark.parametrize(
        "execution_mode, online", [("pandas", True), ("pandas", False)]
    )
    def test_apply_transformation_function_dataframe_multiple_input_output_udf_input_dataframe_return_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], mode=execution_mode)
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.parametrize(
        "execution_mode, online", [("pandas", True), ("pandas", False)]
    )
    def test_apply_transformation_function_dataframe_multiple_input_output_udf_input_dict_return_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], mode=execution_mode)
        def plus_two(col1, col2):
            return pd.DataFrame({"new_col1": col1 + 1, "new_col2": col2 + 2})

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = {"col1": 1, "col2": 10}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert list(result.keys()) == [
            "col1",
            "col2",
            "plus_two_col1_col2_0",
            "plus_two_col1_col2_1",
        ]
        assert result["col1"] == 1
        assert result["col2"] == 10
        assert result["plus_two_col1_col2_0"] == 2
        assert result["plus_two_col1_col2_1"] == 12

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_dataframe_multiple_input_output_udf_input_dataframe_return_tuple_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_dataframe_multiple_input_output_udf_input_dict_return_tuple_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        data = {"col1": 1, "col2": 10}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert list(result.keys()) == [
            "col1",
            "col2",
            "plus_two_col1_col2_0",
            "plus_two_col1_col2_1",
        ]
        assert result["col1"] == 1
        assert result["col2"] == 10
        assert result["plus_two_col1_col2_0"] == 2
        assert result["plus_two_col1_col2_1"] == 12

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_input_output_udf_input_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(
            result.columns
            == ["col1", "col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col1"][0] == 1
        assert result["col1"][1] == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_input_output_udf_input_dict_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        data = {"col1": 1, "col2": 2}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert list(result.keys()) == [
            "col1",
            "col2",
            "plus_two_col1_col2_0",
            "plus_two_col1_col2_1",
        ]
        assert result["col1"] == 1
        assert result["col2"] == 2
        assert result["plus_two_col1_col2_0"] == 2
        assert result["plus_two_col1_col2_1"] == 4

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_input_output_drop_all_udf_input_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1", "col2"], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(result.columns == ["plus_two_col1_col2_0", "plus_two_col1_col2_1"])
        assert len(result) == 2
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_input_output_drop_all_udf_input_dict_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1", "col2"], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        data = {"col1": 1, "col2": 10}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert set(result.keys()) == {"plus_two_col1_col2_0", "plus_two_col1_col2_1"}
        assert result["plus_two_col1_col2_0"] == 2
        assert result["plus_two_col1_col2_1"] == 12

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_input_output_drop_some_udf_input_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1"], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        df = pd.DataFrame(data={"col1": [1, 2], "col2": [10, 11]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert all(
            result.columns == ["col2", "plus_two_col1_col2_0", "plus_two_col1_col2_1"]
        )
        assert len(result) == 2
        assert result["col2"][0] == 10
        assert result["col2"][1] == 11
        assert result["plus_two_col1_col2_0"][0] == 2
        assert result["plus_two_col1_col2_0"][1] == 3
        assert result["plus_two_col1_col2_1"][0] == 12
        assert result["plus_two_col1_col2_1"][1] == 13

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_multiple_input_output_drop_some_udf_input_dict_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf([int, int], drop=["col1"], mode=execution_mode)
        def plus_two(col1, col2):
            return col1 + 1, col2 + 2

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_two],
        )

        data = {"col1": 1, "col2": 10}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert set(result.keys()) == {
            "col2",
            "plus_two_col1_col2_0",
            "plus_two_col1_col2_1",
        }

        assert result["col2"] == 10
        assert result["plus_two_col1_col2_0"] == 2
        assert result["plus_two_col1_col2_1"] == 12

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    def test_apply_transformation_function_polars_udf_input_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        df = pl.DataFrame(data={"tf_name": [1, 2]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=df,
            online=online,
        )

        # Assert
        assert len(result["plus_one_tf_name_"]) == 2
        assert result["plus_one_tf_name_"][0] == 2
        assert result["plus_one_tf_name_"][1] == 3

    @pytest.mark.skipif(
        not HAS_POLARS,
        reason="Polars is not installed.",
    )
    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_function_polars_udf_input_dict_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[plus_one("tf_name")],
        )

        data = {"tf_name": 1}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fv.transformation_functions
            ),
            data=data,
            online=online,
        )

        # Assert
        assert result["plus_one_tf_name_"] == 2

    def test_apply_transformation_function_missing_feature_on_demand_transformations_input_dataframe_python_engine(
        self, mocker, python_engine
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            transformation_functions=[add_one("missing_col1")],
            id=11,
            stream=False,
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        with pytest.raises(exceptions.TransformationFunctionException) as exception:
            tf_engine.apply_transformation_functions(
                execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                    fg.transformation_functions
                ),
                data=df,
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, required for the transformation function 'add_one' are not available."
        )

    def test_apply_transformation_function_missing_feature_on_demand_transformations_input_dict_python_engine(
        self, mocker, python_engine
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            transformation_functions=[add_one("missing_col1")],
            id=11,
            stream=False,
        )

        data = {"tf_name": [1, 2]}

        # Act
        with pytest.raises(exceptions.TransformationFunctionException) as exception:
            tf_engine.apply_transformation_functions(
                execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                    fg.transformation_functions
                ),
                data=data,
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, required for the transformation function 'add_one' are not available."
        )

    def test_apply_transformation_function_missing_feature_model_dependent_transformations_input_dataframe_python_engine(
        self, mocker, python_engine
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[add_one("missing_col1")],
        )

        df = pd.DataFrame(data={"tf_name": [1, 2]})

        # Act
        with pytest.raises(exceptions.TransformationFunctionException) as exception:
            tf_engine.apply_transformation_functions(
                execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                    fv.transformation_functions
                ),
                data=df,
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, required for the transformation function 'add_one' are not available."
        )

    def test_apply_transformation_function_missing_feature_model_dependent_transformations_input_dict_python_engine(
        self, mocker, python_engine
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            id=11,
            stream=False,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg.select_all(),
            featurestore_id=99,
            transformation_functions=[add_one("missing_col1")],
        )

        data = {"tf_name": [1, 2]}

        # Act
        with pytest.raises(exceptions.TransformationFunctionException) as exception:
            tf_engine.apply_transformation_functions(
                execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                    fv.transformation_functions
                ),
                data=data,
            )

        assert (
            str(exception.value)
            == "The following feature(s): `missing_col1`, required for the transformation function 'add_one' are not available."
        )

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_functions_request_parameters_input_dataframe_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            transformation_functions=[add_one("tf_name")],
            id=11,
            stream=False,
        )

        df = pd.DataFrame(data={"tf_name": [1]})

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fg.transformation_functions
            ),
            data=df,
            request_parameters={"tf_name": 10},
            online=online,
        )

        # Assert
        assert result["add_one"].values.tolist() == [11]

    @pytest.mark.parametrize(
        "execution_mode, online",
        [
            ("python", True),
            ("python", False),
            ("pandas", True),
            ("pandas", False),
            ("default", True),
            ("default", False),
        ],
    )
    def test_apply_transformation_functions_request_parameters_input_dict_python_engine(
        self, mocker, python_engine, execution_mode, online
    ):
        # Arrange
        mocker.patch("hopsworks_common.client.get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        @udf(int, mode=execution_mode)
        def add_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test1",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("id"), feature.Feature("tf_name")],
            transformation_functions=[add_one("tf_name")],
            id=11,
            stream=False,
        )

        data = {"tf_name": 1}

        # Act
        result = tf_engine.apply_transformation_functions(
            execution_graph=transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                fg.transformation_functions
            ),
            data=data,
            request_parameters={"tf_name": 10},
            online=online,
        )

        # Assert
        assert result["add_one"] == 11

    def test_build_transformation_function_execution_graph_no_dependencies(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        execution_graph = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2]
        )

        assert tf1 in execution_graph.nodes and tf2 in execution_graph.nodes
        assert len(execution_graph.nodes) == 2
        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == []

    def test_build_transformation_function_execution_graph_with_dependencies(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        @udf(int)
        def add_three(add_one, add_two):
            return add_one + add_two

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_three,
            transformation_type=TransformationType.ON_DEMAND,
        )

        execution_graph = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2, tf3]
        )

        assert len(execution_graph.nodes) == 3
        assert all(tf in execution_graph.nodes for tf in [tf1, tf2, tf3])
        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == []
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Verify topological order: roots before dependents
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf2)

    def test_build_transformation_function_execution_graph_with_dependencies_aliased(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        @udf(int)
        def add(add_one, add_two):
            return add_one + add_two

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1.alias("odt1")
        tf2 = tf2("odt1").alias("odt2")
        tf3 = tf3("odt1", "odt2")

        execution_graph = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2, tf3]
        )

        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == [tf1]
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Verify topological order: tf1 -> tf2 -> tf3
        assert execution_graph.nodes.index(tf1) < execution_graph.nodes.index(tf2)
        assert execution_graph.nodes.index(tf2) < execution_graph.nodes.index(tf3)

    def test_build_transformation_function_execution_graph_with_overwrite_feature(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def col1(col1, col2):
            return col1 + col2

        @udf(int)
        def col2(col2):
            return col1 + 2

        @udf(int)
        def add(col1, col2):
            return col1 + col2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=col1,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=col2,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        execution_graph = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2, tf3]
        )

        assert execution_graph.dependencies[id(tf1)] == []
        assert execution_graph.dependencies[id(tf2)] == []
        assert tf1 in execution_graph.dependencies[id(tf3)]
        assert tf2 in execution_graph.dependencies[id(tf3)]
        assert len(execution_graph.dependencies[id(tf3)]) == 2
        # Verify topological order: roots before dependents
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf2)

    def test_build_transformation_function_execution_graph_with_dependencies_cyclic_dependency(
        self, mocker
    ):
        feature_store_id = 99

        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1, add_three):
            return col1 + 1

        @udf(int)
        def add_two(add_one):
            return add_one + 2

        @udf(int)
        def add_three(add_two, add_one):
            return add_two + add_one

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_three,
            transformation_type=TransformationType.ON_DEMAND,
        )

        with pytest.raises(exceptions.TransformationFunctionException) as e_info:
            _ = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
                [tf1, tf2, tf3]
            )

        assert (
            str(e_info.value)
            == "Cyclic dependency detected in transformation functions."
        )

    def test_apply_transformation_functions_chained_dataframe(
        self, mocker, python_engine
    ):
        """Test that chained transformations work end-to-end when dependent TF is listed first.

        This reproduces the bug where topo_tfs was re-sorted by insertion order,
        breaking topological ordering and causing validation to fail because
        upstream outputs weren't yet registered.
        """

        @udf([float, float])
        def add_one(feature):
            return feature + 1, feature + 1

        @udf([float, float])
        def add_two(feature):
            return feature + 2, feature + 2

        @udf(float)
        def add(feature1, feature2):
            return feature1 + feature2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1("feature").alias("odt1_1", "odt1_2")
        tf2 = tf2("feature").alias("odt2_1", "odt2_2")
        # Dependent TF listed first — this is the scenario that triggered the bug
        tf3 = tf3("odt1_1", "odt2_2")

        dataset = pd.DataFrame({"feature": [0.25, 0.80, 0.42]})

        execution_graph = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf3, tf1, tf2]
        )

        # Verify topological order: roots before dependents despite input order
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf2)

        result = tf_engine.apply_transformation_functions(
            execution_graph=execution_graph,
            data=dataset,
            online=False,
        )

        assert "odt1_1" in result.columns
        assert "odt1_2" in result.columns
        assert "odt2_1" in result.columns
        assert "odt2_2" in result.columns
        assert "add" in result.columns
        # odt1_1 = feature + 1, odt2_2 = feature + 2, add = odt1_1 + odt2_2
        expected = (dataset["feature"] + 1) + (dataset["feature"] + 2)
        pd.testing.assert_series_equal(result["add"], expected, check_names=False)

    def test_apply_transformation_functions_chained_dict(self, mocker, python_engine):
        """Test chained transformations with dict input (online inference path)."""

        @udf([float, float])
        def add_one(feature):
            return feature + 1, feature + 1

        @udf([float, float])
        def add_two(feature):
            return feature + 2, feature + 2

        @udf(float)
        def add(feature1, feature2):
            return feature1 + feature2

        mocker.patch("hopsworks_common.client.get_instance")
        mocker.patch("hsfs.engine.get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine.get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        tf1 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1("feature").alias("odt1_1", "odt1_2")
        tf2 = tf2("feature").alias("odt2_1", "odt2_2")
        tf3 = tf3("odt1_1", "odt2_2")

        dataset = {"feature": 0.25}

        execution_graph = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf3, tf1, tf2]
        )

        result = tf_engine.apply_transformation_functions(
            execution_graph=execution_graph,
            data=dataset,
            online=False,
        )

        assert result["odt1_1"] == 1.25
        assert result["odt2_2"] == 2.25
        assert result["add"] == 3.5

    def test_visualize_empty_dag(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")
        dag = transformation_function_engine.TransformationExecutionDAG(nodes=[])
        dot = dag._to_graphviz()
        # Empty DAG should produce a valid Digraph with no nodes
        assert "Input Features" not in dot.source
        assert "Output Features" not in dot.source

    def test_visualize_independent_tfs(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2]
        )
        dot = dag._to_graphviz()

        # Verify Input and Output nodes exist
        assert "Input Features" in dot.source
        assert "Output Features" in dot.source
        # Verify TF node labels
        assert "add_one" in dot.source
        assert "add_two" in dot.source
        assert "mode: default" in dot.source
        # Verify edge labels contain feature names
        assert "col1" in dot.source

    def test_visualize_diamond_dependency(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        @udf(int)
        def add_two(col1):
            return col1 + 2

        @udf(int)
        def add_three(add_one, add_two):
            return add_one + add_two

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_three,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2, tf3]
        )
        dot = dag._to_graphviz()

        # Verify all TF nodes
        assert "add_one" in dot.source
        assert "add_two" in dot.source
        assert "add_three" in dot.source
        # Only add_three's output goes to Output node (add_one and add_two are consumed)
        assert "Output Features" in dot.source
        # TF->TF edges should have linking column labels
        source = dot.source
        # add_one -> add_three edge and add_two -> add_three edge exist
        assert str(id(tf1)) in source
        assert str(id(tf3)) in source

    def test_visualize_aliased_multi_output(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf([float, float])
        def add_one(feature):
            return feature + 1, feature + 1

        @udf([float, float])
        def add_two(feature):
            return feature + 2, feature + 2

        @udf(float)
        def add(feature1, feature2):
            return feature1 + feature2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf3 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1("feature").alias("odt1_1", "odt1_2")
        tf2 = tf2("feature").alias("odt2_1", "odt2_2")
        tf3 = tf3("odt1_1", "odt2_2")

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2, tf3]
        )
        dot = dag._to_graphviz()

        source = dot.source
        # Verify TF nodes
        assert "add_one" in source
        assert "add_two" in source
        # Terminal outputs (not consumed by downstream): odt1_2, odt2_1, add
        assert "odt1_2" in source
        assert "odt2_1" in source
        # Intermediate outputs on TF->TF edges: odt1_1, odt2_2
        assert "odt1_1" in source
        assert "odt2_2" in source

    def test_visualize_dropped_features(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float, drop=["amount"])
        def min_max_scaler(amount):
            return amount * 2

        @udf(float, drop=["category"])
        def label_encoder(category):
            return category

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=label_encoder,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2]
        )
        dot = dag._to_graphviz()

        source = dot.source
        assert "drops: amount" in source
        assert "drops: category" in source
        assert "#EA5556" in source  # Red color for dropped features

    def test_visualize_orient_parameter(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1]
        )

        # Default is TB
        dot_tb = dag._to_graphviz()
        assert "rankdir=TB" in dot_tb.source

        # LR orientation
        dot_lr = dag._to_graphviz(orient="LR")
        assert "rankdir=LR" in dot_lr.source

    def test_visualize_pass_through_features(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float, drop=["amount"])
        def scale_amount(amount, price):
            return amount / price

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=scale_amount,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1]
        )
        dot = dag._to_graphviz()
        source = dot.source

        # price is not dropped -> should have a dashed pass-through edge to output
        assert "price" in source
        assert "dashed" in source
        # amount IS dropped -> should NOT appear in pass-through edge
        # (it still appears in Input->TF edge and drops label, but not in pass-through)

        text = str(dag)
        # Pass-through features in text output
        assert "price" in text
        assert "scale_amount" in text
        # Output should include both price (pass-through) and scale_amount (TF output)
        output_line = [
            line for line in text.split("\n") if line.startswith("Output Features:")
        ]
        assert len(output_line) == 1
        assert "price" in output_line[0]
        assert "scale_amount" in output_line[0]

    def test_visualize_no_pass_through_when_all_dropped(self, mocker):
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float, drop=["amount"])
        def min_max_scaler(amount):
            return amount * 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler,
            transformation_type=TransformationType.ON_DEMAND,
        )

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1]
        )
        dot = dag._to_graphviz()
        source = dot.source

        # amount is dropped -> no dashed pass-through edge
        assert "dashed" not in source

        text = str(dag)
        output_line = [
            line for line in text.split("\n") if line.startswith("Output Features:")
        ]
        assert len(output_line) == 1
        # Only TF output, no pass-through
        assert "min_max_scaler" in output_line[0]
        assert "amount" not in output_line[0]

    def test_visualize_dropped_intermediate_output(self, mocker):
        """Intermediate TF output dropped by downstream TF should NOT appear in Output edges."""
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf(float)
        def step1(raw_col):
            return raw_col * 2

        @udf(float, drop=["step1"])
        def step2(step1):
            return step1 + 1

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=step1,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=step2,
            transformation_type=TransformationType.ON_DEMAND,
        )

        tf1 = tf1("raw_col")
        tf2 = tf2("step1")

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1, tf2]
        )
        dot = dag._to_graphviz()
        source = dot.source

        # step2 output should map to Output
        assert 'output [label="step2"]' in source or "step2" in source
        # step1 is dropped by step2 -> should NOT have a TF edge to output
        # Filter TF->output edges (exclude input->output pass-through)
        tf_output_edges = [
            line
            for line in source.split("\n")
            if "-> output" in line and "input" not in line
        ]
        # Should have exactly 1 TF->output edge (from step2 TF node)
        assert len(tf_output_edges) == 1
        # The edge label should be "step2" not "step1"
        assert "step2" in tf_output_edges[0]
        assert "step1" not in tf_output_edges[0]

        # Text output should also exclude step1 from Output Features
        text = str(dag)
        output_line = [
            line for line in text.split("\n") if line.startswith("Output Features:")
        ]
        assert len(output_line) == 1
        assert "step2" in output_line[0]
        assert "step1" not in output_line[0]

    def test_visualize_single_edge_per_tf_to_output(self, mocker):
        """Multiple output columns from one TF should produce a single edge to Output."""
        feature_store_id = 99
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        @udf([float, float])
        def multi_out(col1):
            return col1 + 1, col1 + 2

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=multi_out,
            transformation_type=TransformationType.ON_DEMAND,
        )
        tf1 = tf1("col1").alias("out_a", "out_b")

        dag = transformation_function_engine.TransformationFunctionEngine.build_transformation_function_execution_graph(
            [tf1]
        )
        dot = dag._to_graphviz()
        source = dot.source

        # Should have exactly 1 TF->output edge (not 2 separate edges)
        tf_output_edges = [
            line
            for line in source.split("\n")
            if "-> output" in line and "input" not in line
        ]
        assert len(tf_output_edges) == 1
        # Both column names on the single edge label
        assert "out_a" in tf_output_edges[0]
        assert "out_b" in tf_output_edges[0]

    def test_visualize_graphviz_not_installed(self, mocker):
        mocker.patch("hsfs.core.transformation_function_api.TransformationFunctionApi")

        dag = transformation_function_engine.TransformationExecutionDAG(nodes=[])

        mocker.patch.dict("sys.modules", {"graphviz": None})

        with pytest.raises(ImportError, match="pip install graphviz"):
            dag._to_graphviz()
