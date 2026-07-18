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

from types import SimpleNamespace

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
from hsfs.builtin_transformations import impute_mean, min_max_scaler
from hsfs.core import (
    statistics_engine,
    transformation_execution_dag,
    transformation_function_engine,
)
from hsfs.core.feature_descriptive_statistics import FeatureDescriptiveStatistics
from hsfs.engine import python, spark
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationType
from hsfs.transformation_statistics import TransformationStatistics


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
engine._init("python")
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
        tf_engine._save(transformation_fn_instance=tf)

        # Assert
        assert mock_tf_api.return_value._register_transformation_fn.call_count == 1

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

        mock_tf_api.return_value._get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine._get_transformation_fn(name=None, version=None)

        # Assert
        assert mock_tf_api.return_value._get_transformation_fn.call_count == 1
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

        mock_tf_api.return_value._get_transformation_fn.return_value = transformations

        # Act
        result = tf_engine._get_transformation_fns()

        # Assert
        assert mock_tf_api.return_value._get_transformation_fn.call_count == 1
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
        tf_engine._delete(transformation_function_instance=tf1)

        # Assert
        assert mock_tf_api.return_value._delete.call_count == 1

    def test_compute_transformation_fn_statistics(self, mocker):
        # Arrange
        feature_store_id = 99

        mocker.patch("hopsworks_common.client._get_instance")
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
        tf_engine._compute_transformation_fn_statistics(
            training_dataset_obj=td,
            statistics_features=None,
            label_encoder_features=None,
            feature_dataframe=None,
            feature_view_obj=None,
        )

        # Assert
        assert (
            mock_s_engine.return_value._compute_transformation_fn_statistics.call_count
            == 1
        )

    def test_fit_and_transform_no_feature_view_returns_unchanged(self, mocker):
        # A standalone training dataset (e.g. td.insert(query)) has no feature
        # view and no transformation functions, so the dataset is returned
        # unchanged rather than dereferencing a None feature view.
        mocker.patch("hopsworks_common.client._get_instance")

        dataset = pd.DataFrame({"id": [1, 2]})

        # Act
        result = transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=None,
            feature_view_obj=None,
            dataset=dataset,
        )

        # Assert
        assert result is dataset

    def test_fit_and_transform_no_statistics_no_split(self, mocker):
        # Transformations without statistics fit nothing: the frame gets one
        # plain transform and the statistics engine is never touched.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")
        mock_apply = mocker.patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "_apply_transformation_functions",
            side_effect=lambda data, **kwargs: data,
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

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        dataset = pd.DataFrame()

        # Act
        result = tf_engine._fit_and_transform(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Assert
        assert result is dataset
        assert mock_apply.call_count == 1
        assert (
            mock_s_engine.return_value._compute_transformation_fn_statistics.call_count
            == 0
        )

    def test_fit_and_transform_chained_intermediate(self, mocker):
        # A downstream transformation that needs the statistics of an
        # intermediate feature (impute-then-scale) must have those statistics
        # fit on the materialized intermediate, not skipped because the
        # intermediate is absent from the raw data. Verifies the DAG-interleaved
        # fit: raw-feature statistics first, the producing transformation
        # applied, then the intermediate's statistics fit on the now-materialized
        # column.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        tf_impute = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=impute_mean("feature_1").alias("imputed_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf_scale = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler("imputed_feature_1").alias("scaled_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fg = feature_group.FeatureGroup(
            name="t",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("feature_1"), feature.Feature("other")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="t",
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            transformation_functions=[tf_impute, tf_scale],
        )
        td = training_dataset.TrainingDataset(
            name="t",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
        )
        dataset = pd.DataFrame(
            {"feature_1": [1.0, None, 3.0, 5.0], "other": [10, 20, 30, 40]}
        )

        saved = []

        def fake_save(
            feature_descriptive_statistics, td_metadata_instance, feature_view_obj
        ):
            saved.append(list(feature_descriptive_statistics))
            return SimpleNamespace(
                feature_descriptive_statistics=feature_descriptive_statistics
            )

        mocker.patch.object(
            statistics_engine.StatisticsEngine,
            "_save_transformation_fn_statistics",
            side_effect=fake_save,
        )

        transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Statistics are persisted in a single save (one call), so serving
        # retrieves one complete entity covering both the raw feature and the
        # intermediate.
        assert len(saved) == 1
        persisted = {fds.feature_name: fds for fds in saved[0]}
        assert {"feature_1", "imputed_feature_1"} <= set(persisted)
        # The intermediate was profiled on values its producing transformation
        # materialized first (impute_mean fills the missing entry of
        # [1, None, 3, 5] with 3), and the persisted statistics are exactly the
        # fitted ones.
        assert persisted["imputed_feature_1"].mean == 3.0

    @staticmethod
    def _fake_persisting_statistics(mocker):
        # Skip the backend save in both persisting paths. The raw-feature fit
        # profiles and persists through TransformationFunctionEngine
        # ._compute_transformation_fn_statistics; the chained fit persists the
        # provisional statistics it fitted with through StatisticsEngine
        # ._save_transformation_fn_statistics.
        def fake_compute(
            training_dataset,
            columns,
            label_encoder_features,
            feature_dataframe,
            feature_view_obj,
        ):
            descriptive = [
                FeatureDescriptiveStatistics(
                    feature_name=c,
                    mean=float(feature_dataframe[c].mean()),
                    min=float(feature_dataframe[c].min()),
                    max=float(feature_dataframe[c].max()),
                )
                for c in columns
            ]
            return SimpleNamespace(feature_descriptive_statistics=descriptive)

        mocker.patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "_compute_transformation_fn_statistics",
            side_effect=fake_compute,
        )

        def fake_save(
            feature_descriptive_statistics, td_metadata_instance, feature_view_obj
        ):
            return SimpleNamespace(
                feature_descriptive_statistics=feature_descriptive_statistics
            )

        return mocker.patch.object(
            statistics_engine.StatisticsEngine,
            "_save_transformation_fn_statistics",
            side_effect=fake_save,
        )

    def test_fit_and_transform_chained_fused(self, mocker):
        # The chained fit returns the transformed train data (fused
        # fit_transform): each transformation executes exactly once during
        # fitting and the result is identical to applying the full DAG with the
        # fitted statistics, so callers skip their own apply.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        tf_impute = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=impute_mean("feature_1").alias("imputed_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf_scale = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler("imputed_feature_1").alias("scaled_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fg = feature_group.FeatureGroup(
            name="t",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("feature_1"), feature.Feature("other")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="t",
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            transformation_functions=[tf_impute, tf_scale],
        )
        td = training_dataset.TrainingDataset(
            name="t",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
        )
        dataset = pd.DataFrame(
            {"feature_1": [1.0, None, 3.0, 5.0], "other": [10, 20, 30, 40]}
        )

        self._fake_persisting_statistics(mocker)
        execute_spy = mocker.spy(
            transformation_function_engine.TransformationFunctionEngine,
            "_execute_udf",
        )

        transformed = transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # The fused pass executed every transformation exactly once.
        assert execute_spy.call_count == 2
        assert transformed is not None

        expected = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=dataset,
            online=False,
        )
        pd.testing.assert_frame_equal(transformed, expected)

    def test_fit_and_transform_chained_fused_drop(self, mocker):
        # Drops are deferred during the fused pass (later stages may still need
        # the column) and applied once in the final projection, so the returned
        # frame matches a plain DAG apply, including the dropped input.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        @udf(float, drop=["feature_1"])
        def shift(feature_1):
            return feature_1 + 1.0

        tf_shift = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=shift("feature_1").alias("shifted"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf_scale = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler("shifted").alias("scaled"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fg = feature_group.FeatureGroup(
            name="t",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("feature_1"), feature.Feature("other")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="t",
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            transformation_functions=[tf_shift, tf_scale],
        )
        td = training_dataset.TrainingDataset(
            name="t",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
        )
        dataset = pd.DataFrame(
            {"feature_1": [1.0, 2.0, 3.0, 5.0], "other": [10, 20, 30, 40]}
        )

        self._fake_persisting_statistics(mocker)

        transformed = transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        assert "feature_1" not in transformed.columns
        expected = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=dataset,
            online=False,
        )
        pd.testing.assert_frame_equal(transformed, expected)

    def test_fit_and_transform_chained_context(self, mocker):
        # The fused pass must execute transformations with the caller's
        # transformation context, so statistics are fit on the same intermediate
        # values the real transform produces.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        @udf(float)
        def add_offset(feature_1, context):
            return feature_1 + context["offset"]

        tf_offset = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=add_offset("feature_1").alias("offset_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf_scale = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler("offset_feature_1").alias("scaled_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fg = feature_group.FeatureGroup(
            name="t",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("feature_1")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="t",
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            transformation_functions=[tf_offset, tf_scale],
        )
        td = training_dataset.TrainingDataset(
            name="t",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
        )
        dataset = pd.DataFrame({"feature_1": [1.0, 2.0, 3.0]})

        fake_persist = self._fake_persisting_statistics(mocker)

        transformed = transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=td,
            feature_view_obj=fv,
            dataset=dataset,
            transformation_context={"offset": 100.0},
        )

        # The statistics were fit on the intermediate as the context-aware
        # transformation produced it (before this fix the fused fitting pass ran
        # with no context, so a context UDF could not even execute), and the
        # persisted statistics are those fitted values.
        persisted = {fds.feature_name: fds for fds in fake_persist.call_args[0][0]}
        assert persisted["offset_feature_1"].min == 101.0
        assert persisted["offset_feature_1"].max == 103.0
        # min_max_scaler drops its input, so the final frame carries the scaled
        # output computed from the context-shifted intermediate.
        assert transformed["scaled_feature_1"].tolist() == [0.0, 0.5, 1.0]

    def test_fit_and_transform_chained_fused_splits(self, mocker):
        # With splits, statistics are fit on the train split only; the fused
        # pass transforms it along the way (each transformation executes once
        # on it) and the remaining splits get one plain apply each.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        tf_impute = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=impute_mean("feature_1").alias("imputed_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf_scale = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=min_max_scaler("imputed_feature_1").alias("scaled_feature_1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fg = feature_group.FeatureGroup(
            name="t",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("feature_1"), feature.Feature("other")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="t",
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            transformation_functions=[tf_impute, tf_scale],
        )
        td = training_dataset.TrainingDataset(
            name="t",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={"train": 0.8, "test": 0.2},
            train_split="train",
            id=10,
        )
        train_df = pd.DataFrame(
            {"feature_1": [1.0, None, 3.0, 5.0], "other": [10, 20, 30, 40]}
        )
        test_df = pd.DataFrame({"feature_1": [2.0, 4.0], "other": [50, 60]})

        self._fake_persisting_statistics(mocker)
        execute_spy = mocker.spy(
            transformation_function_engine.TransformationFunctionEngine,
            "_execute_udf",
        )

        transformed = transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=td,
            feature_view_obj=fv,
            dataset={"train": train_df, "test": test_df},
        )

        # 2 executions for the fused train pass + 2 for the test apply: the
        # train split is never transformed twice.
        assert execute_spy.call_count == 4
        expected_test = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=test_df,
            online=False,
        )
        pd.testing.assert_frame_equal(transformed["test"], expected_test)
        expected_train = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=train_df,
            online=False,
        )
        pd.testing.assert_frame_equal(transformed["train"], expected_train)

    def test_fit_and_transform_overwrite_feature_fits_on_raw_values(self, mocker):
        # A transformation that overwrites the feature it requires statistics
        # on (output name == input name) consumes the raw feature, not a
        # chained intermediate, so the fit must take the single-profile path
        # and persist statistics of the raw values. Before this fix the name
        # collision routed it through the chained fit, which re-profiled the
        # frame after the overwrite and persisted mean(raw + mean) instead of
        # mean(raw), breaking serving.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        @udf(float)
        def feature_1(feature_1, statistics=TransformationStatistics("feature_1")):  # noqa: B008
            return feature_1 + statistics.feature_1.mean

        tf_overwrite = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=feature_1,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fg = feature_group.FeatureGroup(
            name="t",
            version=1,
            featurestore_id=feature_store_id,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("feature_1")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="t",
            featurestore_id=feature_store_id,
            query=fg.select_all(),
            transformation_functions=[tf_overwrite],
        )
        td = training_dataset.TrainingDataset(
            name="t",
            version=1,
            data_format="CSV",
            featurestore_id=feature_store_id,
            splits={},
            id=10,
        )
        dataset = pd.DataFrame({"feature_1": [1.0, 2.0, 3.0]})

        profiled_frames = []

        def fake_compute(
            training_dataset,
            columns,
            label_encoder_features,
            feature_dataframe,
            feature_view_obj,
        ):
            profiled_frames.append(feature_dataframe.copy())
            descriptive = [
                FeatureDescriptiveStatistics(
                    feature_name=c, mean=float(feature_dataframe[c].mean())
                )
                for c in columns
            ]
            return SimpleNamespace(feature_descriptive_statistics=descriptive)

        mocker.patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "_compute_transformation_fn_statistics",
            side_effect=fake_compute,
        )

        transformed = transformation_function_engine.TransformationFunctionEngine._fit_and_transform(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # One profiling pass, on the raw values (mean 2.0), before the
        # transformation overwrites the column.
        assert len(profiled_frames) == 1
        assert profiled_frames[0]["feature_1"].tolist() == [1.0, 2.0, 3.0]
        # The transformation applied the raw-fitted mean.
        assert transformed["feature_1"].tolist() == [3.0, 4.0, 5.0]

    def test_apply_to_dict_prefixed_udf_unprefixed_request_parameter(self, mocker):
        # A request parameter supplied unprefixed must reach a prefixed
        # transformation when the prefixed feature is absent from the row (the
        # dropped raw input of an on-demand transformation at serving). Before
        # this fix the per-function row snapshot materialized the missing
        # prefixed name as a None placeholder, defeating _execute_udf's
        # unprefixed fallback.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        @udf(int, drop=["col1"], mode="python")
        def add_col1_col2(col1, col2):
            return col1 + col2

        prefixed_udf = add_col1_col2("col1", "col2")
        prefixed_udf.feature_name_prefix = "fg2_"
        tf_prefixed = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=prefixed_udf,
            transformation_type=TransformationType.ON_DEMAND,
        )
        graph = transformation_execution_dag.TransformationExecutionDAG([tf_prefixed])

        result = transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=graph,
            data={"index": 1, "fg2_col2": 3},
            online=True,
            request_parameters={"col1": 10},
        )

        assert result["fg2_add_col1_col2"] == 13

    def test_apply_transformation_functions_defaults_to_sequential(self, mocker):
        # Without an explicit n_processes the DAG runs sequentially, regardless
        # of input size or DAG width: parallelism is strictly opt-in.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")
        mock_parallel = mocker.patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "_apply_to_dataframe_parallel",
        )

        @udf(float)
        def plus_one(col1):
            return col1 + 1.0

        @udf(float)
        def plus_two(col2):
            return col2 + 2.0

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=plus_one("col1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=plus_two("col2"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        data = pd.DataFrame({"col1": [1.0] * 20_000, "col2": [2.0] * 20_000})

        transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
            execution_graph=dag, data=data, online=False
        )

        mock_parallel.assert_not_called()

    def test_apply_transformation_functions_caps_n_processes_with_warning(self, mocker):
        # A worker count above the DAG's maximum parallelism cannot be used;
        # it is capped and the caller is warned about the effective ceiling.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        def fake_parallel(
            execution_graph, data, online, n_processes, engine_type, column_store, merge
        ):
            for tf in execution_graph.nodes:
                for col in tf.hopsworks_udf.output_column_names:
                    column_store[col] = data[data.columns[0]]

        mock_parallel = mocker.patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "_apply_to_dataframe_parallel",
            side_effect=fake_parallel,
        )

        @udf(float)
        def plus_one(col1):
            return col1 + 1.0

        @udf(float)
        def plus_two(col2):
            return col2 + 2.0

        tf1 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=plus_one("col1"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf2 = transformation_function.TransformationFunction(
            feature_store_id,
            hopsworks_udf=plus_two("col2"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        data = pd.DataFrame({"col1": [1.0, 2.0], "col2": [2.0, 3.0]})

        with pytest.warns(UserWarning, match="maximum parallelism"):
            transformation_function_engine.TransformationFunctionEngine._apply_transformation_functions(
                execution_graph=dag, data=data, online=False, n_processes=8
            )

        # The parallel path runs with the capped worker count (the DAG width).
        assert mock_parallel.call_args[0][3] == 2

    def test_fit_and_transform_no_statistics_train_test_split(self, mocker):
        # Splits without statistics: every split gets one plain transform and
        # the statistics engine is never touched.
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
        mock_s_engine = mocker.patch("hsfs.core.statistics_engine.StatisticsEngine")
        mock_apply = mocker.patch.object(
            transformation_function_engine.TransformationFunctionEngine,
            "_apply_transformation_functions",
            side_effect=lambda data, **kwargs: data,
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
            train_split="train",
            id=10,
        )

        fv = feature_view.FeatureView(
            name="test",
            featurestore_id=feature_store_id,
            query=fg1.select_all(),
            transformation_functions=[tf1],
        )

        dataset = {"train": pd.DataFrame(), "test": pd.DataFrame()}

        # Act
        result = tf_engine._fit_and_transform(
            training_dataset=td, feature_view_obj=fv, dataset=dataset
        )

        # Assert
        assert result["train"] is dataset["train"]
        assert result["test"] is dataset["test"]
        assert mock_apply.call_count == 2
        assert (
            mock_s_engine.return_value._compute_transformation_fn_statistics.call_count
            == 0
        )

    def test_get_and_set_feature_statistics_no_statistics_required(self, mocker):
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
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
        tf_engine._get_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_s_engine.return_value._get.call_count == 0

    def test_get_and_set_feature_statistics_statistics_required(self, mocker):
        feature_store_id = 99
        mocker.patch("hopsworks_common.client._get_instance")
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
        tf_engine._get_and_set_feature_statistics(
            training_dataset=td, feature_view_obj=fv, training_dataset_version=1
        )

        # Assert
        assert mock_s_engine.return_value._get.call_count == 1

    def test_execute_udf_on_supported_dataframe(self, mocker, python_engine):
        # Arrange
        @udf(int)
        def add_one(col1):
            return col1 + 1

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        add_one.output_column_names = ["col1"]

        # Act
        result = tf_engine._execute_udf(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )

        # Act
        with pytest.raises(exceptions.FeatureStoreException) as e_info:
            tf_engine._execute_udf(udf=add_one, data=1, online=False)

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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        add_one.output_column_names = [
            "col1"
        ]  # Output column names would already be set when transformation function is executed using TransformationFunctionEngine.

        data = {"col1": 1}

        # Act
        result = tf_engine._execute_udf(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        add_one.output_column_names = [
            "col1"
        ]  # Mocking the output column names, this would be generated when a transformation function is created.

        # Act
        result = tf_engine._apply_udf_on_dict(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        tf_engine = transformation_function_engine.TransformationFunctionEngine(
            feature_store_id=99
        )
        add_one.output_column_names = [
            "col1"
        ]  # Mocking the output column names, this would be generated when a transformation function is created.

        # Act
        result = tf_engine._apply_udf_on_dict(
            udf=add_one, data={"col1": 1}, online=True
        )

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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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

        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
            tf_engine._apply_transformation_functions(
                execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            tf_engine._apply_transformation_functions(
                execution_graph=transformation_execution_dag.TransformationExecutionDAG(
                    [tf1, tf2]
                ),
                data=dataset,
                online=False,
            )

        assert (
            str(e_info.value)
            == "The following feature(s): `col2`, required for the transformation function 'add_two' are not available."
        )

    def test_transformed_column_layout(self, mocker):
        """Drops a transformation's dropped features unless they are expected.

        Expected features are kept (they are shared across feature groups), and
        surviving original columns are ordered before the transformation outputs.
        """

        @udf(int, drop=["col1"])
        def add_one(col1):
            return col1 + 1

        @udf(int, drop=["col2"])
        def add_two(col2):
            return col2 + 2

        mocker.patch("hopsworks_common.client._get_instance")
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
        dag = transformation_execution_dag.TransformationExecutionDAG([tf1, tf2])
        cls = transformation_function_engine.TransformationFunctionEngine

        # col2 is expected, so it is kept even though add_two drops it.
        dropped, order = cls._transformed_column_layout(
            dag, ["col1", "col2", "passthrough"], expected_features={"col2"}
        )
        assert dropped == {"col1"}
        # Surviving original columns first, in input order; then the outputs.
        assert order[:3] == ["col1", "col2", "passthrough"]
        assert set(order[3:]) == {"add_one_col1_", "add_two_col2_"}

        # With no expected features both declared drops apply.
        dropped_all, _ = cls._transformed_column_layout(
            dag, ["col1", "col2"], expected_features=None
        )
        assert dropped_all == {"col1", "col2"}

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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")

        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
            tf_engine._apply_transformation_functions(
                execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
            tf_engine._apply_transformation_functions(
                execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
            tf_engine._apply_transformation_functions(
                execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
            tf_engine._apply_transformation_functions(
                execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
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
        mocker.patch("hopsworks_common.client._get_instance")
        hopsworks_common.connection._hsfs_engine_type = "python"
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)

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
        result = tf_engine._apply_transformation_functions(
            execution_graph=transformation_execution_dag.TransformationExecutionDAG(
                fg.transformation_functions
            ),
            data=data,
            request_parameters={"tf_name": 10},
            online=online,
        )

        # Assert
        assert result["add_one"] == 11

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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

        execution_graph = transformation_execution_dag.TransformationExecutionDAG(
            [tf3, tf1, tf2]
        )

        # Verify topological order: roots before dependents despite input order
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf1)
        assert execution_graph.nodes.index(tf3) > execution_graph.nodes.index(tf2)

        result = tf_engine._apply_transformation_functions(
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

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_instance", return_value=python_engine)
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

        execution_graph = transformation_execution_dag.TransformationExecutionDAG(
            [tf3, tf1, tf2]
        )

        result = tf_engine._apply_transformation_functions(
            execution_graph=execution_graph,
            data=dataset,
            online=False,
        )

        assert result["odt1_1"] == 1.25
        assert result["odt2_2"] == 2.25
        assert result["add"] == 3.5
