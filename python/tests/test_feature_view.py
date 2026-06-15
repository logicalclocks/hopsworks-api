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
import json
import warnings

import pytest
from hopsworks_common import version
from hopsworks_common.client.exceptions import FeatureStoreException
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
    def test_constructed_feature_view_is_json_serializable(self, mocker):
        # The exact payload the client POSTs at feature view creation. A stale
        # attribute or method reference inside to_dict surfaces as an
        # AttributeError that the JSON encoder masks into "not JSON
        # serializable", so the suite must exercise the real serialization of a
        # constructor-built instance (a missed Tag._tags_to_dict rename
        # reached the cluster precisely because no unit test did).
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type")

        @udf(int)
        def plus_one(col1):
            return col1 + 1

        fg = feature_group.FeatureGroup(
            name="test_fg",
            version=1,
            featurestore_id=99,
            primary_key=[],
            partition_key=[],
            features=[feature.Feature("col1"), feature.Feature("col2")],
            id=11,
            stream=False,
        )
        fv = feature_view.FeatureView(
            name="test_fv",
            featurestore_id=99,
            query=fg.select_all(),
            version=1,
            transformation_functions=[plus_one],
        )

        payload = json.loads(fv.json())

        assert payload["name"] == "test_fv"
        assert payload["version"] == 1
        assert len(payload["transformationFunctions"]) == 1

    def test_from_response_json(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch.object(
            FeatureStore,
            "project_id",
            return_value=99,
        )
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi._get")
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
        mocker.patch("hsfs.engine._get_type")
        mocker.patch("hopsworks_common.client._get_instance")
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi._get")
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

    def test_on_demand_transformation_functions_dedup_multi_output_tf(self, mocker):
        """A multi-output ODT is attached to each feature it produces.

        Without dedup, the property returns the TF once per feature, and
        TransformationExecutionDAG raises "Output column 'x' is produced by both
        'tf' and 'tf'." when the backend response is read back at FV creation,
        breaking any chained pipeline with a multi-output TF (e.g.
        add_two -> odt2_1, odt2_2).
        """
        from hsfs.core import transformation_execution_dag
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf([float, float])
        def add_two(feature):
            return feature + 2, feature + 2

        tf = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_two,
            transformation_type=TransformationType.ON_DEMAND,
        )("feature").alias("odt2_1", "odt2_2")

        fv = feature_view.FeatureView(
            name="fv_dedup",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        # Multi-output TF -> the SAME TF appears on every output feature.
        # The backend response path produces fresh Python instances per feature,
        # so simulate that by attaching distinct copies that compare equal.
        fv.schema = [
            training_dataset_feature.TrainingDatasetFeature(
                name="odt2_1",
                type="float",
                transformation_function=tf,
            ),
            training_dataset_feature.TrainingDatasetFeature(
                name="odt2_2",
                type="float",
                transformation_function=tf,
            ),
        ]

        # Property must dedup
        odts = fv._on_demand_transformation_functions
        assert len(odts) == 1
        assert odts[0].hopsworks_udf.function_name == "add_two"

        # DAG construction must succeed (no "produced by both 'add_two' and 'add_two'")
        dag = transformation_execution_dag.TransformationExecutionDAG(odts)
        assert len(dag.nodes) == 1

    def test_from_response_json_hydrates_complete_on_demand_chain(
        self, mocker, backend_fixtures
    ):
        """The backend-serialized on-demand list is hydrated as the complete chain.

        A chain that drops an intermediate (apply_offset -> offset_count, then
        to_micrograms drops offset_count -> pm25_ugm3) surfaces only
        pm25_ugm3 as a feature. The producer apply_offset is attached to no
        feature, so reconstructing from features would lose it. Hydrating the
        backend list recovers the full chain and builds the on-demand graph
        from it.
        """
        import copy

        from hsfs.transformation_function import TransformationFunction

        mocker.patch.object(FeatureStore, "project_id", return_value=99)
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type")
        mocker.patch("hsfs.core.feature_store_api.FeatureStoreApi._get")

        @udf(float)
        def apply_offset(raw_pm25):
            return raw_pm25 + 1

        @udf(float)
        def to_micrograms(offset_count):
            return offset_count * 1000

        producer = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=apply_offset,
            transformation_type=TransformationType.ON_DEMAND,
        )("raw_pm25").alias("offset_count")
        surfaced = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=to_micrograms,
            transformation_type=TransformationType.ON_DEMAND,
        )("offset_count").alias("pm25_ugm3")

        json = copy.deepcopy(
            backend_fixtures["feature_view"]["get_transformations"]["response"]
        )
        json["onDemandTransformationFunctions"] = [
            producer.to_dict(),
            surfaced.to_dict(),
        ]

        fv = feature_view.FeatureView.from_response_json(json)

        odts = fv._on_demand_transformation_functions
        assert {o.hopsworks_udf.function_name for o in odts} == {
            "apply_offset",
            "to_micrograms",
        }
        assert all(o.transformation_type == TransformationType.ON_DEMAND for o in odts)
        # The producer of the dropped intermediate is part of the executable graph.
        assert len(fv._on_demand_transformation_execution_graph.nodes) == 2

    def test_on_demand_transformation_functions_prefers_serialized_list(self, mocker):
        """The hydrated list is authoritative; the feature-walk is only a fallback.

        The feature-walk recovers only transformations surfacing a feature, so a
        dropped-intermediate producer is missing from it. When the backend list
        is present it takes precedence and carries the full chain.
        """
        from hsfs.transformation_function import TransformationFunction

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(float)
        def apply_offset(raw_pm25):
            return raw_pm25 + 1

        @udf(float)
        def to_micrograms(offset_count):
            return offset_count * 1000

        producer = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=apply_offset,
            transformation_type=TransformationType.ON_DEMAND,
        )("raw_pm25").alias("offset_count")
        surfaced = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=to_micrograms,
            transformation_type=TransformationType.ON_DEMAND,
        )("offset_count").alias("pm25_ugm3")

        fv = feature_view.FeatureView(
            name="fv_chain",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        # Only the surfaced output is a feature; offset_count (and apply_offset)
        # surface nothing.
        fv.schema = [
            training_dataset_feature.TrainingDatasetFeature(
                name="pm25_ugm3",
                type="float",
                transformation_function=surfaced,
            ),
        ]

        # Fallback: the feature-walk recovers only the surfaced producer.
        assert [
            o.hopsworks_udf.function_name
            for o in fv._on_demand_transformation_functions
        ] == ["to_micrograms"]

        # Hydrated list takes precedence and carries the dropped-intermediate producer.
        fv._FeatureView__on_demand_transformation_functions = [producer, surfaced]
        assert {
            o.hopsworks_udf.function_name
            for o in fv._on_demand_transformation_functions
        } == {"apply_offset", "to_micrograms"}

    def test_construction_rejects_cyclic_transformations(self, mocker):
        """Building a feature view with a cyclic model-dependent chain is rejected.

        A cycle is caught when the execution DAG is built, which happens during
        feature view construction.
        """
        from hopsworks_common.client.exceptions import TransformationFunctionException
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def f_a(out_b):
            return out_b + 1

        @udf(int)
        def f_b(out_a):
            return out_a + 1

        tf_a = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=f_a("out_b").alias("out_a"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        tf_b = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=f_b("out_a").alias("out_b"),
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        with pytest.raises(
            TransformationFunctionException, match="Cyclic dependency detected"
        ):
            feature_view.FeatureView(
                name="fv_cyclic",
                query=fg1.select_features(),
                featurestore_id=99,
                featurestore_name="test_fs",
                transformation_functions=[tf_a, tf_b],
            )

    def test_warmup_transformation_workers_guard(self, mocker):
        """Pool warmup fires only for a parallel request on the Python engine with transformations."""
        from hsfs.core.transformation_function_engine import (
            TransformationFunctionEngine,
        )
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hopsworks_common.client._get_instance")
        get_type = mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )
        fv = feature_view.FeatureView(
            name="fv_warmup",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[mdt("fg1_feature")],
        )

        warmup = mocker.patch.object(
            TransformationFunctionEngine, "_warmup_online_workers"
        )

        # No parallelism requested: nothing to warm.
        fv._warmup_transformation_workers(None)
        fv._warmup_transformation_workers(1)
        warmup.assert_not_called()

        # Parallel request on the Python engine with transformations: warm the pool.
        fv._warmup_transformation_workers(4)
        warmup.assert_called_once_with(4)

        # Spark pushes transformations down, so the local pool is never warmed.
        warmup.reset_mock()
        get_type.return_value = "spark"
        fv._warmup_transformation_workers(4)
        warmup.assert_not_called()

        # A feature view with no transformations has nothing to warm.
        get_type.return_value = "python"
        fv_no_tf = feature_view.FeatureView(
            name="fv_no_tf",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )
        fv_no_tf._warmup_transformation_workers(4)
        warmup.assert_not_called()

    def test_init_serving_stores_and_warms_n_processes(self, mocker):
        """init_serving records the worker count and pre-spawns the pool."""
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=99,
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

        mocker.patch.object(fv._vector_server, "_init_serving")
        fv._vector_server.serving_keys = []
        mocker.patch.object(fv, "_get_embedding_fgs", return_value=[])
        # init_serving calls init_batch_scoring internally; isolate its own wiring.
        mocker.patch.object(fv, "init_batch_scoring")
        warmup = mocker.patch.object(fv, "_warmup_transformation_workers")

        fv.init_serving(training_dataset_version=1, n_processes=4)

        assert fv._transformation_n_processes == 4
        warmup.assert_called_once_with(4)

    def test_init_batch_scoring_stores_and_warms_n_processes(self, mocker):
        """init_batch_scoring records the worker count and pre-spawns the pool."""
        from hsfs.transformation_function import (
            TransformationFunction,
            TransformationType,
        )

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def add_one(feature):
            return feature + 1

        mdt = TransformationFunction(
            featurestore_id=99,
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

        mocker.patch.object(fv._batch_scoring_server, "_init_batch_scoring")
        warmup = mocker.patch.object(fv, "_warmup_transformation_workers")

        fv.init_batch_scoring(training_dataset_version=1, n_processes=4)

        assert fv._transformation_n_processes == 4
        warmup.assert_called_once_with(4)

    def test_get_feature_vector_defaults_n_processes_from_init(self, mocker):
        """get_feature_vector falls back to the worker count recorded by init_serving."""
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )

        fv._vector_server._serving_initialized = True
        fv._vector_db_client = None
        fv._transformation_n_processes = 4
        get_vector = mocker.patch.object(fv._vector_server, "_get_feature_vector")

        fv.get_feature_vector(entry={"primary_key": 1})

        assert get_vector.call_args.kwargs["n_processes"] == 4

        # An explicit value still overrides the recorded default.
        get_vector.reset_mock()
        fv.get_feature_vector(entry={"primary_key": 1}, n_processes=2)

        assert get_vector.call_args.kwargs["n_processes"] == 2

    def test_from_response_json_basic_info_deprecated(self, mocker, backend_fixtures):
        # Arrange
        mocker.patch("hsfs.engine._get_type")
        mocker.patch("hopsworks_common.client._get_instance")
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
            "hopsworks_common.client._get_connection", return_value=mocked_connection
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        mocker.patch("hsfs.engine._get_type", return_value="python")
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
            "hopsworks_common.client._get_connection", return_value=mocked_connection
        )
        mocker.patch("hsfs.core.feature_view_engine.FeatureViewEngine")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        json = backend_fixtures["feature_view"]["get_feature_view_logging_enabled"][
            "response"
        ]

        # Act
        fv = feature_view.FeatureView.from_response_json(json)

        # Assert
        assert fv.logging_enabled is True

    def test_label_column_name(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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

    def test_get_feature_delegates_to_query(self, mocker):
        # FeatureView.get_feature is a thin wrapper over Query.get_feature.
        # Verify the call routes through, the argument is forwarded
        # verbatim, and the query's return value is returned unchanged.
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            labels=[],
        )
        sentinel = object()
        mock_get_feature = mocker.patch.object(
            fv._query, "get_feature", return_value=sentinel
        )

        result = fv.get_feature("fg1_feature")

        mock_get_feature.assert_called_once_with("fg1_feature")
        assert result is sentinel

    def test_get_feature_propagates_not_found(self, mocker):
        # FeatureView.get_feature must surface a Query-level
        # FeatureStoreException unchanged — callers rely on the error
        # type to distinguish a missing feature from a real lookup result.
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            labels=[],
        )
        mocker.patch.object(
            fv._query,
            "get_feature",
            side_effect=FeatureStoreException("not found"),
        )

        with pytest.raises(FeatureStoreException, match="not found"):
            fv.get_feature("missing")

    def test_get_feature_propagates_ambiguity(self, mocker):
        # Same propagation contract for the ambiguous-name path so the
        # caller can prompt the user to pass a prefixed name.
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features().join(fg2.select_features()),
            featurestore_id=99,
            labels=[],
        )
        mocker.patch.object(
            fv._query,
            "get_feature",
            side_effect=FeatureStoreException("ambiguous"),
        )

        with pytest.raises(FeatureStoreException, match="ambiguous"):
            fv.get_feature("primary_key")

    def test_init_batch_scoring_delegates_to_server(self, mocker):
        # Regression: FeatureView.init_batch_scoring must call the (now
        # private) VectorServer._init_batch_scoring. A spec'd mock makes a
        # call to the pre-rename name (init_batch_scoring) raise instead of
        # silently passing on an auto-created attribute.
        from hsfs.core.vector_server import VectorServer

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            labels=[],
        )
        server = mocker.MagicMock(spec=VectorServer)
        fv._FeatureView__batch_scoring_server = server

        fv.init_batch_scoring(training_dataset_version=1)

        assert fv._serving_training_dataset_version == 1
        server._init_batch_scoring.assert_called_once_with(
            fv, training_dataset_version=1
        )

    def _fv_with_spec_server(self, mocker):
        # Build a FeatureView whose (lazy) vector servers are spec'd
        # VectorServer mocks. ``spec`` restricts the mock to attributes that
        # really exist on VectorServer, so any call to a pre-rename public
        # name (e.g. get_feature_vector instead of _get_feature_vector)
        # raises AttributeError instead of silently passing.
        from hsfs.core.vector_server import VectorServer

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")
        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            labels=[],
        )
        server = mocker.MagicMock(spec=VectorServer)
        server._serving_initialized = True
        server.serving_keys = []
        fv._FeatureView__vector_server = server
        fv._FeatureView__batch_scoring_server = server
        fv._vector_db_client = None
        return fv, server

    def test_init_serving_delegates_to_server(self, mocker):
        # Regression guard for the renamed VectorServer._init_serving.
        fv, server = self._fv_with_spec_server(mocker)
        mocker.patch.object(fv, "_get_embedding_fgs", return_value=[])

        fv.init_serving(training_dataset_version=1)

        server._init_serving.assert_called_once()
        assert server._init_serving.call_args.kwargs["entity"] is fv

    def test_get_feature_vector_delegates_to_server(self, mocker):
        # Regression guard for the renamed VectorServer._get_feature_vector.
        fv, server = self._fv_with_spec_server(mocker)

        result = fv.get_feature_vector(entry={"primary_key": 1})

        server._get_feature_vector.assert_called_once()
        assert result is server._get_feature_vector.return_value

    def test_get_feature_vectors_delegates_to_server(self, mocker):
        # Regression guard for the renamed VectorServer._get_feature_vectors.
        fv, server = self._fv_with_spec_server(mocker)

        result = fv.get_feature_vectors(entry=[{"primary_key": 1}])

        server._get_feature_vectors.assert_called_once()
        assert result is server._get_feature_vectors.return_value

    def test_get_inference_helper_delegates_to_server(self, mocker):
        # Regression guard for the renamed VectorServer._get_inference_helper.
        fv, server = self._fv_with_spec_server(mocker)

        result = fv.get_inference_helper(entry={"primary_key": 1})

        server._get_inference_helper.assert_called_once()
        assert result is server._get_inference_helper.return_value

    def test_get_inference_helpers_delegates_to_server(self, mocker):
        # Regression guard for the renamed VectorServer._get_inference_helpers.
        fv, server = self._fv_with_spec_server(mocker)

        result = fv.get_inference_helpers(entry=[{"primary_key": 1}])

        server._get_inference_helpers.assert_called_once()
        assert result is server._get_inference_helpers.return_value

    def test_transformed_feature_name(self, mocker):
        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "hsfs.core.feature_view_engine.FeatureViewEngine._delete"
        )
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline)
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_odts(data=df_test_data, online=False)

        mock_apply.assert_called_with(
            execution_graph=fv._on_demand_transformation_execution_graph,
            data=df_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online)
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_odts(data=dict_test_data, online=True)

        mock_apply.assert_called_with(
            execution_graph=fv._on_demand_transformation_execution_graph,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline) and transformation_context
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_odts(
            data=df_test_data, online=False, transformation_context=context
        )

        mock_apply.assert_called_with(
            execution_graph=fv._on_demand_transformation_execution_graph,
            data=df_test_data,
            online=False,
            transformation_context=context,
            request_parameters=None,
            n_processes=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online) and request_parameters
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_odts(
            data=dict_test_data, online=True, request_parameters=request_params
        )

        mock_apply.assert_called_with(
            execution_graph=fv._on_demand_transformation_execution_graph,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=request_params,
            n_processes=None,
        )
        assert result_dict == dict_test_data

    def test_execute_odts_no_transformations(self, mocker, caplog):
        import logging

        import pandas as pd

        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Act - online
        fv.execute_odts(data=online_test_data, online=True)

        # Assert - online
        mock_apply.assert_called_with(
            execution_graph=fv._on_demand_transformation_execution_graph,
            data=online_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )

        # Act - offline
        fv.execute_odts(data=offline_test_data, online=False)

        # Assert - offline
        mock_apply.assert_called_with(
            execution_graph=fv._on_demand_transformation_execution_graph,
            data=offline_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline)
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_mdts(data=df_test_data, online=False)

        mock_apply.assert_called_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=df_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online)
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_mdts(data=dict_test_data, online=True)

        mock_apply.assert_called_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Test with DataFrame (offline) and transformation_context
        df_test_data = pd.DataFrame({"fg1_feature": [1.0, 2.0, 3.0]})
        result_df = fv.execute_mdts(
            data=df_test_data, online=False, transformation_context=context
        )

        mock_apply.assert_called_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=df_test_data,
            online=False,
            transformation_context=context,
            request_parameters=None,
            n_processes=None,
        )
        pd.testing.assert_frame_equal(result_df, df_test_data)

        # Test with dict (online) and request_parameters
        dict_test_data = {"fg1_feature": 1.0}
        result_dict = fv.execute_mdts(
            data=dict_test_data, online=True, request_parameters=request_params
        )

        mock_apply.assert_called_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=dict_test_data,
            online=True,
            transformation_context=None,
            request_parameters=request_params,
            n_processes=None,
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            return_value=expected_result,
        )

        # Act
        result = fv.execute_mdts(data=test_data, online=False)

        # Assert
        mock_apply.assert_called_once_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )
        assert result is expected_result

    def test_execute_mdts_no_transformations(self, mocker, caplog):
        import logging

        import pandas as pd

        # Arrange
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
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
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
            "_apply_transformations",
            side_effect=lambda **kwargs: kwargs["data"],
        )

        # Act - online
        fv.execute_mdts(data=online_test_data, online=True)

        # Assert - online
        mock_apply.assert_called_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=online_test_data,
            online=True,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )

        # Act - offline
        fv.execute_mdts(data=offline_test_data, online=False)

        # Assert - offline
        mock_apply.assert_called_with(
            execution_graph=fv._model_dependent_transformation_execution_graph,
            data=offline_test_data,
            online=False,
            transformation_context=None,
            request_parameters=None,
            n_processes=None,
        )


class TestFeatureViewVisualizeTransformations:
    def _make_fv_with_odts(self, mocker, odts_list):
        """Helper: create a FeatureView and wire up ODTs."""
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

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
        return fv

    def test_visualize_transformations_model_dependent(self, mocker):
        from hsfs import transformation_function

        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        @udf(int)
        def add_one(col1):
            return col1 + 1

        tf1 = transformation_function.TransformationFunction(
            99,
            hopsworks_udf=add_one,
            transformation_type=TransformationType.MODEL_DEPENDENT,
        )

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
            transformation_functions=[tf1],
        )

        # visualize() returns None, so verify via _to_mermaid on the DAG
        src = fv._model_dependent_transformation_execution_graph._to_mermaid()
        assert "add_one" in src
        assert "Input Features" in src
        assert "Output Features" in src
        # Also verify visualize_transformations doesn't raise
        fv.visualize_transformations(kind="model_dependent", mode="text")

    def test_visualize_transformations_invalid_kind(self, mocker):
        mocker.patch("hopsworks_common.client._get_instance")
        mocker.patch("hsfs.engine._get_type", return_value="python")

        fv = feature_view.FeatureView(
            name="fv_name",
            query=fg1.select_features(),
            featurestore_id=99,
            featurestore_name="test_fs",
        )

        with pytest.raises(ValueError, match="Invalid kind"):
            fv.visualize_transformations(kind="invalid")
