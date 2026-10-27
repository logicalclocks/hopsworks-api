#
#   Copyright 2026 Hopsworks AB
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

import pytest
from hopsworks_common import tracing
from hsfs import engine, transformation_function
from hsfs.core import (
    transformation_execution_dag,
    transformation_telemetry,
    vector_server,
)
from hsfs.engine import python
from hsfs.hopsworks_udf import udf
from hsfs.transformation_function import TransformationType


prometheus_client = pytest.importorskip("prometheus_client")
trace = pytest.importorskip("opentelemetry.trace")
FEATURE_VIEW = "fraud"
VERSION = 3
SECOND_NS = 1_000_000_000


def _tf(fn, transformation_type=TransformationType.ON_DEMAND):
    return transformation_function.TransformationFunction(
        99, hopsworks_udf=fn, transformation_type=transformation_type
    )


@pytest.fixture
def graphs(mocker):
    """On-demand: `total` depends on `doubled`, `flag` on neither. Model-dependent: `scaled` of `total`."""
    mocker.patch("hopsworks_common.client._get_instance")
    engine._set_instance(engine=python.Engine(), engine_type="python")

    @udf(int, mode="python")
    def doubled(amount):
        return amount * 2

    @udf(int, mode="python")
    def total(doubled, fee):
        return doubled + fee

    @udf(bool, mode="python")
    def flag(amount):
        return amount > 100

    @udf(int, mode="python")
    def scaled(total):
        return total * 10

    on_demand = transformation_execution_dag.TransformationExecutionDAG(
        [_tf(doubled("amount")), _tf(total("doubled", "fee")), _tf(flag("amount"))]
    )
    model_dependent = transformation_execution_dag.TransformationExecutionDAG(
        [_tf(scaled("total"), TransformationType.MODEL_DEPENDENT)]
    )
    return on_demand, model_dependent


@pytest.fixture
def telemetry_off(mocker):
    mocker.patch.object(tracing, "_available", False)
    mocker.patch.object(tracing, "_enabled", False)
    mocker.patch.dict(transformation_telemetry._telemetries, clear=True)


@pytest.fixture
def telemetry_on(mocker, span_exporter):
    # span_exporter switches telemetry on; patching it again here would restore the switched-on value at teardown.
    mocker.patch.dict(transformation_telemetry._telemetries, clear=True)
    return span_exporter


def _server(mocker, graphs):
    on_demand, model_dependent = graphs
    server = vector_server.VectorServer.__new__(vector_server.VectorServer)
    server._feature_view_name = FEATURE_VIEW
    server._feature_view_version = VERSION
    server._on_demand_transformation_functions_execution_graph = on_demand
    server._model_dependent_transformation_functions_execution_graph = model_dependent
    server._on_demand_feature_vector_col_name = ["amount", "fee", "total"]
    mocker.patch.object(server, "_check_missing_request_parameters")
    mocker.patch.object(
        vector_server.VectorServer,
        "transformed_feature_vector_col_name",
        new_callable=mocker.PropertyMock,
        return_value=[],
    )
    if tracing._is_available():
        server._telemetry = transformation_telemetry._Telemetry(
            FEATURE_VIEW,
            VERSION,
            {
                transformation_telemetry.KIND_ON_DEMAND: on_demand,
                transformation_telemetry.KIND_MODEL_DEPENDENT: model_dependent,
            },
        )
    return server


def _apply(server, amount=60):
    return server._apply_transformation(
        {"amount": amount, "fee": 1}, request_parameter={}
    )


def _outputs(graph):
    return ",".join(graph.nodes[0].hopsworks_udf.output_column_names)


def _metrics():
    registry = prometheus_client.CollectorRegistry()
    registry.register(transformation_telemetry._Collector())
    return registry


def _sample(registry, name, kind="on_demand", **labels):
    return registry.get_sample_value(
        name,
        {
            "feature_view": FEATURE_VIEW,
            "feature_view_version": str(VERSION),
            "kind": kind,
            **labels,
        },
    )


class TestDisabled:
    def test_transforms_without_reading_a_clock(self, mocker, graphs, telemetry_off):
        clock = mocker.patch.object(transformation_telemetry.time, "perf_counter_ns")
        server = _server(mocker, graphs)
        traced = mocker.spy(server, "_apply_transformation_traced")
        child_span = mocker.spy(tracing, "_child_span")

        _, encoded = _apply(server)

        assert 1210 in encoded.values()
        assert server._telemetry is None
        clock.assert_not_called()
        traced.assert_not_called()
        child_span.assert_not_called()
        assert transformation_telemetry._telemetries == {}

    def test_helpers_hand_out_one_shared_no_op(self, telemetry_off):
        assert tracing._child_span("x") is tracing._NO_SPAN
        assert tracing._capture() is None
        assert tracing._take_row() is False


class TestEnabled:
    def test_sampled_request_traces_its_first_row(self, mocker, graphs, telemetry_on):
        server = _server(mocker, graphs)

        with (
            trace.get_tracer("test").start_as_current_span("predict"),
            tracing._request_scope(1),
        ):
            _apply(server)
            _apply(server, amount=70)

        spans = telemetry_on.get_finished_spans()
        names = [span.name for span in spans]
        assert names.count("transformations") == 1
        assert names.count("doubled") == 1
        by_name = {span.name: span for span in spans}
        row_id = by_name["transformations"].context.span_id
        assert by_name["on_demand_transformations"].parent.span_id == row_id
        assert by_name["model_dependent_transformations"].parent.span_id == row_id
        assert (
            by_name["total"].parent.span_id
            == by_name["on_demand_transformations"].context.span_id
        )
        assert by_name["total"].attributes["hopsworks.transformation.depends_on"] == (
            "doubled"
        )
        assert by_name["doubled"].end_time <= by_name["total"].start_time
        # Both rows are measured, though only the first is traced.
        metrics = _metrics()
        assert _sample(metrics, "hopsworks_transformations_calls_total") == 2
        assert (
            _sample(
                metrics,
                "hopsworks_transformation_function_calls_total",
                kind="model_dependent",
                transformation="scaled",
                outputs=_outputs(graphs[1]),
            )
            == 2
        )

    def test_only_the_serving_vector_server_is_timed(self, mocker, telemetry_on):
        # A batch scoring server of the same feature view version would replace the serving one's series.
        mocker.patch.object(
            vector_server.tf_engine_mod.TransformationFunctionEngine,
            "_get_ready_to_use_transformation_fns",
            return_value=[],
        )
        entity = mocker.Mock(labels=[], features=[])
        entity._on_demand_transformation_functions = []
        server = vector_server.VectorServer.__new__(vector_server.VectorServer)
        server._feature_view_name = FEATURE_VIEW
        server._feature_view_version = VERSION
        server._training_dataset_version = None

        server._init_transformation(entity)
        assert server._telemetry is None
        server._init_transformation(entity, timed=True)
        assert server._telemetry is not None

    def test_separate_phase_passes_each_trace_the_first_row(
        self, mocker, graphs, telemetry_on
    ):
        server = _server(mocker, graphs)
        on_demand, model_dependent = graphs

        with (
            trace.get_tracer("test").start_as_current_span("predict"),
            tracing._request_scope(1),
        ):
            traced = []
            for graph in (on_demand, on_demand, model_dependent, model_dependent):
                with server._telemetry._row(graph) as row_traced:
                    traced.append(row_traced)

        assert traced == [True, False, True, False]

    def test_switching_tracing_off_stops_measuring_until_it_is_back_on(
        self, mocker, graphs, telemetry_on
    ):
        server = _server(mocker, graphs)

        tracing._set_active(False)
        clock = mocker.spy(transformation_telemetry.time, "perf_counter_ns")
        with (
            trace.get_tracer("test").start_as_current_span("predict"),
            tracing._request_scope(1),
        ):
            _, encoded = _apply(server)
        assert 1210 in encoded.values()
        clock.assert_not_called()
        assert telemetry_on.get_finished_spans()[:-1] == ()
        assert _sample(_metrics(), "hopsworks_transformations_calls_total") == 0

        tracing._set_active(True)
        with (
            trace.get_tracer("test").start_as_current_span("predict"),
            tracing._request_scope(1),
        ):
            _apply(server)
        names = [span.name for span in telemetry_on.get_finished_spans()]
        assert names.count("transformations") == 1
        assert _sample(_metrics(), "hopsworks_transformations_calls_total") == 1

    def test_unsampled_request_is_measured_but_not_traced(
        self, mocker, graphs, telemetry_on
    ):
        server = _server(mocker, graphs)

        with tracing._request_scope(1):
            _apply(server)

        assert telemetry_on.get_finished_spans() == ()
        metrics = _metrics()
        assert _sample(metrics, "hopsworks_transformations_calls_total") == 1
        assert _sample(metrics, "hopsworks_transformations_seconds_total") > 0

    def test_exports_dag_edges(self, mocker, graphs, telemetry_on):
        _server(mocker, graphs)
        metrics = _metrics()

        def edge(name, depends_on, kind="on_demand", outputs=None):
            return _sample(
                metrics,
                "hopsworks_transformation_dag_edge",
                kind=kind,
                transformation=name,
                outputs=outputs or name,
                depends_on=depends_on,
            )

        assert edge("doubled", "") == 1
        assert edge("flag", "") == 1
        assert edge("total", "doubled") == 1
        assert edge("total", "") is None
        assert (
            edge("scaled", "", kind="model_dependent", outputs=_outputs(graphs[1])) == 1
        )

    def test_failed_function_counts_an_error_and_fails_its_span(
        self, mocker, telemetry_on
    ):
        mocker.patch("hopsworks_common.client._get_instance")
        engine._set_instance(engine=python.Engine(), engine_type="python")

        @udf(int, mode="python")
        def broken(amount):
            raise ValueError("bad amount")

        on_demand = transformation_execution_dag.TransformationExecutionDAG(
            [_tf(broken("amount"))]
        )
        server = _server(
            mocker,
            (on_demand, transformation_execution_dag.TransformationExecutionDAG([])),
        )

        with (
            trace.get_tracer("test").start_as_current_span("predict"),
            tracing._request_scope(1),
            pytest.raises(Exception, match="bad amount"),
        ):
            _apply(server)

        assert (
            _sample(
                _metrics(),
                "hopsworks_transformation_function_errors_total",
                transformation="broken",
                outputs="broken",
            )
            == 1
        )
        [span] = [s for s in telemetry_on.get_finished_spans() if s.name == "broken"]
        assert span.status.status_code == trace.StatusCode.ERROR

    def test_no_row_span_when_no_transformation_runs(
        self, mocker, graphs, telemetry_on
    ):
        server = _server(mocker, graphs)
        empty = transformation_execution_dag.TransformationExecutionDAG([])

        with (
            trace.get_tracer("test").start_as_current_span("predict"),
            tracing._request_scope(1),
            server._telemetry._row(empty, None) as traced,
        ):
            assert traced is False

        assert [s.name for s in telemetry_on.get_finished_spans()] == ["predict"]


class TestWindow:
    def test_reports_the_extremes_of_the_last_complete_window(self):
        series = transformation_telemetry._Series()
        window = transformation_telemetry._WINDOW_NS

        series._observe(5, 0)
        series._observe(2, 1)
        series._observe(9, 2)
        assert series._last_window(window - 1) == (0, 0, 0)
        assert series._last_window(window) == (3, 2, 9)

        series._observe(4, window)
        assert series._last_window(window + 1) == (3, 2, 9)
        assert series.calls == 4
        assert series.total_ns == 20

    def test_forgets_a_window_once_another_has_passed_without_calls(self):
        series = transformation_telemetry._Series()
        window = transformation_telemetry._WINDOW_NS

        series._observe(5, 0)
        assert series._last_window(2 * window) == (0, 0, 0)

        series._observe(7, 3 * window)
        assert series._last_window(3 * window + 1) == (0, 5, 5)
