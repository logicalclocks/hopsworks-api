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
"""Timing of the transformation functions a feature view runs in a deployment with tracing enabled.

A vector server builds a [`_Telemetry`][] only when [`hopsworks_common.tracing`][] is enabled; otherwise it keeps `None` and its transformation path is the plain one.
With telemetry on, every on-demand and model-dependent transformation, and each of the two phases, keeps a fixed-size aggregate:

- the cumulative call count and duration, exported as Prometheus counters, so an average over any interval and across replicas is `rate(seconds) / rate(calls)`;
- the minimum and maximum duration over the last complete window of `_WINDOW_SECONDS`, exported as gauges.

Spans are opened only for sampled requests, and only for the first row of each.
Each feature view's transformation DAG is exported as one gauge series per edge.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from hopsworks_common import tracing


if TYPE_CHECKING:
    from collections.abc import Iterator

    from hsfs import transformation_function
    from hsfs.core.transformation_execution_dag import TransformationExecutionDAG
    from typing_extensions import Self


_logger = logging.getLogger(__name__)

KIND_ON_DEMAND = "on_demand"
KIND_MODEL_DEPENDENT = "model_dependent"
ROW_SPAN_NAME = "transformations"
SPAN_KIND_ATTRIBUTE = "hopsworks.span.kind"

_WINDOW_SECONDS = 60
_WINDOW_NS = _WINDOW_SECONDS * 1_000_000_000

_PHASE_LABELS = ["feature_view", "feature_view_version", "kind"]
_FUNCTION_LABELS = [*_PHASE_LABELS, "transformation", "outputs"]


class _Series:
    """Running count and duration of one transformation or phase, plus its extremes over fixed windows.

    Updates are not locked: under concurrent requests a window can miss an extreme, and a rotation racing an update can drop one call from a window.
    """

    __slots__ = (
        "calls",
        "errors",
        "total_ns",
        "_window_end_ns",
        "_window_calls",
        "_window_min_ns",
        "_window_max_ns",
        "_last_calls",
        "_last_min_ns",
        "_last_max_ns",
    )

    def __init__(self):
        self.calls = 0
        self.errors = 0
        self.total_ns = 0
        self._window_end_ns = 0
        self._window_calls = 0
        self._window_min_ns = 0
        self._window_max_ns = 0
        self._last_calls = 0
        self._last_min_ns = 0
        self._last_max_ns = 0

    def _observe(self, duration_ns: int, now_ns: int) -> None:
        if now_ns >= self._window_end_ns:
            self._rotate(now_ns)
        self.calls += 1
        self.total_ns += duration_ns
        if self._window_calls == 0 or duration_ns < self._window_min_ns:
            self._window_min_ns = duration_ns
        if duration_ns > self._window_max_ns:
            self._window_max_ns = duration_ns
        self._window_calls += 1

    def _rotate(self, now_ns: int) -> None:
        # A window that ended more than a window ago saw no calls in the window after it, so it is not the last complete one.
        fresh = now_ns < self._window_end_ns + _WINDOW_NS
        self._last_calls = self._window_calls if fresh else 0
        self._last_min_ns = self._window_min_ns
        self._last_max_ns = self._window_max_ns
        self._window_calls = 0
        self._window_min_ns = 0
        self._window_max_ns = 0
        self._window_end_ns = now_ns + _WINDOW_NS

    def _last_window(self, now_ns: int) -> tuple[int, int, int]:
        """Calls, minimum and maximum of the last complete window as of `now_ns`, without rotating."""
        if now_ns >= self._window_end_ns + _WINDOW_NS:
            return 0, 0, 0
        if now_ns >= self._window_end_ns:
            return self._window_calls, self._window_min_ns, self._window_max_ns
        return self._last_calls, self._last_min_ns, self._last_max_ns


def _node_id(tf: transformation_function.TransformationFunction) -> str:
    # Unique within a DAG, which rejects two functions producing the same column; the function name is not, since one UDF is often attached to several features.
    return ",".join(tf.hopsworks_udf.output_column_names)


class _Telemetry:
    """The transformation timings of one feature view version served by a vector server."""

    def __init__(
        self,
        feature_view_name: str,
        feature_view_version: int | str,
        execution_graphs: dict[str, TransformationExecutionDAG],
    ):
        self.feature_view = feature_view_name
        self.feature_view_version = str(feature_view_version)
        self.phases = {kind: _Series() for kind in execution_graphs}
        # Keyed by id(tf): the DAG holds the transformation functions for the life of the vector server.
        self.functions: dict[int, tuple[str, Any, _Series, dict[str, Any]]] = {}
        self.edges: list[tuple[str, str, str, str]] = []
        for kind, graph in execution_graphs.items():
            for tf in graph.nodes:
                predecessors = graph.dependencies.get(id(tf)) or []
                udf = tf.hopsworks_udf
                attributes = {
                    SPAN_KIND_ATTRIBUTE: "TRANSFORMATION",
                    "hopsworks.transformation.kind": kind,
                    "hopsworks.transformation.function": udf.function_name,
                    "hopsworks.transformation.inputs": ", ".join(
                        udf.transformation_features
                    ),
                    "hopsworks.transformation.outputs": ", ".join(
                        udf.output_column_names
                    ),
                    # Joined with " | " because every entry is itself a comma-separated output list.
                    "hopsworks.transformation.depends_on": " | ".join(
                        _node_id(p) for p in predecessors
                    ),
                }
                self.functions[id(tf)] = (kind, tf, _Series(), attributes)
                for predecessor in predecessors or [None]:
                    self.edges.append(
                        (
                            kind,
                            udf.function_name,
                            _node_id(tf),
                            "" if predecessor is None else _node_id(predecessor),
                        )
                    )
        _register(self)

    def _row(self, *execution_graphs: TransformationExecutionDAG | None):
        """A context manager opening the span that groups the transformation phases of one feature vector, when this row of a sampled request is traced.

        `execution_graphs` are the DAGs the phases inside will run; with none that has a function, no span is opened.
        It yields whether the phases of this row are traced.
        """
        if not any(graph is not None and graph.nodes for graph in execution_graphs):
            return _UNTRACED_ROW
        if not tracing._take_row():
            return _UNTRACED_ROW
        return self._traced_row()

    @contextlib.contextmanager
    def _traced_row(self) -> Iterator[bool]:
        with tracing._child_span(
            ROW_SPAN_NAME,
            {
                SPAN_KIND_ATTRIBUTE: "TRANSFORMATIONS",
                "hopsworks.feature_view.name": self.feature_view,
                "hopsworks.feature_view.version": self.feature_view_version,
            },
        ):
            yield True

    def _phase(
        self,
        kind: str,
        execution_graph: TransformationExecutionDAG | None,
        traced: bool,
    ):
        """A context manager timing one phase of one feature vector, yielding the observer the DAG executor reports each function to."""
        if execution_graph is None or not execution_graph.nodes:
            return tracing._NO_SPAN
        return _Phase(self, kind, execution_graph, traced)


class _Phase:
    """One on-demand or model-dependent phase of one feature vector: times itself and the functions the DAG executor reports."""

    __slots__ = ("_telemetry", "_kind", "_graph", "_traced", "_span", "_start_ns")

    def __init__(self, telemetry: _Telemetry, kind: str, graph, traced: bool):
        self._telemetry = telemetry
        self._kind = kind
        self._graph = graph
        self._traced = traced
        self._span = None

    def __enter__(self) -> Self:
        if self._traced:
            self._span = tracing._child_span(
                f"{self._kind}_transformations",
                {
                    SPAN_KIND_ATTRIBUTE: "TRANSFORMATION_PHASE",
                    "hopsworks.transformation.kind": self._kind,
                    "hopsworks.transformation.count": len(self._graph.nodes),
                },
            )
            self._span.__enter__()
        self._start_ns = time.perf_counter_ns()
        return self

    def __exit__(self, exc_type, exc, tb):
        end_ns = time.perf_counter_ns()
        self._telemetry.phases[self._kind]._observe(end_ns - self._start_ns, end_ns)
        if self._span is not None:
            return self._span.__exit__(exc_type, exc, tb)
        return False

    def _start(self) -> int:
        """Mark the start of a transformation function call, in monotonic nanoseconds."""
        return time.perf_counter_ns()

    def _finish(
        self,
        tf: transformation_function.TransformationFunction,
        start_ns: int,
        error: BaseException | None = None,
    ) -> None:
        """Record a transformation function call that began at `start_ns`, from [`_start`][]."""
        end_ns = time.perf_counter_ns()
        entry = self._telemetry.functions.get(id(tf))
        if entry is None:
            return
        series = entry[2]
        series._observe(end_ns - start_ns, end_ns)
        if error is not None:
            series.errors += 1
        if self._traced:
            # The wall clock is read only for a traced call, and its start derived from the monotonic duration.
            end_wall_ns = time.time_ns()
            tracing._record_span(
                tf.hopsworks_udf.function_name,
                end_wall_ns - (end_ns - start_ns),
                end_wall_ns,
                entry[3],
                error,
            )


# Shared by every untraced row, so measuring a row allocates no context manager.
_UNTRACED_ROW = contextlib.nullcontext(False)

_telemetries: dict[tuple[str, str], _Telemetry] = {}
_collector_lock = threading.Lock()
_collector_registered = False


def _register(telemetry: _Telemetry) -> None:
    global _collector_registered
    # A vector server re-initialised for the same feature view replaces the previous one's series.
    _telemetries[(telemetry.feature_view, telemetry.feature_view_version)] = telemetry
    with _collector_lock:
        if _collector_registered:
            return
        _collector_registered = True
        try:
            from prometheus_client import REGISTRY
        except ImportError:
            return
        try:
            REGISTRY.register(_Collector())
        except ValueError as error:
            # The default registry already holds these names, so this module was loaded a second time.
            _logger.warning("Transformation metrics not exposed: %s", error)


class _Collector:
    """Publishes every served feature view's transformation series when Prometheus scrapes the predictor."""

    def collect(self):
        from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

        now_ns = time.perf_counter_ns()
        phase_calls = CounterMetricFamily(
            "hopsworks_transformations_calls",
            "Feature vectors whose on-demand or model-dependent transformations ran",
            labels=_PHASE_LABELS,
        )
        phase_seconds = CounterMetricFamily(
            "hopsworks_transformations_seconds",
            "Time spent applying all on-demand or all model-dependent transformations to feature vectors",
            labels=_PHASE_LABELS,
        )
        phase_min = GaugeMetricFamily(
            "hopsworks_transformations_window_min_seconds",
            f"Fastest feature vector's transformations in the last complete {_WINDOW_SECONDS} s window",
            labels=_PHASE_LABELS,
        )
        phase_max = GaugeMetricFamily(
            "hopsworks_transformations_window_max_seconds",
            f"Slowest feature vector's transformations in the last complete {_WINDOW_SECONDS} s window",
            labels=_PHASE_LABELS,
        )
        function_calls = CounterMetricFamily(
            "hopsworks_transformation_function_calls",
            "Calls of one transformation function, one per feature vector",
            labels=_FUNCTION_LABELS,
        )
        function_seconds = CounterMetricFamily(
            "hopsworks_transformation_function_seconds",
            "Time spent in one transformation function",
            labels=_FUNCTION_LABELS,
        )
        function_errors = CounterMetricFamily(
            "hopsworks_transformation_function_errors",
            "Calls of one transformation function that raised",
            labels=_FUNCTION_LABELS,
        )
        function_min = GaugeMetricFamily(
            "hopsworks_transformation_function_window_min_seconds",
            f"Fastest call of one transformation function in the last complete {_WINDOW_SECONDS} s window",
            labels=_FUNCTION_LABELS,
        )
        function_max = GaugeMetricFamily(
            "hopsworks_transformation_function_window_max_seconds",
            f"Slowest call of one transformation function in the last complete {_WINDOW_SECONDS} s window",
            labels=_FUNCTION_LABELS,
        )
        dag_edge = GaugeMetricFamily(
            "hopsworks_transformation_dag_edge",
            "An edge of a feature view's transformation DAG; a function fed only by features has one series with an empty depends_on",
            labels=[*_FUNCTION_LABELS, "depends_on"],
        )
        for telemetry in list(_telemetries.values()):
            view = [telemetry.feature_view, telemetry.feature_view_version]
            for kind, series in telemetry.phases.items():
                labels = [*view, kind]
                phase_calls.add_metric(labels, series.calls)
                phase_seconds.add_metric(labels, series.total_ns / 1e9)
                calls, minimum, maximum = series._last_window(now_ns)
                if calls:
                    phase_min.add_metric(labels, minimum / 1e9)
                    phase_max.add_metric(labels, maximum / 1e9)
            for kind, tf, series, _attributes in telemetry.functions.values():
                labels = [*view, kind, tf.hopsworks_udf.function_name, _node_id(tf)]
                function_calls.add_metric(labels, series.calls)
                function_seconds.add_metric(labels, series.total_ns / 1e9)
                function_errors.add_metric(labels, series.errors)
                calls, minimum, maximum = series._last_window(now_ns)
                if calls:
                    function_min.add_metric(labels, minimum / 1e9)
                    function_max.add_metric(labels, maximum / 1e9)
            for kind, name, outputs, depends_on in telemetry.edges:
                dag_edge.add_metric([*view, kind, name, outputs, depends_on], 1)
        yield from (
            phase_calls,
            phase_seconds,
            phase_min,
            phase_max,
            function_calls,
            function_seconds,
            function_errors,
            function_min,
            function_max,
            dag_edge,
        )
