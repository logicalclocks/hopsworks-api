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

import asyncio

import pytest
from hopsworks_common import request_metrics, tracing
from hsfs import feature_view


prometheus_client = pytest.importorskip("prometheus_client")

LOOKUP = "hopsworks_prediction_stage_seconds_count"


@pytest.fixture
def fresh(mocker):
    stages = {}
    buckets = {}
    mocker.patch.object(request_metrics, "_stages", stages)
    mocker.patch.object(request_metrics, "_buckets", buckets)
    mocker.patch.object(tracing, "_request_observers", [request_metrics._observe])
    return buckets


def _metrics():
    registry = prometheus_client.CollectorRegistry()
    registry.register(request_metrics._Collector())
    return registry


def _count(metrics, stage, name):
    return metrics.get_sample_value(LOOKUP, {"stage": stage, "name": name})


class _View:
    """Stands in for a FeatureView: the decorator reads only name and version."""

    def __init__(self, name):
        self.name = name
        self.version = 1

    @feature_view._timed_stage("feature_lookup")
    def get_feature_vector(self):
        return self.name

    @feature_view._timed_stage("feature_lookup")
    async def get_feature_vectors_async(self):
        return [self.name]

    @feature_view._timed_stage("feature_log")
    def log(self):
        return None


def test_a_declared_stage_is_exported_at_zero_before_any_request(fresh):
    request_metrics._declare("feature_lookup", "fraud:1")

    metrics = _metrics()

    assert _count(metrics, "feature_lookup", "fraud:1") == 0
    assert (
        metrics.get_sample_value(
            "hopsworks_prediction_stage_window_min_seconds",
            {"stage": "feature_lookup", "name": "fraud:1"},
        )
        is None
    )


def test_each_feature_view_and_model_of_a_request_is_its_own_series(
    fresh, span_exporter
):
    fraud, risk = _View("fraud"), _View("risk")
    with tracing._request_scope(1):
        fraud.get_feature_vector()
        asyncio.run(risk.get_feature_vectors_async())
        tracing._add_stage_ns("transformations", "fraud:1", 2_000)
        with tracing._stage("model_predict", "fraud_model:3"):
            pass
        with tracing._stage("model_predict", "risk_model:1"):
            pass
        risk.log()

    metrics = _metrics()

    for stage, name in [
        ("feature_lookup", "fraud:1"),
        ("feature_lookup", "risk:1"),
        ("transformations", "fraud:1"),
        ("model_predict", "fraud_model:3"),
        ("model_predict", "risk_model:1"),
        ("feature_log", "risk:1"),
    ]:
        assert _count(metrics, stage, name) == 1, (stage, name)
    assert metrics.get_sample_value(
        "hopsworks_prediction_stage_window_max_seconds",
        {"stage": "transformations", "name": "fraud:1"},
    ) == pytest.approx(2e-6)


def test_a_feature_view_lookup_inside_the_predictors_lookup_is_counted_once(
    fresh, span_exporter
):
    fraud = _View("fraud")
    with (
        tracing._request_scope(1),
        tracing._stage("feature_lookup", "fraud:1"),
    ):
        fraud.get_feature_vector()

    assert _count(_metrics(), "feature_lookup", "fraud:1") == 1


def test_a_feature_view_call_outside_a_request_is_not_timed(fresh, span_exporter):
    _View("fraud").log()

    assert _count(_metrics(), "feature_log", "fraud:1") is None
    assert span_exporter.get_finished_spans() == ()


def test_a_stage_times_nothing_while_tracing_is_off(fresh, mocker):
    mocker.patch.object(tracing, "_enabled", False)
    clock = mocker.spy(tracing.time, "perf_counter_ns")

    with tracing._request_scope(1):
        _View("fraud").get_feature_vector()
        with tracing._stage("feature_lookup", "fraud:1"):
            pass

    clock.assert_not_called()


def test_a_stage_of_a_sampled_request_is_a_span(fresh, span_exporter):
    trace = pytest.importorskip("opentelemetry.trace")

    with (
        trace.get_tracer("test").start_as_current_span("predict"),
        tracing._request_scope(1),
        tracing._stage("model_predict", "fraud_model:3", {"hopsworks.rows": 3}),
    ):
        pass

    spans = {span.name: span for span in span_exporter.get_finished_spans()}
    assert spans["model_predict"].attributes["hopsworks.rows"] == 3


def test_a_duration_lands_in_the_first_bucket_at_or_above_it(fresh):
    request_metrics._observe(
        {("model_predict", "m:1"): 1_000_000, ("feature_log", "f:1"): 20 * 10**9}
    )

    bounds = request_metrics._BUCKETS_SECONDS
    assert fresh[("model_predict", "m:1")][bounds.index(0.001)] == 1
    assert fresh[("feature_log", "f:1")][-1] == 1
    assert bounds[0] == 1e-05 and bounds[-1] == 10.0
