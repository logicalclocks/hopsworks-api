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
from hopsworks_common import request_metrics, tracing


prometheus_client = pytest.importorskip("prometheus_client")


@pytest.fixture
def fresh(mocker):
    mocker.patch.object(
        request_metrics,
        "_stages",
        {stage: request_metrics._Series() for stage in request_metrics.STAGES},
    )
    buckets = {
        stage: [0] * (len(request_metrics._BUCKETS_SECONDS) + 1)
        for stage in request_metrics.STAGES
    }
    mocker.patch.object(request_metrics, "_buckets", buckets)
    mocker.patch.object(tracing, "_request_observers", [request_metrics._observe])
    return buckets


def _metrics():
    registry = prometheus_client.CollectorRegistry()
    registry.register(request_metrics._Collector())
    return registry


def test_every_stage_is_exported_at_zero_before_any_request(fresh):
    metrics = _metrics()

    for stage in request_metrics.STAGES:
        assert (
            metrics.get_sample_value(
                "hopsworks_prediction_stage_seconds_count", {"stage": stage}
            )
            == 0
        )
    assert (
        metrics.get_sample_value(
            "hopsworks_prediction_stage_window_min_seconds",
            {"stage": "feature_lookup"},
        )
        is None
    )


def test_a_request_adds_each_stage_it_ran_once(fresh, span_exporter):
    with tracing._request_scope(1):
        with tracing._stage("feature_lookup"):
            tracing._add_stage_ns("transformations", 2_000)
        with tracing._stage("model_predict"):
            pass

    metrics = _metrics()

    def count(stage):
        return metrics.get_sample_value(
            "hopsworks_prediction_stage_seconds_count", {"stage": stage}
        )

    assert count("feature_lookup") == 1
    assert count("transformations") == 1
    assert count("model_predict") == 1
    assert count("feature_log") == 0
    assert metrics.get_sample_value(
        "hopsworks_prediction_stage_seconds_sum", {"stage": "transformations"}
    ) == pytest.approx(2e-6)
    assert (
        metrics.get_sample_value(
            "hopsworks_prediction_stage_window_max_seconds",
            {"stage": "transformations"},
        )
        == 2e-6
    )


def test_a_stage_outside_a_request_is_not_counted(fresh, span_exporter):
    with tracing._stage("feature_lookup"):
        pass

    assert (
        _metrics().get_sample_value(
            "hopsworks_prediction_stage_seconds_count", {"stage": "feature_lookup"}
        )
        == 0
    )


def test_a_stage_times_nothing_while_tracing_is_off(fresh, mocker):
    mocker.patch.object(tracing, "_enabled", False)
    clock = mocker.spy(tracing.time, "perf_counter_ns")

    with tracing._request_scope(1), tracing._stage("feature_lookup"):
        pass

    clock.assert_not_called()


def test_a_stage_of_a_sampled_request_is_a_span(fresh, span_exporter):
    trace = pytest.importorskip("opentelemetry.trace")

    with (
        trace.get_tracer("test").start_as_current_span("predict"),
        tracing._request_scope(1),
        tracing._stage("model_predict", {"hopsworks.rows": 3}),
    ):
        pass

    spans = {span.name: span for span in span_exporter.get_finished_spans()}
    assert spans["model_predict"].attributes["hopsworks.rows"] == 3


def test_a_duration_lands_in_the_first_bucket_at_or_above_it(fresh):
    request_metrics._observe({"model_predict": 1_000_000, "feature_log": 20 * 10**9})

    bounds = request_metrics._BUCKETS_SECONDS
    assert fresh["model_predict"][bounds.index(0.001)] == 1
    assert fresh["feature_log"][-1] == 1
    assert bounds[0] == 1e-05 and bounds[-1] == 10.0
