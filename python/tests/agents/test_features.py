"""Featurization and, more importantly, completeness.

The arithmetic here is easy to get right and easy to check. The part that
silently produces wrong answers is *when* a trace is considered done: featurize
too early and a trajectory evaluator reads half a trajectory and marks a correct
agent wrong, with nothing in the output to say the trace was incomplete.
"""

import json
from datetime import datetime, timedelta, timezone

import pytest
from hopsworks_agents.eval import select_ready_traces, trace_features


NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)
SECOND_NS = 1_000_000_000


def span(
    span_id="s1",
    parent="",
    *,
    trace_id="t1",
    start_ns=0,
    end_ns=SECOND_NS,
    ingested_seconds_ago=600,
    status="",
    messages=None,
    **extra,
):
    return {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent,
        "deployment_id": 7,
        "name": span_id,
        "start_time_ns": start_ns,
        "end_time_ns": end_ns,
        "status_code": status,
        "status_message": "",
        "session_id": "sess-1",
        "user_id": "",
        "messages": json.dumps(messages) if messages is not None else "",
        "created_at": NOW - timedelta(seconds=ingested_seconds_ago),
        **extra,
    }


def attr(span_id, key, value):
    return {"span_id": span_id, "attr_key": key, "attr_value": value}


class TestCompleteness:
    def test_quiet_trace_with_a_root_is_ready(self):
        result = select_ready_traces([span()], now=NOW, grace=timedelta(minutes=3))
        assert result.ready == ["t1"]

    def test_a_trace_still_receiving_spans_is_not_ready(self):
        # the root arrived, but a child landed seconds ago: featurizing now
        # would describe a trajectory that is still being written
        spans = [
            span("root", ingested_seconds_ago=600),
            span("child", parent="root", ingested_seconds_ago=5),
        ]
        result = select_ready_traces(spans, now=NOW, grace=timedelta(minutes=3))
        assert result.ready == []
        assert result.pending == ["t1"]

    def test_a_root_span_alone_does_not_mean_finished(self):
        # the failure this whole design guards against: the root span is not a
        # completion signal, it is just another span that happened to arrive
        result = select_ready_traces(
            [span("root", ingested_seconds_ago=1)], now=NOW, grace=timedelta(minutes=3)
        )
        assert result.ready == []

    def test_a_trace_whose_root_never_arrives_is_flagged_not_dropped(self):
        # an agent that crashed before exporting its root span is exactly the
        # trace worth keeping; dropping it hides the failures
        spans = [span("child", parent="missing", ingested_seconds_ago=3600)]
        result = select_ready_traces(
            spans,
            now=NOW,
            grace=timedelta(minutes=3),
            root_timeout=timedelta(minutes=30),
        )
        assert result.partial == ["t1"]
        assert result.ready == []

    def test_a_rootless_trace_still_within_the_timeout_waits(self):
        spans = [span("child", parent="missing", ingested_seconds_ago=60)]
        result = select_ready_traces(
            spans,
            now=NOW,
            grace=timedelta(minutes=3),
            root_timeout=timedelta(minutes=30),
        )
        assert result.pending == ["t1"]

    def test_ingestion_time_drives_the_decision_not_span_start(self):
        # a span whose work happened long ago but which the sidecar only just
        # inserted must keep the trace open -- the watermark is on ingestion
        # time precisely so queueing delays cannot skip spans
        spans = [
            span("root", start_ns=0, ingested_seconds_ago=600),
            span("late", parent="root", start_ns=0, ingested_seconds_ago=2),
        ]
        assert select_ready_traces(
            spans, now=NOW, grace=timedelta(minutes=3)
        ).pending == ["t1"]

    def test_spans_without_an_ingestion_time_are_not_guessed_at(self):
        naive = span()
        del naive["created_at"]
        result = select_ready_traces([naive], now=NOW)
        assert result.pending == ["t1"]

    def test_traces_are_judged_independently(self):
        spans = [
            span("a", trace_id="quiet", ingested_seconds_ago=600),
            span("b", trace_id="busy", ingested_seconds_ago=1),
        ]
        result = select_ready_traces(spans, now=NOW, grace=timedelta(minutes=3))
        assert result.ready == ["quiet"]
        assert result.pending == ["busy"]


class TestTraceFeatures:
    def test_final_output_prefers_the_root_span_attribute(self):
        # what the SDK stamps from the response object -- authoritative, unlike
        # anything reconstructed from child spans
        spans = [span("root", messages=[{"role": "assistant", "content": "guessed"}])]
        row = trace_features(spans, [attr("root", "output.value", "authoritative")])
        assert row["final_output"] == "authoritative"

    def test_final_output_falls_back_to_the_last_assistant_message(self):
        spans = [
            span("root", messages=[{"role": "user", "content": "q"}]),
            span(
                "child",
                parent="root",
                start_ns=SECOND_NS,
                messages=[{"role": "assistant", "content": "answer"}],
            ),
        ]
        assert trace_features(spans)["final_output"] == "answer"

    def test_final_output_is_empty_rather_than_invented(self):
        assert trace_features([span("root")])["final_output"] == ""

    def test_counts_tools_and_their_errors(self):
        spans = [
            span("root"),
            span("t1s", parent="root", start_ns=1),
            span("t2s", parent="root", start_ns=2, status="STATUS_CODE_ERROR"),
        ]
        attrs = [
            attr("t1s", "openinference.span.kind", "TOOL"),
            attr("t1s", "tool.name", "search"),
            attr("t2s", "openinference.span.kind", "TOOL"),
            attr("t2s", "tool.name", "refund"),
        ]
        row = trace_features(spans, attrs)
        assert row["tool_call_count"] == 2
        assert row["tool_error_count"] == 1
        assert json.loads(row["tool_names"]) == ["search", "refund"]

    def test_sums_tokens_across_model_and_token_bearing_agent_spans(self):
        spans = [
            span("root"),
            span("llm1", parent="root", start_ns=1),
            span("llm2", parent="root", start_ns=2),
            span("agent", parent="root", start_ns=3),
            span("chat", parent="root", start_ns=4),
            span("tool", parent="root", start_ns=5),
        ]
        attrs = [
            attr("llm1", "openinference.span.kind", "LLM"),
            attr("llm1", "gen_ai.usage.input_tokens", "10"),
            attr("llm1", "gen_ai.usage.output_tokens", "5"),
            attr("llm2", "openinference.span.kind", "LLM"),
            attr("llm2", "llm.token_count.prompt", "7"),
            attr("llm2", "llm.token_count.completion", "3"),
            attr("agent", "openinference.span.kind", "AGENT"),
            attr("agent", "gen_ai.usage.prompt_tokens", "6"),
            attr("agent", "gen_ai.usage.completion_tokens", "2"),
            attr("chat", "gen_ai.operation.name", "chat"),
            attr("chat", "llm.token_count.prompt", "5"),
            attr("chat", "llm.token_count.completion", "1"),
            attr("tool", "openinference.span.kind", "TOOL"),
            attr("tool", "gen_ai.usage.input_tokens", "999"),
        ]
        row = trace_features(spans, attrs)
        assert row["input_tokens"] == 28
        assert row["output_tokens"] == 11

    def test_eval_traffic_is_marked(self):
        # the separation every downstream query depends on
        row = trace_features(
            [span("root")], [attr("root", "hopsworks.eval.run_id", "run_7")]
        )
        assert row["is_eval"] is True
        assert row["eval_run_id"] == "run_7"

    def test_production_traffic_is_not_marked(self):
        row = trace_features([span("root")])
        assert row["is_eval"] is False
        assert row["eval_run_id"] == ""

    def test_attributes_are_found_on_child_spans(self):
        # the model name lives on the LLM span, never on the root
        spans = [span("root"), span("llm", parent="root", start_ns=1)]
        attrs = [
            attr("llm", "openinference.span.kind", "LLM"),
            attr("llm", "gen_ai.request.model", "claude-opus-5"),
        ]
        assert trace_features(spans, attrs)["model_name"] == "claude-opus-5"

    def test_cost_is_null_when_no_price_is_known(self):
        # zero would claim the trace was free and average into cost dashboards
        # as though that were true
        spans = [span("root"), span("llm", parent="root", start_ns=1)]
        attrs = [
            attr("llm", "openinference.span.kind", "LLM"),
            attr("llm", "gen_ai.usage.input_tokens", "1000"),
        ]
        assert trace_features(spans, attrs)["estimated_cost"] is None

    def test_cost_is_computed_when_prices_are_given(self):
        spans = [span("root"), span("llm", parent="root", start_ns=1)]
        attrs = [
            attr("llm", "openinference.span.kind", "LLM"),
            attr("llm", "gen_ai.usage.input_tokens", "1000000"),
            attr("llm", "gen_ai.usage.output_tokens", "500000"),
        ]
        row = trace_features(
            spans,
            attrs,
            input_token_price_per_million=3.0,
            output_token_price_per_million=15.0,
        )
        assert row["estimated_cost"] == pytest.approx(3.0 + 7.5)

    def test_latency_spans_the_whole_trace(self):
        spans = [
            span("root", start_ns=0, end_ns=5 * SECOND_NS),
            span("child", parent="root", start_ns=SECOND_NS, end_ns=9 * SECOND_NS),
        ]
        # the child outlived the root: latency must cover it, not stop at the
        # root's end
        assert trace_features(spans)["latency_ms"] == pytest.approx(9000.0)

    def test_partial_traces_say_so(self):
        row = trace_features([span("child", parent="missing")], is_partial=True)
        assert row["trace_status"] == "PARTIAL"
        assert row["root_span_id"] == ""

    def test_counts_guardrail_events(self):
        events = [
            {"name": "hopsworks.guardrail.trigger", "span_id": "root"},
            {"name": "something.else", "span_id": "root"},
        ]
        row = trace_features([span("root")], [], events)
        assert row["guardrail_trigger_count"] == 1

    def test_empty_input_is_rejected_rather_than_producing_a_blank_row(self):
        with pytest.raises(ValueError):
            trace_features([])
