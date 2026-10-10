"""The client: calling an agent, and turning a trace into what a evaluator needs.

Ported from the Stage 1 probe, so the parsing is the same shape that was
measured against a real deployment — but the failure paths are new, and they
are what decides whether a network problem reads as a broken agent.
"""

import pytest
from hopsworks_agents.eval.client import HopsworksAgentClient


class FakeResponse:
    def __init__(self, json_body=None, status_code=200, text=""):
        self._json = json_body or {}
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"status {self.status_code}")


class FakeSession:
    def __init__(self, responses=None, post=None, post_raises=None):
        self._responses = responses or {}
        self._post = post
        self._post_raises = post_raises
        self.posted = []

    def get(self, url, **kwargs):
        for fragment, response in self._responses.items():
            if fragment in url:
                return response
        return FakeResponse(status_code=404)

    def post(self, url, **kwargs):
        self.posted.append({"url": url, **kwargs})
        if self._post_raises:
            raise self._post_raises
        return self._post


def client(session, **kwargs):
    return HopsworksAgentClient(
        session=session,
        api_base="https://cluster",
        project_id=1,
        project_name="proj",
        deployment_id=7,
        agent_url="https://agent",
        **kwargs,
    )


REPLY = {
    "message": {"role": "assistant", "content": [{"type": "text", "text": "4"}]},
    "status": "completed",
    "metadata": {"trace_id": "abc123"},
}


class TestCalling:
    def test_extracts_the_answer_and_the_trace_id(self):
        c = client(FakeSession(post=FakeResponse(REPLY)))
        result = c.call("2+2?", traceparent="00-abc123-s-01", baggage="b", timeout_s=10)
        assert result.text == "4"
        assert result.trace_id == "abc123"
        assert result.error == ""

    def test_sends_correlation_headers(self):
        session = FakeSession(post=FakeResponse(REPLY))
        client(session).call(
            "q",
            traceparent="00-t-s-01",
            baggage="hopsworks.eval.run_id=r",
            timeout_s=10,
        )
        headers = session.posted[0]["headers"]
        assert headers["traceparent"] == "00-t-s-01"
        assert headers["baggage"] == "hopsworks.eval.run_id=r"

    def test_a_network_failure_becomes_an_error_not_an_exception(self):
        # the runner classifies it as INFRA_ERROR and excludes it from the pass
        # rate; an exception here would abort the whole suite instead
        c = client(FakeSession(post_raises=ConnectionError("refused")))
        result = c.call("q", traceparent="t", baggage="b", timeout_s=10)
        assert "refused" in result.error
        assert result.text == ""

    def test_an_http_error_is_reported_not_parsed(self):
        c = client(FakeSession(post=FakeResponse(status_code=500, text="boom")))
        result = c.call("q", traceparent="t", baggage="b", timeout_s=10)
        assert "500" in result.error

    def test_a_failed_status_is_an_error(self):
        client(FakeSession(post=FakeResponse({**REPLY, "status": "failed"})))
        assert (
            client(FakeSession(post=FakeResponse({**REPLY, "status": "failed"})))
            .call("q", traceparent="t", baggage="b", timeout_s=10)
            .error
            != ""
        )


class TestManifest:
    def test_reads_capabilities_and_the_chat_path(self):
        manifest = {
            "capabilities": {"trace_correlation": True},
            "endpoints": {"chat": "/v1/custom-chat"},
        }
        session = FakeSession({".well-known": FakeResponse(manifest)})
        c = client(session)
        assert c.manifest()["capabilities"]["trace_correlation"] is True
        # the path the manifest declares is used, not a hardcoded one
        assert c._chat_path == "/v1/custom-chat"


class TestTrace:
    def _trace(self, spans, attributes):
        return FakeResponse({"spans": spans, "spanAttributes": attributes})

    def test_a_missing_trace_is_none_not_an_error(self):
        # a permanent condition the runner must tolerate: sidecar insert
        # failures are logged and dropped
        assert client(FakeSession()).fetch_trace("nope") is None

    def test_tool_names_come_back_in_call_order(self):
        # a trajectory evaluator asks about sequence, so an arbitrary order would
        # make its verdict arbitrary too
        spans = [
            {"spanId": "root", "parentSpanId": "", "startTimeNs": 0},
            {"spanId": "b", "parentSpanId": "root", "startTimeNs": 200},
            {"spanId": "a", "parentSpanId": "root", "startTimeNs": 100},
        ]
        attrs = [
            {"spanId": "a", "attrKey": "openinference.span.kind", "attrValue": "TOOL"},
            {"spanId": "a", "attrKey": "tool.name", "attrValue": "lookup"},
            {"spanId": "b", "attrKey": "openinference.span.kind", "attrValue": "TOOL"},
            {"spanId": "b", "attrKey": "tool.name", "attrValue": "refund"},
        ]
        session = FakeSession({"/traces/": self._trace(spans, attrs)})
        assert client(session).fetch_trace("t")["tool_names"] == ["lookup", "refund"]

    def test_reports_the_root_span_so_readiness_can_tell_partial_from_complete(self):
        spans = [{"spanId": "root", "parentSpanId": "", "startTimeNs": 0}]
        session = FakeSession({"/traces/": self._trace(spans, [])})
        assert client(session).fetch_trace("t")["root_span_id"] == "root"

    def test_a_trace_with_no_root_span_reports_none(self):
        spans = [{"spanId": "child", "parentSpanId": "missing", "startTimeNs": 0}]
        session = FakeSession({"/traces/": self._trace(spans, [])})
        # the runner reads this as PARTIAL rather than RECEIVED
        assert client(session).fetch_trace("t")["root_span_id"] == ""

    def test_counts_tool_errors(self):
        spans = [
            {"spanId": "root", "parentSpanId": "", "startTimeNs": 0},
            {
                "spanId": "t1",
                "parentSpanId": "root",
                "startTimeNs": 1,
                "statusCode": "STATUS_CODE_ERROR",
            },
        ]
        attrs = [
            {"spanId": "t1", "attrKey": "openinference.span.kind", "attrValue": "TOOL"}
        ]
        session = FakeSession({"/traces/": self._trace(spans, attrs)})
        assert client(session).fetch_trace("t")["tool_error_count"] == 1

    def test_counts_tokens_from_model_and_token_bearing_agent_spans(self):
        spans = [
            {"spanId": "root", "parentSpanId": "", "startTimeNs": 0},
            {"spanId": "chat", "parentSpanId": "root", "startTimeNs": 1},
            {"spanId": "agent", "parentSpanId": "root", "startTimeNs": 2},
            {"spanId": "tool", "parentSpanId": "root", "startTimeNs": 3},
        ]
        attrs = [
            {"spanId": "chat", "attrKey": "gen_ai.operation.name", "attrValue": "chat"},
            {
                "spanId": "chat",
                "attrKey": "gen_ai.usage.input_tokens",
                "attrValue": "10",
            },
            {
                "spanId": "chat",
                "attrKey": "gen_ai.usage.output_tokens",
                "attrValue": "5",
            },
            {
                "spanId": "agent",
                "attrKey": "openinference.span.kind",
                "attrValue": "AGENT",
            },
            {
                "spanId": "agent",
                "attrKey": "gen_ai.usage.prompt_tokens",
                "attrValue": "7",
            },
            {
                "spanId": "agent",
                "attrKey": "gen_ai.usage.completion_tokens",
                "attrValue": "3",
            },
            {
                "spanId": "tool",
                "attrKey": "openinference.span.kind",
                "attrValue": "TOOL",
            },
            {
                "spanId": "tool",
                "attrKey": "gen_ai.usage.input_tokens",
                "attrValue": "999",
            },
        ]

        trace = client(
            FakeSession({"/traces/": self._trace(spans, attrs)})
        ).fetch_trace("t")
        assert trace["input_tokens"] == 17
        assert trace["output_tokens"] == 8

    def test_an_empty_trace_is_none(self):
        session = FakeSession({"/traces/": FakeResponse({"spans": []})})
        assert client(session).fetch_trace("t") is None


class TestWhereTheAgentIs:
    """The address trials are sent to.

    Assuming a /v1/{project}/{name} prefix produced a 404 that read as a missing
    agent; the manifest and every endpoint are served at the root of the
    deployment's own service.
    """

    def _client(self, deployment: dict):
        from hopsworks_agents.eval.client import HopsworksAgentClient

        class Session:
            def get(self, url, **kwargs):
                class R:
                    @staticmethod
                    def json():
                        return deployment

                    status_code = 200

                    @staticmethod
                    def raise_for_status():
                        return None

                return R()

        return HopsworksAgentClient(
            session=Session(),
            api_base="https://h",
            project_id=1,
            project_name="g1",
            deployment_id=7,
        )

    def test_it_is_the_deployments_own_service(self):
        client = self._client({"name": "customeragent", "projectNamespace": "g1"})
        assert client._base_url() == "http://customeragent.g1.svc.cluster.local"

    def test_there_is_no_project_and_name_path_prefix(self):
        # what the 404 was: the agent serves its manifest at the root
        client = self._client({"name": "customeragent", "projectNamespace": "g1"})
        assert "/v1/g1/customeragent" not in client._base_url()

    def test_the_namespace_is_not_assumed_to_be_the_project_name(self):
        client = self._client({"name": "a", "projectNamespace": "ns-17"})
        assert client._base_url() == "http://a.ns-17.svc.cluster.local"

    def test_it_falls_back_to_the_project_name_when_no_namespace_is_given(self):
        client = self._client({"name": "a"})
        assert client._base_url() == "http://a.g1.svc.cluster.local"

    def test_a_deployment_with_no_name_says_so(self):
        # rather than building an address with a hole in it and failing at the request

        with pytest.raises(RuntimeError, match="no name"):
            self._client({})._base_url()

    def test_an_explicit_url_wins(self):
        from hopsworks_agents.eval.client import HopsworksAgentClient

        client = HopsworksAgentClient(
            session=None,
            api_base="https://h",
            project_id=1,
            project_name="g1",
            deployment_id=7,
            agent_url="https://elsewhere/agent/",
        )
        assert client._base_url() == "https://elsewhere/agent"
