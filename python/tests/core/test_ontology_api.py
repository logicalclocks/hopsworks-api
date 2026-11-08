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

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, Mock

import pytest
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import ontology_api


def _response(status: int, body=None, lines=None):
    response = MagicMock()
    response.status_code = status
    response.reason = "reason"
    response.content = json.dumps(body).encode() if body is not None else b""
    response.json.return_value = body
    response.iter_lines.return_value = lines or []
    response.__enter__.return_value = response
    return response


def _error(
    status: int, body=None, url="https://h/hopsworks-api/pythonapp/demo/sales/api/x"
):
    return RestAPIError(url, _response(status, body if body is not None else {}))


@pytest.fixture
def hopsworks_client(mocker):
    ontology_api._tokens.clear()
    fake = Mock()
    fake._project_id = 119
    fake._project_name = "demo project"
    fake._base_url = "https://hopsworks.example:443/"
    fake._verify = "/tmp/ca.pem"
    fake._send_request.return_value = {
        "token": "minted",
        "expiresAt": int(time.time() * 1000) + 600_000,
    }
    mocker.patch(
        "hopsworks_common.core.ontology_api.client._get_instance", return_value=fake
    )
    yield fake
    ontology_api._tokens.clear()


class TestEvents:
    def test_reads_data_lines_up_to_done(self):
        lines = [
            ": ping",
            "",
            'data: {"type": "step", "label": "Intent"}',
            "data: not json",
            "data: [1, 2]",
            'data:{"type": "result", "answer": {}}',
            "data: [DONE]",
            'data: {"type": "step", "label": "after the end"}',
        ]

        events = list(ontology_api._events(lines))

        assert events == [
            {"type": "step", "label": "Intent"},
            {"type": "result", "answer": {}},
        ]


class TestAnswer:
    def test_folds_a_run_into_the_answer_its_sql_and_rows(self):
        rows = [{"state": "Georgia", "customers": 7}]
        events = [
            {"type": "step", "label": "Intent", "thought": "a count"},
            {"type": "sql", "sql": "SELECT 1"},
            {
                "type": "result",
                "answer": {
                    "response": "Georgia has the most customers.",
                    "sql_code": "SELECT s.name ...",
                    "sql_response_from_db": [json.dumps(rows)],
                },
            },
            {"type": "charts", "content": "a bar chart"},
        ]

        answer = ontology_api._answer(events)

        assert answer == {
            "answer": "Georgia has the most customers.",
            "sql": "SELECT s.name ...",
            "rows": rows,
            "row_count": 1,
            "truncated": False,
            "reasoning": "Intent: a count",
            "chart": "a bar chart",
        }

    def test_caps_the_rows_and_says_so(self):
        rows = [{"n": i} for i in range(ontology_api.MAX_ROWS + 5)]
        answer = ontology_api._answer(
            [{"type": "result", "answer": {"sql_response_from_db": rows}}]
        )

        assert len(answer["rows"]) == ontology_api.MAX_ROWS
        assert answer["row_count"] == ontology_api.MAX_ROWS + 5
        assert answer["truncated"] is True

    def test_falls_back_to_the_streamed_sql(self):
        answer = ontology_api._answer(
            [
                {"type": "sql", "sql": "SELECT 2"},
                {"type": "result", "answer": {"response": "two"}},
            ]
        )

        assert answer["sql"] == "SELECT 2"
        assert answer["rows"] == []

    def test_an_error_event_fails_the_run(self):
        with pytest.raises(RuntimeError, match="no source"):
            ontology_api._answer([{"type": "error", "message": "no source"}])

    def test_a_run_without_a_result_fails(self):
        with pytest.raises(RuntimeError, match="without an answer"):
            ontology_api._answer([{"type": "step", "label": "Intent"}])


class TestReadiness:
    def test_ready_with_everything_in_place(self):
        verdict = ontology_api._summarise(
            {"calculated": True}, {"count": 1}, {"data": [{"name": "demo"}]}
        )

        assert verdict["ready"] is True
        assert verdict["databases"] == ["demo"]
        assert verdict["blockers"] == verdict["unverified"] == []

    def test_an_unreadable_source_list_is_unverified_not_a_blocker(self):
        verdict = ontology_api._summarise(
            {"calculated": True}, ontology_api._UNVERIFIED, {"data": [{"name": "demo"}]}
        )

        assert verdict["ready"] is True
        assert verdict["can_execute_sql"] is None
        assert len(verdict["unverified"]) == 1

    def test_a_role_without_chat_is_a_blocker(self):
        verdict = ontology_api._summarise(
            ontology_api._UNVERIFIED, {"count": 1}, {"data": [{"name": "demo"}]}
        )

        assert verdict["ready"] is False
        assert verdict["ontology_compiled"] is None
        assert "cannot use chat" in verdict["blockers"][0]

    def test_not_compiled_and_nothing_ingested(self):
        verdict = ontology_api._summarise(
            {"calculated": False}, {"count": 0}, {"data": []}
        )

        assert verdict["ready"] is False
        assert len(verdict["blockers"]) == 3

    def test_probe_tells_a_refusal_from_a_failure(self, hopsworks_client, mocker):
        get = mocker.patch("hopsworks_common.core.ontology_api._get")
        get.side_effect = _error(403)
        assert (
            ontology_api._probe("sales", "api/connections") is ontology_api._UNVERIFIED
        )
        get.side_effect = _error(404)
        assert (
            ontology_api._probe("sales", "api/connections") is ontology_api._UNVERIFIED
        )
        get.side_effect = _error(500)
        assert ontology_api._probe("sales", "api/connections") is None
        get.side_effect = _error(401)
        with pytest.raises(RestAPIError):
            ontology_api._probe("sales", "api/connections")


class TestRefusal:
    def test_hopsworks_refusing_the_token_is_left_to_its_own_message(self):
        exc = _error(
            403, url="https://h/hopsworks-api/api/project/119/apps/sales/proxy-token"
        )

        assert ontology_api._refusal(exc, "sales") is None

    @pytest.mark.parametrize(
        ("status", "body", "expected"),
        [
            (
                403,
                {"detail": "chat:use"},
                "Observers and Feature store restricted members",
            ),
            (
                409,
                {"detail": "Semantic layer not compiled"},
                "cannot answer right now: Semantic layer not compiled",
            ),
            (503, None, "start it with `hops app start sales`"),
            (500, {"detail": "boom"}, "answered HTTP 500: boom"),
        ],
    )
    def test_the_app_refusing_says_what_to_do(self, status, body, expected):
        assert expected in ontology_api._refusal(_error(status, body), "sales")


class TestRequests:
    def test_a_token_is_minted_once_and_sent_as_the_proxy_cookie(
        self, hopsworks_client, mocker
    ):
        request = mocker.patch(
            "hopsworks_common.core.ontology_api.requests.request",
            return_value=_response(200, {"items": []}),
        )

        assert ontology_api._get(
            "sales", "api/terms", {"query": "cust", "limit": None}
        ) == {"items": []}
        ontology_api._get("sales", "api/terms")

        hopsworks_client._send_request.assert_called_once_with(
            "POST", ["project", 119, "apps", "sales", "proxy-token"]
        )
        args, kwargs = request.call_args_list[0]
        assert args == (
            "GET",
            "https://hopsworks.example:443/hopsworks-api/pythonapp/demo%20project/sales/api/terms",
        )
        assert kwargs["params"] == {"query": "cust"}
        assert kwargs["cookies"] == {"proxy_session": "minted"}
        assert kwargs["verify"] == "/tmp/ca.pem"
        assert "auth" not in kwargs

    def test_a_token_near_its_expiry_is_minted_again(self, hopsworks_client, mocker):
        mocker.patch(
            "hopsworks_common.core.ontology_api.requests.request",
            return_value=_response(200, {}),
        )
        hopsworks_client._send_request.return_value = {
            "token": "short",
            "expiresAt": int(time.time() * 1000) + 30_000,
        }

        ontology_api._get("sales", "api/terms")
        ontology_api._get("sales", "api/terms")

        assert hopsworks_client._send_request.call_count == 2

    def test_a_refusal_raises_with_the_response(self, hopsworks_client, mocker):
        mocker.patch(
            "hopsworks_common.core.ontology_api.requests.request",
            return_value=_response(409, {"detail": "busy"}),
        )

        with pytest.raises(RestAPIError) as raised:
            ontology_api._post(
                "sales", "api/question-entity-coverage", {"question": "q"}
            )
        assert raised.value.response.status_code == 409

    def test_ask_streams_the_events_of_one_turn(self, hopsworks_client, mocker):
        lines = [
            'data: {"type": "step", "label": "Intent"}',
            ": ping",
            'data: {"type": "result", "answer": {"response": "14"}}',
            "data: [DONE]",
        ]
        request = mocker.patch(
            "hopsworks_common.core.ontology_api.requests.request",
            return_value=_response(200, lines=lines),
        )

        events = list(
            ontology_api._ask("sales", "How many orders?", conversation_id="c-1")
        )

        assert [e["type"] for e in events] == ["step", "result"]
        kwargs = request.call_args.kwargs
        assert kwargs["json"] == {
            "question": "How many orders?",
            "conversation_id": "c-1",
        }
        assert kwargs["stream"] is True
        assert kwargs["headers"] == {"Accept": "text/event-stream"}
