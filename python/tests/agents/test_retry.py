"""What an evaluation job retries, and what it lets fail.

Twelve monitors of one execution start together; under that load the platform
refused three of them a listing or a feature-group read it would have answered a
moment later, and those three graded nothing.
"""

import pytest
from hopsworks_agents.eval._retry import _retrying


class Refused(Exception):
    def __init__(self, status):
        super().__init__(f"HTTP {status}")
        self.response = type("Response", (), {"status_code": status})()


def flaky(*failures, result="ok"):
    calls = []

    def call():
        calls.append(1)
        if len(calls) <= len(failures):
            raise failures[len(calls) - 1]
        return result

    return call, calls


def test_a_server_error_is_retried_until_it_answers():
    call, calls = flaky(Refused(500), Refused(503))
    assert _retrying(call, what="listing", sleep=lambda _s: None) == "ok"
    assert len(calls) == 3


def test_no_answer_at_all_is_retried_too():
    call, calls = flaky(ConnectionError("reset"))
    assert _retrying(call, what="listing", sleep=lambda _s: None) == "ok"
    assert len(calls) == 2


def test_a_refusal_of_the_request_is_not():
    call, calls = flaky(Refused(404))
    with pytest.raises(Refused):
        _retrying(call, what="listing", sleep=lambda _s: None)
    assert len(calls) == 1


def test_it_gives_up_after_the_last_wait():
    call, calls = flaky(Refused(500), Refused(500), Refused(500))
    with pytest.raises(Refused):
        _retrying(call, what="listing", sleep=lambda _s: None)
    assert len(calls) == 3
