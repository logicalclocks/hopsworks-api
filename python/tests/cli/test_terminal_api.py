"""The CLI's terminal API forwards every given ``hours`` value, including an out-of-range one."""

import pytest
from hopsworks.cli import terminal_api
from hopsworks_common import client


class _Client:
    def __init__(self):
        self.calls = []

    def _send_request(self, method, path, query_params=None, **kwargs):
        self.calls.append((method, path, query_params))
        return {"sessionId": "s", "minutesUntilExpiration": 719}


@pytest.fixture
def fake_client(monkeypatch):
    fake = _Client()
    monkeypatch.setattr(client, "_get_instance", lambda: fake)
    return fake


@pytest.mark.parametrize(
    "call, verb",
    [
        (terminal_api.start_session, "start"),
        (terminal_api.extend_session, "extend"),
    ],
)
def test_hours_is_left_to_the_cluster_when_not_given(fake_client, call, verb):
    call(7)
    assert fake_client.calls == [("POST", ["project", 7, "terminal", verb], None)]


@pytest.mark.parametrize("hours", [0, 1, 12])
@pytest.mark.parametrize(
    "call, verb",
    [
        (terminal_api.start_session, "start"),
        (terminal_api.extend_session, "extend"),
    ],
)
def test_every_given_hours_value_reaches_the_backend(fake_client, call, verb, hours):
    # 0 is out of range and must reach the backend to be refused there, not
    # be swallowed client-side into "use the default".
    call(7, hours)
    assert fake_client.calls == [
        ("POST", ["project", 7, "terminal", verb], {"hours": hours})
    ]
