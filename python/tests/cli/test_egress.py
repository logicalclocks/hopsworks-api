"""The egress policy of a project, as ``hops project info`` and ``hops datasource`` show it.

The answers are recorded bodies of ``GET /project/<id>/kube/egress``; only the HTTP
call is replaced.
"""

from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner
from hopsworks.cli import egress
from hopsworks.cli.commands import datasource as ds
from hopsworks.cli.main import cli


ASK = "Ask for it: https://hopsable.example/account/billing?egress=29898:5432/TCP"

ENFORCED = {
    "mode": "ENFORCE",
    "enforced": True,
    "allowed": [
        {"port": 443, "endPort": None, "protocol": "TCP", "source": "global"},
        {"port": 80, "endPort": None, "protocol": "TCP", "source": "global"},
        {"port": 8000, "endPort": 8010, "protocol": "TCP", "source": "global"},
        {"port": 6543, "endPort": None, "protocol": "TCP", "source": "project"},
    ],
    "ask": ASK,
}
OFF = {"mode": "OFF", "enforced": False, "allowed": [], "ask": None}
NOT_ENFORCED = {"mode": "ENFORCE", "enforced": False, "allowed": [], "ask": None}


def test_the_summary_groups_the_open_ports_by_source():
    assert egress.summary(ENFORCED) == (
        "enforced: 443/TCP 80/TCP 8000-8010/TCP (global); 6543/TCP (project)"
    )


def test_the_summary_of_a_policy_that_is_off_or_not_enforced():
    assert egress.summary(OFF) == "off"
    assert egress.summary(NOT_ENFORCED) == "not enforced in this project"


@pytest.mark.parametrize(
    ("port", "protocol", "expected"),
    [
        (443, "TCP", True),
        (6543, "TCP", True),
        (8005, "TCP", True),
        (8011, "TCP", False),
        (5432, "TCP", False),
        (443, "UDP", False),
    ],
)
def test_a_port_is_open_when_listed_under_its_protocol(port, protocol, expected):
    assert egress.is_open(ENFORCED, port, protocol) is expected


def test_every_port_is_open_when_the_policy_does_not_enforce():
    assert egress.is_open(NOT_ENFORCED, 5432)
    assert egress.is_open(OFF, 5432)


def test_the_closed_line_carries_the_ask():
    assert egress.closed_line("demo", 5432, ASK) == (
        f"Egress from project demo to port 5432/TCP is closed. {ASK}"
    )


def _send(answer):
    from hopsworks_common.core import rest

    return (
        mock.patch.object(rest, "_project_path", side_effect=lambda *p: list(p)),
        mock.patch.object(rest, "_send_request", side_effect=answer),
    )


def test_the_row_is_unavailable_when_the_read_fails():
    def fail(method, path, **kw):
        raise RuntimeError("503")

    path, send = _send(fail)
    with path, send:
        assert egress.row() == ["Egress", "unavailable"]


def test_the_read_names_the_port_and_protocol_only_when_given():
    path, send = _send(lambda method, path, **kw: ENFORCED)
    with path, send as sent:
        egress.read()
        egress.read(5432)
    assert sent.call_args_list[0].kwargs == {}
    assert sent.call_args_list[1].kwargs == {
        "query_params": {"port": 5432, "protocol": "TCP"}
    }


def _create(answer, extra=()):
    """Run ``datasource create sql --test`` with the policy read answering ``answer``."""
    from hopsworks_common.core import rest

    def send(method, path, **kw):
        if path[-1] == "egress":
            return answer(kw)
        return {"statusCode": 0, "connectionOutput": "OK"}

    with (
        mock.patch.object(ds.session, "get_feature_store") as fs,
        mock.patch.object(rest, "_project_path", side_effect=lambda *p: list(p)),
        mock.patch.object(rest, "_send_request", side_effect=send),
    ):
        fs.return_value.id = 7
        fs.return_value.project_name = "demo"
        return CliRunner().invoke(
            cli,
            [
                "datasource", "create", "sql", "n", "--test",
                "--database-type", "postgresql", "--host", "db.example",
                "--port", "5432", "--database", "d", "--user", "u",
                *extra,
            ],
        )  # fmt: skip


def test_a_test_warns_when_the_sources_port_is_closed():
    result = _create(lambda kw: ENFORCED)
    assert result.exit_code == 0, result.output
    assert f"Egress from project demo to port 5432/TCP is closed. {ASK}" in (
        result.output
    )
    assert "Connection to n works" in result.output


def test_a_test_says_nothing_when_the_port_is_open_or_the_read_fails():
    open_port = {**ENFORCED, "allowed": [{"port": 5432, "protocol": "TCP"}]}
    assert "is closed" not in _create(lambda kw: open_port).output

    def fail(kw):
        raise RuntimeError("503")

    result = _create(fail)
    assert result.exit_code == 0, result.output
    assert "is closed" not in result.output
