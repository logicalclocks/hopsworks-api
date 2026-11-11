"""`hops ontology`, and the reference code the `ontology` factory build copies (HWORKS-3744)."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
import yaml
from click.testing import CliRunner
from hopsworks.cli.main import cli
from hopsworks_common.client.exceptions import RestAPIError
from hopsworks_common.core import ontology_api


REF = (
    Path(ontology_api.__file__).resolve().parents[2]
    / "hopsworks/skills/data/hops-ontology/references/auto_ontology"
)

EVENTS = [
    {"type": "step", "label": "Intent", "thought": "a count of orders"},
    {"type": "sql", "sql": "SELECT COUNT(*) FROM sales_orders"},
    {
        "type": "result",
        "answer": {
            "response": "14 orders were placed in March 2025.",
            "sql_code": "SELECT COUNT(*) AS orders FROM sales_orders WHERE ...",
            "sql_response_from_db": [{"orders": 14}],
        },
    },
]


def _asked(events):
    calls = []

    def ask(app_name, question, conversation_id=None, evidence=None, timeout=900.0):
        calls.append((app_name, question, conversation_id, timeout))
        yield from events

    return ask, calls


def test_ask_prints_the_steps_the_answer_its_sql_and_its_rows(
    mock_project, monkeypatch
):
    ask, calls = _asked(EVENTS)
    monkeypatch.setattr(ontology_api, "_ask", ask)

    done = CliRunner().invoke(
        cli,
        [
            "ontology",
            "ask",
            "sales-ontology",
            "How many orders in March?",
            "--conversation-id",
            "c-1",
        ],
    )

    assert done.exit_code == 0, done.output
    assert calls == [("sales-ontology", "How many orders in March?", "c-1", 900.0)]
    assert "Intent: a count of orders" in done.output
    assert "14 orders were placed in March 2025." in done.output
    assert "SELECT COUNT(*) AS orders FROM sales_orders WHERE ..." in done.output
    assert ["14"] in [line.split() for line in done.output.splitlines()]


def test_ask_in_json_mode_prints_only_the_answer(mock_project, monkeypatch):
    rows = [{"n": i} for i in range(ontology_api.MAX_ROWS + 1)]
    events = [
        {"type": "result", "answer": {"response": "many", "sql_response_from_db": rows}}
    ]
    monkeypatch.setattr(ontology_api, "_ask", _asked(events)[0])

    done = CliRunner().invoke(
        cli, ["ontology", "ask", "sales-ontology", "All?", "--json"]
    )

    assert done.exit_code == 0, done.output
    answer = json.loads(done.output)
    assert answer["truncated"] is True
    assert answer["row_count"] == ontology_api.MAX_ROWS + 1
    assert len(answer["rows"]) == ontology_api.MAX_ROWS


def test_ask_says_why_the_app_refused(mock_project, monkeypatch):
    response = mock.Mock(status_code=403, reason="Forbidden", content=b"{}")
    response.json.return_value = {"detail": "missing permission chat:use"}

    def refused(*args, **kwargs):
        raise RestAPIError(
            "https://h/hopsworks-api/pythonapp/demo/sales-ontology/api/chat/completions",
            response,
        )
        yield

    monkeypatch.setattr(ontology_api, "_ask", refused)

    done = CliRunner().invoke(cli, ["ontology", "ask", "sales-ontology", "q"])

    assert done.exit_code != 0
    assert "get no chat" in done.output
    assert "Metadata operation error" not in done.output


def test_status_of_an_app_that_is_not_serving(mock_project):
    mock_project.get_app_api.return_value.get_app.return_value = SimpleNamespace(
        state="STOPPED", serving=False, app_url=None
    )

    done = CliRunner().invoke(cli, ["ontology", "status", "sales-ontology"])

    assert done.exit_code == 1
    assert "hops app start sales-ontology" in done.output


def test_status_of_a_ready_system(mock_project, monkeypatch):
    mock_project.get_app_api.return_value.get_app.return_value = SimpleNamespace(
        state="RUNNING",
        serving=True,
        app_url="https://h/hopsworks-api/pythonapp/demo/sales-ontology/",
    )
    monkeypatch.setattr(
        ontology_api,
        "_readiness",
        lambda app: ontology_api._summarise(
            {"calculated": True}, {"count": 1}, {"data": [{"name": "demo"}]}
        ),
    )

    done = CliRunner().invoke(cli, ["ontology", "status", "sales-ontology", "--json"])

    assert done.exit_code == 0, done.output
    assert json.loads(done.output)["ready"] is True


def test_status_of_a_missing_app(mock_project):
    mock_project.get_app_api.return_value.get_app.return_value = None

    done = CliRunner().invoke(cli, ["ontology", "status", "nope"])

    assert done.exit_code != 0
    assert "no app nope" in done.output


# region Reference code


def test_the_launchers_are_the_contracts_guarded_calls():
    # The guard matters: the app's chat worker is a spawned process, which re-runs
    # app.py, and an unguarded app() there starts a second server.
    def code(path):
        return [
            line
            for line in path.read_text().splitlines()
            if line and not line.startswith("#")
        ]

    guard = 'if __name__ == "__main__":'
    assert code(REF / "app.py") == [
        "from auto_ontology.hopsworks.launch import app",
        guard,
        "    app()",
    ]
    for launcher, job in [
        ("ingest", "ingest"),
        ("compile", "compile"),
        ("embed", "embed"),
        ("reset", "reset"),
        ("import_model", "import"),
    ]:
        assert code(REF / "jobs" / f"{launcher}.py") == [
            "from auto_ontology.hopsworks.launch import job",
            guard,
            f'    job("{job}")',
        ]


def test_the_vllm_configs_leave_the_served_name_to_hopsworks():
    llm = yaml.safe_load((REF / "vllm/llm.yaml").read_text())
    embed = yaml.safe_load((REF / "vllm/embed.yaml").read_text())

    assert llm["gpu-memory-utilization"] + embed["gpu-memory-utilization"] <= 0.95
    assert "moe-backend" not in llm
    assert embed["runner"] == "pooling"
    assert json.loads(embed["pooler-config"])["pooling_type"] == "MEAN"
    assert "served-model-name" not in llm and "served-model-name" not in embed


def test_the_template_has_the_contracts_keys():
    template = yaml.safe_load((REF / "ontology.yaml.template").read_text())

    assert list(template) == [
        "schema_version",
        "name",
        "app",
        "feature_store",
        "feature_groups",
        "embedding",
        "llm",
        "sources",
        "jobs",
        "semantic_compilation",
        "pii_detection",
        "distinct_value_probing",
    ]
    assert list(template["jobs"]) == ["ingest", "compile", "embed", "reset", "import"]


@pytest.fixture
def wwi():
    spec = importlib.util.spec_from_file_location(
        "wwi_setup", REF / "examples/wwi/setup.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_example_data_is_seeded_and_a_few_hundred_rows(wwi):
    rows = wwi.generate()

    assert rows == wwi.generate()
    assert list(rows) == list(wwi.TABLES)
    assert 300 <= sum(len(r) for r in rows.values()) <= 1000


def test_the_example_answers_follow_from_the_data(wwi):
    db = sqlite3.connect(":memory:")
    for table, columns in wwi.TABLES.items():
        # SQLite has no inline KEY; the foreign keys and the rest carry over.
        kept = [
            c.strip() for c in columns.split(",\n") if not c.strip().startswith("KEY")
        ]
        db.execute(f"CREATE TABLE {table} ({', '.join(kept)})")
        stored = [wwi._stored(table, row) for row in wwi.generate()[table]]
        stored = [
            tuple(v.isoformat() if hasattr(v, "isoformat") else v for v in row)
            for row in stored
        ]
        db.executemany(
            f"INSERT INTO {table} VALUES ({', '.join('?' * len(stored[0]))})", stored
        )

    questions = yaml.safe_load((REF / "examples/wwi/questions.yaml").read_text())[
        "questions"
    ]
    assert len(questions) == 5
    for q in questions:
        got = [
            tuple(
                float(v)
                if isinstance(v, str) and v.replace(".", "", 1).isdigit()
                else v
                for v in row
            )
            for row in db.execute(q["sql"]).fetchall()
        ]
        expected = q["answer"]
        if isinstance(expected, list) and len(got) == 1:
            assert list(got[0]) == expected, q["question"]
        elif isinstance(expected, list):
            assert [row[0] for row in got] == expected, q["question"]
        else:
            assert got == [(expected,)], q["question"]


# endregion
