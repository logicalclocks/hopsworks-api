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

import json
import multiprocessing
import re

from hopsworks_common import events


def _lines(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def test_nothing_is_written_without_the_variable(tmp_path, monkeypatch):
    monkeypatch.delenv(events.ENV_VAR, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))

    events.emit("fg", "create", "done", name="orders", version=1)

    assert list(tmp_path.iterdir()) == []


def test_a_line_has_the_documented_shape(tmp_path, monkeypatch):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))

    events.emit(
        "file", "create", "done", path="Users/meb10000/churn.ipynb", detail={"rows": 3}
    )

    (line,) = _lines(target)
    assert line["v"] == 1
    assert re.fullmatch(r"[A-Za-z0-9_-]{1,64}", line["id"])
    assert line["at"].endswith("+00:00")
    assert line["kind"] == "file"
    assert line["key"] == {"path": "Users/meb10000/churn.ipynb"}
    assert (line["op"], line["phase"]) == ("create", "done")
    assert line["detail"] == {"rows": 3}
    assert "source" not in line and "project" not in line


def _write_many(target, worker):
    import os

    os.environ[events.ENV_VAR] = target
    for i in range(200):
        events.emit("fg", "update", "done", name=f"fg_{worker}", version=i + 1)


def test_parallel_writers_never_interleave_lines(tmp_path):
    target = str(tmp_path / "events.jsonl")
    workers = [
        multiprocessing.Process(target=_write_many, args=(target, w)) for w in range(4)
    ]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    lines = _lines(target)
    assert len(lines) == 800
    assert len({line["id"] for line in lines}) == 800


def test_progress_is_written_at_most_once_a_second_per_key(tmp_path, monkeypatch):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))
    events._last_progress.clear()

    for done in range(50):
        events.emit(
            "fg",
            "update",
            "progress",
            name="orders",
            version=1,
            progress={"done": done, "unit": "rows"},
        )
    events.emit("fg", "update", "done", name="orders", version=1)

    assert [line["phase"] for line in _lines(target)] == ["progress", "done"]


def test_a_detail_too_long_for_a_line_is_dropped(tmp_path, monkeypatch):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))

    events.emit(
        "job",
        "update",
        "failed",
        name="orders-features",
        detail={f"k{i}": "x" * 200 for i in range(30)},
    )

    (line,) = _lines(target)
    assert len(json.dumps(line["detail"])) <= 1024
    assert all(len(v) <= 200 for v in line["detail"].values())


def test_a_write_error_never_reaches_the_caller(tmp_path, monkeypatch):
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("")
    monkeypatch.setenv(events.ENV_VAR, str(blocker / "events.jsonl"))

    events.emit("model", "create", "done", name="churn", version=2)


def test_a_path_is_written_relative_to_the_project(tmp_path, monkeypatch, mocker):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))
    mocker.patch(
        "hopsworks_common.client._get_instance"
    ).return_value._project_name = "blah"

    events.emit("file", "create", "done", path="/Projects/blah/Users/u/x.ipynb")
    events.emit(
        "file",
        "create",
        "done",
        path="hopsfs://namenode:8020/Projects/blah/Resources/a.csv",
    )
    events.emit("file", "create", "done", path="Users/u/y.ipynb")
    events.emit("file", "create", "done", path="Resources//z.csv")

    assert [line["key"]["path"] for line in _lines(target)] == [
        "Users/u/x.ipynb",
        "Resources/a.csv",
        "Users/u/y.ipynb",
        "Resources/z.csv",
    ]


def test_a_tracked_block_reports_done_or_failed(tmp_path, monkeypatch):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))

    with events.tracked("model", "create", long=True, name="churn") as change:
        change.key["version"] = 3
    try:
        with events.tracked("job", "delete", name="etl"):
            raise RuntimeError("job busy\nsecond line")
    except RuntimeError:
        pass

    lines = _lines(target)
    assert [(line["kind"], line["phase"]) for line in lines] == [
        ("model", "started"),
        ("model", "done"),
        ("job", "failed"),
    ]
    assert lines[1]["key"] == {"name": "churn", "version": 3}
    assert lines[2]["detail"] == {"error": "RuntimeError: job busy"}


def test_the_decorator_reports_the_version_the_call_assigned(tmp_path, monkeypatch):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))

    class Group:
        name, version = "orders", None

    @events.tracking(
        "fg", "create", key=lambda g: {"name": g.name, "version": g.version}
    )
    def save(group):
        group.version = 4

    save(Group())

    (line,) = _lines(target)
    assert line["key"] == {"name": "orders", "version": 4}


def test_an_online_insert_reports_the_rows_delivered(tmp_path, monkeypatch):
    from hsfs.core import kafka_engine

    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))
    events._last_progress.clear()
    acked, _ = kafka_engine._build_ack_callback_and_optional_progress_bar(
        n_rows=3, is_multi_part_insert=False, offline_write_options={}
    )

    with events.tracked("fg", "update", long=True, name="orders", version=1):
        for _ in range(3):
            acked(None, None)

    progress = [line for line in _lines(target) if line["phase"] == "progress"]
    assert progress
    assert progress[0]["progress"] == {"done": 1, "unit": "rows", "total": 3}


def test_an_insert_reports_its_row_count(tmp_path, monkeypatch):
    target = tmp_path / "events.jsonl"
    monkeypatch.setenv(events.ENV_VAR, str(target))
    events._last_progress.clear()

    with events.tracked("fg", "update", long=True, name="orders", version=1):
        events.progress(0, 5, "rows")
        events.note(rows=5)

    lines = _lines(target)
    assert [line["phase"] for line in lines] == ["started", "progress", "done"]
    assert lines[1]["progress"] == {"done": 0, "unit": "rows", "total": 5}
    assert lines[2]["detail"] == {"rows": 5}
