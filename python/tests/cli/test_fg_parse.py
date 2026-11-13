"""``hops fg parse``: the pure decisions of a run and the command surface.

The plan (parse, unchanged, removed, skipped), the run state across killed
runs, page ranges, the time budget and the group naming are plain functions
tested here with real files on disk. Docling itself and the group's stores
are tested on a cluster in tests/live/test_fg_parse.py.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import click
import pandas as pd
import pytest
from click.testing import CliRunner
from hopsworks.cli import parse_run
from hopsworks.cli.commands import parse as parse_cmd
from hopsworks.cli.main import cli


# --- paths and names -------------------------------------------------------------


@pytest.mark.parametrize(
    ("given", "normal"),
    [
        ("Resources/docs/", "Resources/docs"),
        ("/Resources//docs/./a.pdf", "Resources/docs/a.pdf"),
        ("  Resources/hopsable/uploads ", "Resources/hopsable/uploads"),
    ],
)
def test_paths_are_project_relative(given, normal):
    assert parse_run.normal_path(given) == normal


def test_a_path_out_of_the_project_is_refused():
    with pytest.raises(parse_run.ParseError, match="leaves the project"):
        parse_run.normal_path("Resources/../../other")


@pytest.mark.parametrize(
    ("root", "group"),
    [
        ("Resources/hopsable/uploads", "uploads_pages"),
        ("Resources/Q3 Report.pdf", "q3_report_pages"),
        ("Resources/2024-filings", "f_2024_filings_pages"),
        ("Resources/---", "files_pages"),
    ],
)
def test_the_default_group_is_the_name_plus_pages(root, group):
    assert parse_cmd.default_group(root) == group


def test_the_description_names_path_and_owner_and_reads_back():
    text = parse_run.description("Resources/docs", "docs_pages")
    assert parse_run.described(SimpleNamespace(description=text)) == {
        "path": "Resources/docs",
        "owner": "docs_pages",
    }
    assert parse_run.described(SimpleNamespace(description="made by hand")) is None


# --- one path per group (design-docling-parse-r1 MED 1) --------------------------


def _fs_with(description):
    group = SimpleNamespace(version=1, description=description)
    return SimpleNamespace(get_feature_groups=lambda name: [group])


def test_a_group_made_for_another_path_is_refused_before_anything_changes():
    fs = _fs_with(parse_run.description("Resources/a", "g_pages"))
    with pytest.raises(click.ClickException, match="g_pages parses Resources/a"):
        parse_cmd.latest_owned(fs, "g_pages", "Resources/b")


def test_a_group_not_made_by_parse_is_refused():
    fs = _fs_with("someone's group")
    with pytest.raises(click.ClickException, match="not made by hops fg parse"):
        parse_cmd.latest_owned(fs, "g_pages", "Resources/a")


def test_the_same_path_reuses_the_group():
    fs = _fs_with(parse_run.description("Resources/a", "g_pages"))
    assert parse_cmd.latest_owned(fs, "g_pages", "Resources/a").version == 1


# --- the plan ----------------------------------------------------------------------


def test_the_plan_parses_new_and_changed_files_and_removes_files_gone():
    files = {"a.pdf": "1", "b.pdf": "2", "c.docx": "3"}
    held = {"a.pdf": "1", "b.pdf": "old", "gone.pdf": "9"}
    p = parse_run.plan(files, held, stops={})
    assert p.parse == ["b.pdf", "c.docx"]
    assert p.unchanged == ["a.pdf"]
    assert p.removed == ["gone.pdf"]
    assert p.skipped == {}


def test_a_file_that_stopped_two_runs_is_skipped_until_its_content_changes():
    stops = {"big.pdf@1": parse_run.STOPS_BEFORE_SKIP}
    skipped = parse_run.plan({"big.pdf": "1"}, {}, stops)
    assert skipped.parse == [] and "smaller --range-pages" in skipped.skipped["big.pdf"]
    changed = parse_run.plan({"big.pdf": "2"}, {}, stops)
    assert changed.parse == ["big.pdf"]


def test_one_stop_is_retried():
    p = parse_run.plan({"big.pdf": "1"}, {}, {"big.pdf@1": 1})
    assert p.parse == ["big.pdf"]


# --- run state across killed runs (design-docling-parse-r2 LOW) ------------------


def test_a_run_killed_inside_a_file_counts_one_stop_and_the_next_retries_it(tmp_path):
    killed = parse_run.State(tmp_path, "g")
    killed.at("Resources/a@b.pdf", "d1", 51, 100)  # the process ends here

    first = parse_run.State(tmp_path, "g")
    assert first.take_over() == "Resources/a@b.pdf pages 51-100"
    assert first.data["stops"] == {"Resources/a@b.pdf@d1": 1}
    assert parse_run.plan({"Resources/a@b.pdf": "d1"}, {}, first.data["stops"]).parse

    first.at("Resources/a@b.pdf", "d1", 51, 100)  # killed again
    second = parse_run.State(tmp_path, "g")
    second.take_over()
    p = parse_run.plan({"Resources/a@b.pdf": "d1"}, {}, second.data["stops"])
    assert p.parse == [] and "Resources/a@b.pdf" in p.skipped


def test_a_finished_file_clears_its_stops_and_a_clean_run_leaves_nothing_current(
    tmp_path,
):
    state = parse_run.State(tmp_path, "g")
    state.data["stops"]["a.pdf@d"] = 1
    state.at("a.pdf", "d", 1, 50)
    state.done("a.pdf", "d")
    state.end()
    saved = json.loads((tmp_path / parse_run.STATE_DIR / "g.json").read_text())
    assert saved == {"stops": {}}
    assert parse_run.State(tmp_path, "g").take_over() is None


def test_an_unreadable_state_file_stops_the_run_before_any_write(tmp_path):
    (tmp_path / parse_run.STATE_DIR).mkdir(parents=True)
    (tmp_path / parse_run.STATE_DIR / "g.json").write_text("{not json")
    with pytest.raises(parse_run.ParseError, match="Nothing was written"):
        parse_run.State(tmp_path, "g")


# --- ranges, files and caps ---------------------------------------------------------


def test_ranges_cover_every_page_once():
    assert parse_run.ranges(120, 50) == [(1, 50), (51, 100), (101, 120)]
    assert parse_run.ranges(50, 50) == [(1, 50)]
    assert parse_run.ranges(0, 50) == []


def test_files_are_listed_under_the_path_with_their_sizes(tmp_path):
    docs = tmp_path / "Resources" / "docs"
    (docs / "sub").mkdir(parents=True)
    (docs / "a.pdf").write_bytes(b"x" * 10)
    (docs / "sub" / "b.DOCX").write_bytes(b"y" * 3)
    (docs / "notes.bin").write_bytes(b"z")
    assert parse_run.list_files(tmp_path, "Resources/docs") == {
        "Resources/docs/a.pdf": 10,
        "Resources/docs/sub/b.DOCX": 3,
    }
    assert parse_run.list_files(tmp_path, "Resources/docs/a.pdf") == {
        "Resources/docs/a.pdf": 10
    }
    with pytest.raises(parse_run.ParseError, match="not in the project"):
        parse_run.list_files(tmp_path, "Resources/missing")


class _Group:
    """The group's two calls a run makes, on real pandas frames."""

    name = "g_pages"

    def __init__(self, rows=()):
        self.rows = pd.DataFrame(list(rows), columns=["path", "page", "file_sha256"])
        self.inserted = None
        self.removed = None

    def select(self, columns):
        return SimpleNamespace(read=lambda: self.rows[columns])

    def insert(self, frame):
        self.inserted = frame

    def remove_rows(self, frame):
        self.removed = frame


class _Conv:
    parser = "test"


def _run(tmp_path, fg, monkeypatch, pages_per_file, clock, max_minutes=60, **caps):
    calls = []

    def convert(conv, file, *, ocr, range_pages, on_range, past_budget):
        out = []
        for first, last in parse_run.ranges(pages_per_file[file.name], range_pages):
            if out and past_budget():
                raise parse_run.OutOfTime
            on_range(first, last)
            calls.append((file.name, first, last))
            out += [(p, f"{file.name} p{p}") for p in range(first, last + 1)]
        return out

    monkeypatch.setattr(parse_run, "convert", convert)
    monkeypatch.setattr(parse_run, "pages_of", lambda f: pages_per_file[f.name])
    outcome = parse_run.run(
        fg=fg, mount=tmp_path, root="Resources/docs", group="g_pages", conv=_Conv(),
        ocr="auto", range_pages=50, max_pages=caps.get("max_pages", 500),
        max_file_mb=caps.get("max_file_mb", 500), max_minutes=max_minutes,
        log=lambda *a: None, clock=clock,
    )  # fmt: skip
    return outcome, calls


def _docs(tmp_path, **files):
    docs = tmp_path / "Resources" / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    for name, body in files.items():
        (docs / name).write_bytes(body)
    return docs


def test_a_run_writes_new_pages_and_deletes_pages_and_files_gone(tmp_path, monkeypatch):
    _docs(tmp_path, **{"a.pdf": b"new a", "b.pdf": b"b"})
    b_sha = parse_run.sha256(tmp_path / "Resources/docs/b.pdf")
    fg = _Group([
        ("Resources/docs/a.pdf", 1, "old"), ("Resources/docs/a.pdf", 2, "old"),
        ("Resources/docs/a.pdf", 3, "old"), ("Resources/docs/b.pdf", 1, b_sha),
        ("Resources/docs/gone.pdf", 1, "x"),
    ])  # fmt: skip
    outcome, _ = _run(
        tmp_path, fg, monkeypatch, {"a.pdf": 2, "b.pdf": 1}, clock=lambda: 0
    )
    assert outcome.parsed == ["Resources/docs/a.pdf"] and outcome.unchanged == 1
    assert list(fg.inserted["page"]) == [1, 2]
    assert sorted(map(tuple, fg.removed.values.tolist())) == [
        ("Resources/docs/a.pdf", 3),
        ("Resources/docs/gone.pdf", 1),
    ]


def test_a_file_over_the_size_cap_stops_the_run_before_any_parse(tmp_path, monkeypatch):
    _docs(tmp_path, **{"big.pdf": b"x" * 2_000_001})
    fg = _Group()
    with pytest.raises(parse_run.ParseError, match="over 2 MB"):
        _run(tmp_path, fg, monkeypatch, {"big.pdf": 1}, clock=lambda: 0, max_file_mb=2)
    assert fg.inserted is None


def test_pages_over_the_cap_stop_the_run_before_any_parse(tmp_path, monkeypatch):
    _docs(tmp_path, **{"a.pdf": b"a", "b.pdf": b"b"})
    with pytest.raises(parse_run.ParseError, match="have 600 pages, over the 500"):
        _run(
            tmp_path,
            _Group(),
            monkeypatch,
            {"a.pdf": 300, "b.pdf": 300},
            clock=lambda: 0,
        )


def test_the_budget_keeps_finished_files_and_leaves_the_rest_for_the_next_run(
    tmp_path, monkeypatch
):
    _docs(tmp_path, **{"a.pdf": b"a", "b.pdf": b"b", "c.pdf": b"c"})
    now = [0.0]

    def clock():
        now[0] += 1000  # each read is a range's worth of time
        return now[0]

    fg = _Group()
    outcome, _ = _run(tmp_path, fg, monkeypatch, {"a.pdf": 10, "b.pdf": 200, "c.pdf": 10},
                      clock=clock, max_minutes=60)  # fmt: skip
    assert outcome.parsed == ["Resources/docs/a.pdf"]
    assert outcome.left == ["Resources/docs/b.pdf", "Resources/docs/c.pdf"]
    assert "60 min" in outcome.stopped_by
    assert set(fg.inserted["path"]) == {"Resources/docs/a.pdf"}


def test_a_file_that_alone_outlasts_the_budget_is_named_and_not_retried(
    tmp_path, monkeypatch
):
    _docs(tmp_path, **{"huge.pdf": b"h", "small.pdf": b"s"})
    now = [0.0]

    def clock():
        now[0] += 1000
        return now[0]

    outcome, calls = _run(tmp_path, _Group(), monkeypatch, {"huge.pdf": 400, "small.pdf": 1},
                          clock=clock)  # fmt: skip
    assert (
        "does not finish within the 60 min"
        in outcome.skipped["Resources/docs/huge.pdf"]
    )
    assert outcome.left == ["Resources/docs/small.pdf"]

    again, calls = _run(tmp_path, _Group(), monkeypatch, {"huge.pdf": 400, "small.pdf": 1},
                        clock=lambda: 0)  # fmt: skip
    assert "Resources/docs/huge.pdf" in again.skipped
    assert [c[0] for c in calls] == ["small.pdf"]


# --- command surface -----------------------------------------------------------------


def test_the_command_is_under_fg_with_delete():
    result = CliRunner().invoke(cli, ["fg", "parse", "--help"])
    text = " ".join(result.output.split())
    assert result.exit_code == 0, result.output
    assert "Markdown pages" in text and "hops fg embed GROUP --text text" in text
    result = CliRunner().invoke(cli, ["fg", "parse", "delete", "--help"])
    assert result.exit_code == 0 and "deletion runs as an execution" in " ".join(
        result.output.split()
    )


def test_range_pages_below_five_is_refused():
    result = CliRunner().invoke(
        cli, ["fg", "parse", "Resources/docs", "--range-pages", "2"]
    )
    assert result.exit_code == 2 and "--range-pages" in result.output
