"""Unit tests for the local half of ``hops session``.

Covers the pure-local logic that needs no live cluster: deriving Claude Code's
project-directory slug from the cwd, and resolving which session JSONL to push
(explicit id, most-recent default, and the error paths).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hopsworks.cli.commands import session


def test_cwd_slug_matches_claude_code_mapping(monkeypatch):
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: Path("/Users/lex/x-y.z")))
    assert session._cwd_slug() == "-Users-lex-x-y-z"


@pytest.fixture
def sessions_dir(tmp_path, monkeypatch):
    """Point the module at a tmp ``projects`` root and return a slug's dir."""
    monkeypatch.setattr(session, "_CLAUDE_PROJECTS", tmp_path)
    slug_dir = tmp_path / "-Users-lex-proj"
    slug_dir.mkdir()
    return slug_dir


def test_resolve_session_picks_most_recent(sessions_dir):
    old = sessions_dir / "old.jsonl"
    new = sessions_dir / "new.jsonl"
    old.write_text("{}")
    new.write_text("{}")
    # Make `new` unambiguously the freshest regardless of write order.
    import os

    os.utime(old, (1, 1))
    os.utime(new, (2, 2))
    assert session._resolve_local_session("-Users-lex-proj", None) == new


def test_resolve_session_honors_explicit_id(sessions_dir):
    (sessions_dir / "abc.jsonl").write_text("{}")
    (sessions_dir / "def.jsonl").write_text("{}")
    assert session._resolve_local_session("-Users-lex-proj", "abc").name == "abc.jsonl"


def test_resolve_session_unknown_id_errors(sessions_dir):
    (sessions_dir / "abc.jsonl").write_text("{}")
    with pytest.raises(Exception, match="not found"):
        session._resolve_local_session("-Users-lex-proj", "missing")


def test_resolve_session_no_dir_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(session, "_CLAUDE_PROJECTS", tmp_path)
    with pytest.raises(Exception, match="No Claude Code sessions"):
        session._resolve_local_session("-nope", None)


def test_is_active_session_newest_and_recent(sessions_dir):
    import os

    old = sessions_dir / "old.jsonl"
    new = sessions_dir / "new.jsonl"
    old.write_text("{}")
    new.write_text("{}")
    os.utime(old, (1, 1))  # ancient
    assert session._is_active_session(new) is True  # newest, just written
    assert session._is_active_session(old) is False  # not the newest


def test_is_active_session_stale_is_not_active(sessions_dir):
    import os

    only = sessions_dir / "only.jsonl"
    only.write_text("{}")
    os.utime(only, (1, 1))  # newest but written long ago
    assert session._is_active_session(only) is False


def test_local_away_reads_markers(sessions_dir):
    (sessions_dir / "abc.away.json").write_text('{"project": "feast_bench"}')
    (sessions_dir / "def.away.json").write_text('{"project": "fraud"}')
    away = session._local_away("-Users-lex-proj")
    assert away == {"abc": "feast_bench", "def": "fraud"}


# --- baton reclaim on pull ---------------------------------------------------


def test_transcript_relation_same():
    lines = ["a", "b", "c"]
    assert session._transcript_relation(lines, list(lines), baseline=1) == "same"


def test_transcript_relation_fast_forward():
    # local is a strict prefix of remote -> the pod advanced, take remote.
    assert session._transcript_relation(
        ["a", "b"], ["a", "b", "c", "d"], baseline=2
    ) == "fast_forward"


def test_transcript_relation_local_ahead():
    # remote is a strict prefix of local -> we advanced locally, keep local.
    assert session._transcript_relation(
        ["a", "b", "c"], ["a", "b"], baseline=2
    ) == "local_ahead"


def test_transcript_relation_diverged_past_baseline():
    # Shared prefix through the baseline (2), tails differ beyond it.
    assert session._transcript_relation(
        ["a", "b", "x"], ["a", "b", "y"], baseline=2
    ) == "diverged"


def test_transcript_relation_baseline_mismatch():
    # They already differ inside the handed-off prefix: not one lineage.
    assert session._transcript_relation(
        ["a", "X", "c"], ["a", "b", "c"], baseline=3
    ) == "baseline_mismatch"


def test_pod_alive_true_when_session_present(monkeypatch):
    monkeypatch.setattr(session.terminal_api, "get_session", lambda pid: {"id": "s"})
    assert session._pod_alive(1) is True


def test_pod_alive_false_only_on_definitive_none(monkeypatch):
    monkeypatch.setattr(session.terminal_api, "get_session", lambda pid: None)
    assert session._pod_alive(1) is False


def test_pod_alive_failsafe_alive_on_error(monkeypatch):
    def boom(pid):
        raise RuntimeError("terminal feature disabled")

    monkeypatch.setattr(session.terminal_api, "get_session", boom)
    # Unknown liveness must not silently authorise a steal.
    assert session._pod_alive(1) is True
