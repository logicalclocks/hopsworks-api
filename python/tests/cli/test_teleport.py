"""Unit tests for the local half of ``hops teleport``.

Covers the pure-local logic that needs no live cluster: deriving Claude Code's
project-directory slug from the cwd, and resolving which session JSONL to ship
(explicit id, most-recent default, and the error paths).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from hopsworks.cli.commands import teleport


def test_cwd_slug_matches_claude_code_mapping(monkeypatch):
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: Path("/Users/lex/x-y.z")))
    assert teleport._cwd_slug() == "-Users-lex-x-y-z"


@pytest.fixture
def sessions_dir(tmp_path, monkeypatch):
    """Point the module at a tmp ``projects`` root and return a slug's dir."""
    monkeypatch.setattr(teleport, "_CLAUDE_PROJECTS", tmp_path)
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
    assert teleport._resolve_session("-Users-lex-proj", None) == new


def test_resolve_session_honors_explicit_id(sessions_dir):
    (sessions_dir / "abc.jsonl").write_text("{}")
    (sessions_dir / "def.jsonl").write_text("{}")
    assert teleport._resolve_session("-Users-lex-proj", "abc").name == "abc.jsonl"


def test_resolve_session_unknown_id_errors(sessions_dir):
    (sessions_dir / "abc.jsonl").write_text("{}")
    with pytest.raises(Exception, match="not found"):
        teleport._resolve_session("-Users-lex-proj", "missing")


def test_resolve_session_no_dir_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(teleport, "_CLAUDE_PROJECTS", tmp_path)
    with pytest.raises(Exception, match="No Claude Code sessions"):
        teleport._resolve_session("-nope", None)
