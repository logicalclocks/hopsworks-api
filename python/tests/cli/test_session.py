"""Unit tests for the local half of ``hops session``.

Covers the pure-local logic that needs no live cluster: deriving Claude Code's
project-directory slug from the cwd, and resolving which session JSONL to push
(explicit id, most-recent default, and the error paths).
"""

from __future__ import annotations

import json
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
    assert (
        session._transcript_relation(["a", "b"], ["a", "b", "c", "d"], baseline=2)
        == "fast_forward"
    )


def test_transcript_relation_local_ahead():
    # remote is a strict prefix of local -> we advanced locally, keep local.
    assert (
        session._transcript_relation(["a", "b", "c"], ["a", "b"], baseline=2)
        == "local_ahead"
    )


def test_transcript_relation_diverged_past_baseline():
    # Shared prefix through the baseline (2), tails differ beyond it.
    assert (
        session._transcript_relation(["a", "b", "x"], ["a", "b", "y"], baseline=2)
        == "diverged"
    )


def test_transcript_relation_baseline_mismatch():
    # They already differ inside the handed-off prefix: not one lineage.
    assert (
        session._transcript_relation(["a", "X", "c"], ["a", "b", "c"], baseline=3)
        == "baseline_mismatch"
    )


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


# --- new / manifest / owner --------------------------------------------------


class _FakeDataset:
    """Minimal dataset_api stand-in: directory listings + JSON downloads."""

    def __init__(self, dirs: dict[str, list[str]], files: dict[str, dict]):
        self._dirs = dirs
        self._files = files

    def list(self, path: str) -> list[str]:
        if path not in self._dirs:
            raise RuntimeError(f"no such dir: {path}")
        return self._dirs[path]

    def download(self, remote: str, local_path: str, overwrite: bool = False):
        if remote not in self._files:
            raise RuntimeError(f"404: {remote}")
        Path(local_path).write_text(json.dumps(self._files[remote]))


def test_current_user_email_reads_client_stash(monkeypatch):
    class _Client:
        _user_email = "lex@logicalclocks.com"

    monkeypatch.setattr(session.client, "_get_instance", lambda: _Client())
    assert session._current_user_email() == "lex@logicalclocks.com"


def test_current_user_email_none_when_unavailable(monkeypatch):
    def boom():
        raise RuntimeError("no client")

    monkeypatch.setattr(session.client, "_get_instance", boom)
    assert session._current_user_email() is None


def test_build_manifest_carries_owner_mode_and_cwd(monkeypatch):
    monkeypatch.setattr(session, "_current_user_email", lambda: "lex@x.com")
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: Path("/Users/lex/p")))
    m = session._build_manifest("sid1", "-Users-lex-p", "new", None)
    assert m["session_id"] == "sid1"
    assert m["slug"] == "-Users-lex-p"
    assert m["mode"] == "new"
    assert m["model"] is None
    assert m["prompt"] is None
    assert m["cwd"] == "/Users/lex/p"
    assert m["user"] == "lex@x.com"


def test_build_manifest_carries_prompt(monkeypatch):
    monkeypatch.setattr(session, "_current_user_email", lambda: None)
    m = session._build_manifest("s", "-p", "new", None, "summarize the project")
    assert m["prompt"] == "summarize the project"


def test_upload_manifest_uploads_last_write_to_dest(tmp_path):
    seen: dict = {}

    class _DS:
        def upload(self, local_path, upload_path, overwrite):
            seen["upload_path"] = upload_path
            seen["overwrite"] = overwrite
            seen["content"] = json.loads(Path(local_path).read_text())

    session._upload_manifest(_DS(), "Resources/teleport/slug", "sid", {"mode": "new"})
    assert seen["upload_path"] == "Resources/teleport/slug"
    assert seen["overwrite"] is True
    assert seen["content"] == {"mode": "new"}


def test_upload_manifest_raises_on_failure():
    class _DS:
        def upload(self, **kwargs):
            raise RuntimeError("nope")

    with pytest.raises(Exception, match="Failed to upload teleport manifest"):
        session._upload_manifest(_DS(), "dest", "sid", {"a": 1})


def _teleport_tree():
    root = session._TELEPORT_DATASET
    dirs = {
        root: [f"{root}/-Users-lex-a", f"{root}/-Users-lex-b/"],
        f"{root}/-Users-lex-a": [f"{root}/-Users-lex-a/s1.jsonl"],
        f"{root}/-Users-lex-b": [f"{root}/-Users-lex-b/s2.jsonl"],
    }
    files = {f"{root}/-Users-lex-b/s2.teleport.json": {"cwd": "/Users/lex/b"}}
    return _FakeDataset(dirs, files)


def test_scan_slugs_lists_subdirs():
    assert session._scan_slugs(_teleport_tree()) == ["-Users-lex-a", "-Users-lex-b"]


def test_locate_session_finds_slug_and_origin_cwd():
    ds = _teleport_tree()
    assert session._locate_session(ds, "s2") == ("-Users-lex-b", "/Users/lex/b")
    # A session with no manifest still resolves its slug (cwd unknown).
    assert session._locate_session(ds, "s1") == ("-Users-lex-a", None)


def test_locate_session_none_when_absent():
    assert session._locate_session(_teleport_tree(), "missing") is None
