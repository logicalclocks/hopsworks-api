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


def test_is_active_session_stale_is_not_active(sessions_dir, monkeypatch):
    import os

    only = sessions_dir / "only.jsonl"
    only.write_text("{}")
    os.utime(only, (1, 1))  # newest but written long ago
    monkeypatch.setattr(session, "_held_open", lambda p: False)
    assert session._is_active_session(only) is False


def test_is_active_session_stale_but_held_open_is_active(sessions_dir, monkeypatch):
    # The rename-race fix: an idle-but-open claude session must stay live.
    import os

    only = sessions_dir / "only.jsonl"
    only.write_text("{}")
    os.utime(only, (1, 1))  # newest but written long ago
    monkeypatch.setattr(session, "_held_open", lambda p: True)
    assert session._is_active_session(only) is True


def test_is_active_session_no_lsof_falls_back_to_mtime(sessions_dir, monkeypatch):
    import os

    only = sessions_dir / "only.jsonl"
    only.write_text("{}")
    os.utime(only, (1, 1))
    monkeypatch.setattr(session, "_held_open", lambda p: None)  # lsof absent
    assert session._is_active_session(only) is False


def test_is_active_session_non_newest_but_held_open_is_active(
    sessions_dir, monkeypatch
):
    # A file a live process holds open is never renamed, even when it is not the
    # newest transcript (an explicit push of an older, still-running session).
    import os

    old = sessions_dir / "old.jsonl"
    new = sessions_dir / "new.jsonl"
    old.write_text("{}")
    new.write_text("{}")
    os.utime(old, (1, 1))
    os.utime(new, (2, 2))  # new is the newest
    monkeypatch.setattr(session, "_held_open", lambda p: True)
    assert session._is_active_session(old) is True


def test_held_open_none_without_lsof(monkeypatch):
    monkeypatch.setattr(session.shutil, "which", lambda name: None)
    assert session._held_open(Path("/tmp/x.jsonl")) is None


def test_held_open_true_on_pids_false_on_empty(monkeypatch):
    monkeypatch.setattr(session.shutil, "which", lambda name: "/usr/bin/lsof")

    class _Proc:
        def __init__(self, out):
            self.stdout = out

    monkeypatch.setattr(session.subprocess, "run", lambda *a, **k: _Proc("123\n456\n"))
    assert session._held_open(Path("/tmp/x.jsonl")) is True
    monkeypatch.setattr(session.subprocess, "run", lambda *a, **k: _Proc(""))
    assert session._held_open(Path("/tmp/x.jsonl")) is False


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


def test_pod_alive_true_when_session_running(monkeypatch):
    monkeypatch.setattr(
        session.terminal_api, "get_session", lambda pid: {"id": "s", "running": True}
    )
    assert session._pod_alive(1) is True


def test_pod_alive_false_when_terminal_idle(monkeypatch):
    # GET /terminal/session answers 200 for an idle terminal too, with a
    # running=false descriptor — that is a DEAD pod, not a live one.
    monkeypatch.setattr(
        session.terminal_api,
        "get_session",
        lambda pid: {"sessionId": None, "running": False},
    )
    assert session._pod_alive(1) is False


def test_pod_alive_false_on_definitive_none(monkeypatch):
    monkeypatch.setattr(session.terminal_api, "get_session", lambda pid: None)
    assert session._pod_alive(1) is False


def test_pod_alive_failsafe_alive_on_error(monkeypatch):
    def boom(pid):
        raise RuntimeError("terminal feature disabled")

    monkeypatch.setattr(session.terminal_api, "get_session", boom)
    # Unknown liveness must not silently authorise a steal.
    assert session._pod_alive(1) is True


def test_stop_session_force_stops_via_delete_on_terminal_root(monkeypatch):
    seen = {}

    class _Client:
        def _send_request(self, method, path_params, **kwargs):
            seen["method"] = method
            seen["path"] = path_params

    monkeypatch.setattr(session.terminal_api.client, "_get_instance", lambda: _Client())
    session.terminal_api.stop_session(119)
    assert seen["method"] == "DELETE"
    assert seen["path"] == ["project", 119, "terminal"]


# --- teleport root / manifest ------------------------------------------------

# The per-user private HopsFS home the transcripts are staged under.
_ROOT = "Users/lex/teleport"


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
            # Same shape the real DatasetApi raises for a missing path, so the
            # CLI's absent-vs-failure split is what actually gets tested.
            raise _rest_error(404)
        Path(local_path).write_text(json.dumps(self._files[remote]))


def test_teleport_root_is_the_users_private_home(monkeypatch):
    class _Client:
        _username = "lex"

    monkeypatch.setattr(session.client, "_get_instance", lambda: _Client())
    assert session._teleport_root() == "Users/lex/teleport"


def test_teleport_root_falls_back_to_hdfs_user_inside_a_pod(monkeypatch):
    # The internal client (terminal pod / job) has no _username; the username is
    # the tail of the project__username hdfs identity in the env.
    class _InternalClient:
        pass

    monkeypatch.setattr(session.client, "_get_instance", lambda: _InternalClient())
    monkeypatch.delenv("HDFS_USER", raising=False)
    monkeypatch.setenv("HADOOP_USER_NAME", "myproj__lex")
    assert session._teleport_root() == "Users/lex/teleport"


def test_teleport_root_errors_without_username(monkeypatch):
    class _Client:
        _username = None

    monkeypatch.setattr(session.client, "_get_instance", lambda: _Client())
    monkeypatch.delenv("HADOOP_USER_NAME", raising=False)
    monkeypatch.delenv("HDFS_USER", raising=False)
    with pytest.raises(Exception, match="username"):
        session._teleport_root()


def test_build_manifest_carries_mode_and_cwd(monkeypatch):
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: Path("/Users/lex/p")))
    m = session._build_manifest("sid1", "-Users-lex-p", "new", None)
    assert m["session_id"] == "sid1"
    assert m["slug"] == "-Users-lex-p"
    assert m["mode"] == "new"
    assert m["model"] is None
    assert m["prompt"] is None
    # str(Path.cwd()); rendered with the platform separator (backslashes on
    # Windows), so compare against the same rather than a hardcoded POSIX path.
    assert m["cwd"] == str(Path("/Users/lex/p"))
    # The owner filter is gone: isolation is structural (per-user 0700 home), so
    # the manifest no longer carries a `user` field.
    assert "user" not in m


def test_build_manifest_carries_prompt():
    m = session._build_manifest("s", "-p", "new", None, "summarize the project")
    assert m["prompt"] == "summarize the project"


def test_upload_manifest_uploads_last_write_to_dest(tmp_path):
    seen: dict = {}

    class _DS:
        def upload(self, local_path, upload_path, overwrite):
            seen["upload_path"] = upload_path
            seen["overwrite"] = overwrite
            seen["content"] = json.loads(Path(local_path).read_text())

    session._upload_manifest(_DS(), f"{_ROOT}/slug", "sid", {"mode": "new"})
    assert seen["upload_path"] == f"{_ROOT}/slug"
    assert seen["overwrite"] is True
    assert seen["content"] == {"mode": "new"}


def test_upload_manifest_raises_on_failure():
    class _DS:
        def upload(self, **kwargs):
            raise RuntimeError("nope")

    with pytest.raises(Exception, match="Failed to upload teleport manifest"):
        session._upload_manifest(_DS(), "dest", "sid", {"a": 1})


def _teleport_tree():
    dirs = {
        _ROOT: [f"{_ROOT}/-Users-lex-a", f"{_ROOT}/-Users-lex-b/"],
        f"{_ROOT}/-Users-lex-a": [f"{_ROOT}/-Users-lex-a/s1.jsonl"],
        f"{_ROOT}/-Users-lex-b": [f"{_ROOT}/-Users-lex-b/s2.jsonl"],
    }
    files = {f"{_ROOT}/-Users-lex-b/s2.teleport.json": {"cwd": "/Users/lex/b"}}
    return _FakeDataset(dirs, files)


@pytest.fixture
def _fixed_root(monkeypatch):
    """Pin ``_teleport_root`` so the dataset helpers need no live client."""
    monkeypatch.setattr(session, "_teleport_root", lambda: _ROOT)


def test_scan_slugs_lists_subdirs(_fixed_root):
    assert session._scan_slugs(_teleport_tree()) == ["-Users-lex-a", "-Users-lex-b"]


def test_locate_session_finds_slug_and_origin_cwd(_fixed_root):
    ds = _teleport_tree()
    assert session._locate_session(ds, "s2") == ("-Users-lex-b", "/Users/lex/b")
    # A session with no manifest still resolves its slug (cwd unknown).
    assert session._locate_session(ds, "s1") == ("-Users-lex-a", None)


def test_locate_session_none_when_absent(_fixed_root):
    assert session._locate_session(_teleport_tree(), "missing") is None


def test_validate_session_id_accepts_well_formed():
    assert session._validate_session_id("abc-123_DEF") == "abc-123_DEF"


@pytest.mark.parametrize(
    "bad",
    ["../etc/passwd", "a/b", "/abs/path", "has space", "dot.name", ""],
)
def test_validate_session_id_rejects_unsafe(bad):
    import click

    with pytest.raises(click.ClickException):
        session._validate_session_id(bad)


# --- hidden failures must surface --------------------------------------------


def _rest_error(status: int):
    """A RestAPIError carrying only the pieces the CLI inspects."""
    from hopsworks_common.client.exceptions import RestAPIError

    class _Resp:
        status_code = status
        reason = "err"
        content = b""

        def json(self):
            raise ValueError

    return RestAPIError("http://cluster/x", _Resp())


def test_remote_session_ids_missing_dir_is_empty(_fixed_root):
    class _DS:
        def list(self, path):
            raise _rest_error(400)

    assert session._remote_session_ids(_DS(), "-slug") == []


def test_remote_session_ids_real_failure_surfaces(_fixed_root):
    # An auth/network failure must not read as "nothing staged".
    class _DS:
        def list(self, path):
            raise _rest_error(500)

    with pytest.raises(Exception, match="500"):
        session._remote_session_ids(_DS(), "-slug")


def test_scan_slugs_missing_root_is_empty(_fixed_root):
    class _DS:
        def list(self, path):
            raise _rest_error(404)

    assert session._scan_slugs(_DS()) == []


def test_scan_slugs_real_failure_surfaces(_fixed_root):
    class _DS:
        def list(self, path):
            raise _rest_error(500)

    with pytest.raises(Exception, match="500"):
        session._scan_slugs(_DS())


def test_read_baton_absent_is_none():
    class _DS:
        def download(self, remote, local_path, overwrite=False):
            raise _rest_error(404)

    assert session._read_baton(_DS(), "dest", "sid") is None


def test_read_baton_real_failure_fails_closed():
    # A network/auth error must not degrade to "no baton": batonless copy
    # semantics would bypass the ownership gate the baton enforces.
    class _DS:
        def download(self, remote, local_path, overwrite=False):
            raise _rest_error(500)

    with pytest.raises(Exception, match="baton"):
        session._read_baton(_DS(), "dest", "sid")


def test_read_baton_corrupt_fails_closed(tmp_path):
    class _DS:
        def download(self, remote, local_path, overwrite=False):
            Path(local_path).write_text("{not json")

    with pytest.raises(Exception, match="unreadable"):
        session._read_baton(_DS(), "dest", "sid")


# --- push golden output -------------------------------------------------------


class _PushDataset:
    """Dataset stub for push: records uploads, serves JSON sidecar downloads.

    ``ack_on_manifest`` simulates the pod's landing hook: uploading the
    ``<id>.teleport.json`` manifest makes a ``<id>.landed.json`` ack appear
    that echoes the manifest's ``pushed_at``. Acks are never deleted, so a
    stale pre-push ack (different pushed_at) must not satisfy the landing poll.
    """

    def __init__(self, files: dict[str, dict], ack_on_manifest: bool = False):
        self._files = files
        self._ack_on_manifest = ack_on_manifest
        self.uploads: list[str] = []
        self.removed: list[str] = []

    def mkdir(self, path: str) -> None:
        pass

    def upload(self, local_path, upload_path, overwrite=False) -> None:
        name = Path(local_path).name
        self.uploads.append(name)
        self._files[f"{upload_path}/{name}"] = Path(local_path).read_text()
        if self._ack_on_manifest and name.endswith(".teleport.json"):
            sid = name[: -len(".teleport.json")]
            manifest = json.loads(Path(local_path).read_text())
            self._files[f"{upload_path}/{sid}.landed.json"] = {
                "landed_at": "now",
                "pushed_at": manifest["pushed_at"],
            }

    def remove(self, path: str) -> None:
        self.removed.append(path)
        self._files.pop(path, None)

    def download(self, remote, local_path, overwrite=False) -> None:
        if remote not in self._files:
            raise _rest_error(404)
        value = self._files[remote]
        Path(local_path).write_text(
            value if isinstance(value, str) else json.dumps(value)
        )

    def list(self, path: str) -> list[str]:
        return [p for p in self._files if p.startswith(path + "/")]


class _FakeProject:
    id = 7
    name = "demo"

    def __init__(self, ds):
        self._ds = ds

    def get_dataset_api(self):
        return self._ds


def _push_setup(tmp_path, monkeypatch, landed: bool):
    """Wire a full fake push environment; return (runner, dataset, slug)."""
    from click.testing import CliRunner

    workdir = tmp_path / "proj"
    workdir.mkdir()
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: workdir))
    slug = session._cwd_slug()

    claude_root = tmp_path / "claude"
    slug_dir = claude_root / slug
    slug_dir.mkdir(parents=True)
    jsonl = slug_dir / "sid1.jsonl"
    jsonl.write_text('{"line": 1}\n')
    import os

    os.utime(jsonl, (1, 1))  # stale: not the session we are in
    monkeypatch.setattr(session, "_CLAUDE_PROJECTS", claude_root)
    monkeypatch.setattr(session, "_held_open", lambda p: False)

    monkeypatch.setattr(session, "_teleport_root", lambda: _ROOT)
    # Keep the push tests off the real repo/prefs: git sync has its own tests.
    monkeypatch.setattr(session.git_sync, "maybe_collect", lambda *a, **k: None)
    # A stale ack from a previous land is always present (acks are never
    # deleted); only a fresh ack echoing THIS push's pushed_at (ack_on_manifest)
    # may satisfy the landing poll.
    files = {
        f"{_ROOT}/{slug}/sid1.landed.json": {
            "landed_at": "stale",
            "pushed_at": "1999-01-01T00:00:00+00:00",
        }
    }
    ds = _PushDataset(files, ack_on_manifest=landed)
    monkeypatch.setattr(session.conn, "get_project", lambda ctx: _FakeProject(ds))
    monkeypatch.setattr(
        session.terminal_api, "start_session", lambda pid: {"wsUrl": "/terminal/ws"}
    )
    monkeypatch.setattr(session, "_open_browser", lambda url: True)
    monkeypatch.setattr(session.time, "sleep", lambda s: None)

    class _Client:
        _base_url = "https://hops:8182"

    monkeypatch.setattr(session.client, "_get_instance", lambda: _Client())
    return CliRunner(), ds, slug


def _all_output(result) -> str:
    try:
        return result.output + result.stderr
    except ValueError:  # stderr mixed into output on this click version
        return result.output


def test_push_happy_path_is_three_check_lines(tmp_path, monkeypatch):
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    result = runner.invoke(session.session_group, ["push"], catch_exceptions=False)
    assert result.exit_code == 0
    text = _all_output(result)
    assert "✓ Pushed session sid1 to demo" in text
    assert "✓ Terminal pod ready for demo" in text
    assert "✓ Landed on pod demo" in text
    # The landed ack makes the manual kit and the old noise unnecessary.
    assert "mkdir -p" not in text
    assert "Push marker" not in text
    assert "WebSocket" not in text
    assert slug not in text  # human output never leaks the raw slug
    assert "sid1.jsonl" in ds.uploads
    assert "sid1.teleport.json" in ds.uploads


_PUSHED = "2026-09-05T08:00:00+00:00"


def _with_pod_baton(ds, slug, alive, monkeypatch, landed=True):
    """Stage a pod-held baton and its manifest, plus the landing ack when landed.

    Also pins the pod's liveness.
    """
    ds._files[f"{_ROOT}/{slug}/sid1.baton.json"] = {
        "session_id": "sid1",
        "holder": "pod:demo",
        "transferred_lines": 1,
    }
    ds._files[f"{_ROOT}/{slug}/sid1.teleport.json"] = {
        "session_id": "sid1",
        "pushed_at": _PUSHED,
    }
    if landed:
        ds._files[f"{_ROOT}/{slug}/sid1.landed.json"] = {
            "landed_at": "then",
            "pushed_at": _PUSHED,
        }
    monkeypatch.setattr(session, "_pod_alive", lambda pid: alive)


def test_push_refuses_when_a_live_pod_holds_the_baton(tmp_path, monkeypatch):
    # The pod's copy is canonical and may hold un-pulled work; a re-push would
    # overwrite the transport and, on land, the pod's live transcript.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    _with_pod_baton(ds, slug, alive=True, monkeypatch=monkeypatch)
    result = runner.invoke(session.session_group, ["push"])
    assert result.exit_code != 0
    assert "live pod holds this session" in _all_output(result)
    assert "sid1.jsonl" not in ds.uploads  # nothing was overwritten


def test_push_force_overrides_the_live_pod_gate(tmp_path, monkeypatch):
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    _with_pod_baton(ds, slug, alive=True, monkeypatch=monkeypatch)
    result = runner.invoke(
        session.session_group, ["push", "--force"], catch_exceptions=False
    )
    assert result.exit_code == 0
    assert "sid1.jsonl" in ds.uploads


def test_push_proceeds_when_the_pod_holder_is_dead(tmp_path, monkeypatch):
    # A dead pod cannot hold un-pulled work worth protecting.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    _with_pod_baton(ds, slug, alive=False, monkeypatch=monkeypatch)
    result = runner.invoke(session.session_group, ["push"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "sid1.jsonl" in ds.uploads


def test_push_proceeds_when_the_live_pod_never_landed_the_push(tmp_path, monkeypatch):
    # The baton names the pod from the moment of the push, but the pod lands
    # only once a Terminal tab has opened a shell in it. Until then it holds
    # nothing worth protecting, and the stale ack of an older land (always
    # present in the fixture) must not count as evidence either.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    _with_pod_baton(ds, slug, alive=True, monkeypatch=monkeypatch, landed=False)
    result = runner.invoke(session.session_group, ["push"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "sid1.jsonl" in ds.uploads


def test_push_keeps_the_gate_when_the_manifest_cannot_be_read(tmp_path, monkeypatch):
    # Without the manifest there is no way to tell whether the pod landed the
    # push, so the gate stays closed rather than guessing.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    _with_pod_baton(ds, slug, alive=True, monkeypatch=monkeypatch, landed=False)
    del ds._files[f"{_ROOT}/{slug}/sid1.teleport.json"]
    result = runner.invoke(session.session_group, ["push"])
    assert result.exit_code != 0
    assert "sid1.jsonl" not in ds.uploads


def test_push_twice_is_idempotent(tmp_path, monkeypatch):
    # The first push renames the local transcript aside; the second finds it
    # there, ships the same content over the staged copy without any flag, and
    # leaves the local state as it was. The pod has not landed the push (no
    # Terminal tab was opened), which is the case a repeat must not refuse.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=False)
    slug_dir = tmp_path / "claude" / slug
    for _ in range(2):
        result = runner.invoke(session.session_group, ["push"], catch_exceptions=False)
        assert result.exit_code == 0, _all_output(result)
    assert ds.uploads.count("sid1.jsonl") == 2
    assert sorted(p.name for p in slug_dir.iterdir()) == [
        "sid1.away.json",
        "sid1.jsonl.away",
    ]


def test_push_refuses_to_clobber_a_staged_copy_that_advanced(tmp_path, monkeypatch):
    # Lines the staged copy has and this machine lacks are work (a pod's
    # synced-back appends, another machine's push) the overwrite would destroy.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    ds._files[f"{_ROOT}/{slug}/sid1.jsonl"] = '{"line": 1}\n{"line": 2}\n'
    result = runner.invoke(session.session_group, ["push"])
    assert result.exit_code != 0
    text = _all_output(result)
    assert "1 line(s) this machine does not" in text and "hops session pull" in text
    assert "sid1.jsonl" not in ds.uploads

    result = runner.invoke(
        session.session_group, ["push", "--force"], catch_exceptions=False
    )
    assert result.exit_code == 0
    assert "sid1.jsonl" in ds.uploads


def test_push_writes_the_baton_after_the_manifest(tmp_path, monkeypatch):
    # A failed manifest upload must never leave the store naming a pod holder
    # for a session that pod will never receive.
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    result = runner.invoke(session.session_group, ["push"], catch_exceptions=False)
    assert result.exit_code == 0
    assert ds.uploads.index("sid1.teleport.json") < ds.uploads.index("sid1.baton.json")


def test_push_prints_landing_kit_when_not_landed(tmp_path, monkeypatch):
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=False)
    # SESSION_ID is positional now.
    result = runner.invoke(
        session.session_group, ["push", "sid1"], catch_exceptions=False
    )
    assert result.exit_code == 0
    text = _all_output(result)
    assert "Not landed yet" in text
    assert "Terminal tab is open" in text
    # The URL is always shown, opened or not: the user may need the tab that
    # does the landing.
    assert "https://hops:8182/p/7?terminal=open" in text
    assert "claude --resume sid1" in text
    assert "Push marker" not in text


def _pull_setup(tmp_path, monkeypatch, alive: bool, landed: bool):
    """Wire a fake store holding one pushed session; return (runner, ds, slug, root)."""
    from click.testing import CliRunner

    workdir = tmp_path / "proj"
    workdir.mkdir()
    monkeypatch.setattr(Path, "cwd", classmethod(lambda cls: workdir))
    slug = session._cwd_slug()
    claude_root = tmp_path / "claude"
    monkeypatch.setattr(session, "_CLAUDE_PROJECTS", claude_root)
    monkeypatch.setattr(session, "_teleport_root", lambda: _ROOT)
    ds = _PushDataset({f"{_ROOT}/{slug}/sid1.jsonl": '{"line": 1}\n'})
    _with_pod_baton(ds, slug, alive=alive, monkeypatch=monkeypatch, landed=landed)
    monkeypatch.setattr(session.conn, "get_project", lambda ctx: _FakeProject(ds))

    class _Client:
        _base_url = "https://hops:8182"

    monkeypatch.setattr(session.client, "_get_instance", lambda: _Client())
    return CliRunner(), ds, slug, claude_root


def test_pull_refuses_while_a_live_pod_has_landed_the_session(tmp_path, monkeypatch):
    runner, ds, slug, _ = _pull_setup(tmp_path, monkeypatch, alive=True, landed=True)
    result = runner.invoke(session.session_group, ["pull"])
    assert result.exit_code != 0
    assert "has landed this session" in _all_output(result)
    assert "sid1.baton.json" not in ds.uploads


def test_pull_reclaims_a_push_the_live_pod_never_landed(tmp_path, monkeypatch):
    # A running terminal pod nobody opened a tab for has not landed the push,
    # so the reclaim needs no --force and the store must record the hand-back.
    runner, ds, slug, root = _pull_setup(
        tmp_path, monkeypatch, alive=True, landed=False
    )
    result = runner.invoke(session.session_group, ["pull"], catch_exceptions=False)
    assert result.exit_code == 0, _all_output(result)
    assert (root / slug / "sid1.jsonl").read_text() == '{"line": 1}\n'
    baton = json.loads(ds._files[f"{_ROOT}/{slug}/sid1.baton.json"])
    assert baton["holder"].startswith("laptop:")
    # Consumed with the push's own stamp, or the next Terminal tab would
    # boot-land the reclaimed copy as a ghost tab.
    assert ds._files[f"{_ROOT}/{slug}/sid1.teleport.json.consumed"] == _PUSHED


def test_pull_twice_is_idempotent(tmp_path, monkeypatch):
    runner, ds, slug, root = _pull_setup(
        tmp_path, monkeypatch, alive=False, landed=True
    )
    for _ in range(2):
        result = runner.invoke(session.session_group, ["pull"], catch_exceptions=False)
        assert result.exit_code == 0, _all_output(result)
    assert (root / slug / "sid1.jsonl").read_text() == '{"line": 1}\n'
    baton = json.loads(ds._files[f"{_ROOT}/{slug}/sid1.baton.json"])
    assert baton["holder"].startswith("laptop:")
    assert ds._files[f"{_ROOT}/{slug}/sid1.teleport.json.consumed"] == _PUSHED


def test_pull_force_from_a_landed_live_pod_leaves_the_consumed_marker_to_the_pod(
    tmp_path, monkeypatch
):
    # The pod wrote that marker itself at land time; overwriting a name it has
    # cached is the lost-write trap the other markers avoid.
    runner, ds, slug, _ = _pull_setup(tmp_path, monkeypatch, alive=True, landed=True)
    result = runner.invoke(
        session.session_group, ["pull", "--force"], catch_exceptions=False
    )
    assert result.exit_code == 0, _all_output(result)
    assert f"{_ROOT}/{slug}/sid1.teleport.json.consumed" not in ds._files


def _reset_setup(tmp_path, monkeypatch, staged: bool, remembered: bool, running: bool):
    """Wire a fake store with (or without) staged keys; return (runner, ds, forgot)."""
    from click.testing import CliRunner

    root = _ROOT.rsplit("/", 1)[0]
    files = (
        {f"{root}/.ssh/id_ed25519": "k", f"{root}/.ssh/id_ed25519.pub": "ssh-ed25519 A"}
        if staged
        else {}
    )
    ds = _PushDataset(files)
    monkeypatch.setattr(session, "_teleport_root", lambda: _ROOT)
    monkeypatch.setattr(session.conn, "get_project", lambda ctx: _FakeProject(ds))
    monkeypatch.setattr(
        session.git_sync, "_prefs", lambda: {"answer": "always"} if remembered else {}
    )
    forgot = []
    monkeypatch.setattr(
        session.git_sync, "forget_prefs", lambda: forgot.append(True) or remembered
    )
    monkeypatch.setattr(
        session.terminal_api, "get_session", lambda pid: {"running": running}
    )
    return CliRunner(), ds, forgot


def test_reset_forgets_the_choice_and_removes_the_staged_keys(tmp_path, monkeypatch):
    runner, ds, forgot = _reset_setup(
        tmp_path, monkeypatch, staged=True, remembered=True, running=True
    )
    result = runner.invoke(
        session.session_group, ["reset", "--yes"], catch_exceptions=False
    )
    assert result.exit_code == 0, _all_output(result)
    text = _all_output(result)
    assert sorted(Path(p).name for p in ds.removed) == ["id_ed25519", "id_ed25519.pub"]
    assert forgot == [True]
    assert "Forgot the git sync choice" in text
    # A running pod keeps the key it copied, so the user is told how to refresh.
    assert "hops session stop" in text


def test_reset_with_nothing_remembered_or_staged_is_a_noop(tmp_path, monkeypatch):
    runner, ds, forgot = _reset_setup(
        tmp_path, monkeypatch, staged=False, remembered=False, running=False
    )
    result = runner.invoke(
        session.session_group, ["reset", "--yes"], catch_exceptions=False
    )
    assert result.exit_code == 0
    assert "Nothing to reset" in _all_output(result)
    assert ds.removed == [] and forgot == []


def test_reset_asks_before_touching_anything(tmp_path, monkeypatch):
    runner, ds, forgot = _reset_setup(
        tmp_path, monkeypatch, staged=True, remembered=True, running=False
    )
    result = runner.invoke(session.session_group, ["reset"], input="n\n")
    assert result.exit_code != 0
    assert ds.removed == [] and forgot == []


@pytest.mark.parametrize(
    ("opened", "lead"), [(True, "Opening the terminal"), (False, "Open the terminal")]
)
def test_the_terminal_url_is_shown_whatever_the_browser_said(
    tmp_path, monkeypatch, opened, lead
):
    runner, ds, slug = _push_setup(tmp_path, monkeypatch, landed=True)
    monkeypatch.setattr(session, "_open_browser", lambda url: opened)
    result = runner.invoke(session.session_group, ["push"], catch_exceptions=False)
    text = _all_output(result)
    assert lead in text and "https://hops:8182/p/7?terminal=open" in text


def test_open_browser_trusts_xdg_open_exit_status_not_a_started_process(monkeypatch):
    calls = []

    class _Done:
        def __init__(self, rc):
            self.returncode = rc

    monkeypatch.setattr(session.shutil, "which", lambda name: "/usr/bin/xdg-open")
    monkeypatch.setattr(
        session.subprocess, "run", lambda *a, **k: calls.append(a[0]) or _Done(3)
    )
    assert session._open_browser("https://h/p/1?terminal=open") is False
    assert calls == [["/usr/bin/xdg-open", "https://h/p/1?terminal=open"]]
