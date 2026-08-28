"""Unit tests for the git-sync gates and consent flow (``hops session push``)."""

from __future__ import annotations

from pathlib import Path

from hopsworks.cli import git_sync


def test_is_ssh_url():
    assert git_sync._is_ssh_url("git@github.com:org/repo.git") is True
    assert git_sync._is_ssh_url("ssh://git@github.com/org/repo.git") is True
    assert git_sync._is_ssh_url("ssh://git@host:2222/org/repo.git") is True
    assert git_sync._is_ssh_url("https://github.com/org/repo.git") is False
    assert git_sync._is_ssh_url("http://host/repo.git") is False
    assert git_sync._is_ssh_url("/srv/git/repo.git") is False


def test_ssh_host_parsing():
    assert git_sync._ssh_host("git@github.com:org/repo.git") == "github.com"
    assert git_sync._ssh_host("ssh://git@example.com/org/repo.git") == "example.com"
    assert git_sync._ssh_host("ssh://ubuntu@10.0.0.5:2222/tmp/x.git") == "10.0.0.5"


def _state(url="git@github.com:org/repo.git"):
    return {
        "root": "/tmp/repo",
        "root_rel_cwd": "",
        "remotes": {"origin": url},
        "remote": "origin",
        "url": url,
        "branch": "main",
        "head": "deadbeef",
    }


def test_not_a_git_dir_is_silent_none(monkeypatch):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: None)
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_https_remote_prints_unsupported(monkeypatch, capsys):
    monkeypatch.setattr(
        git_sync, "_repo_state", lambda: _state("https://github.com/org/repo.git")
    )
    assert git_sync.maybe_collect(object(), "Users/lex") is None
    assert "git sync only supported for ssh key git usage" in capsys.readouterr().err


def test_never_preference_skips_without_prompt(monkeypatch):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(git_sync, "_prefs", lambda: {"answer": "never"})
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_non_interactive_without_always_skips(monkeypatch):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(git_sync, "_prefs", dict)
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: False)
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_passphrase_key_prints_unsupported(monkeypatch, tmp_path, capsys):
    key = tmp_path / "id_rsa"
    key.write_text("locked")
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(
        git_sync, "_prefs", lambda: {"answer": "always", "key_file": str(key)}
    )
    monkeypatch.setattr(git_sync, "_key_passphrase_free", lambda k: False)
    assert git_sync.maybe_collect(object(), "Users/lex") is None
    assert (
        "git sync not supported for passphrase-protected ssh keys"
        in capsys.readouterr().err
    )


def test_always_happy_path_returns_manifest_git(monkeypatch, tmp_path):
    key = tmp_path / "id_rsa"
    key.write_text("open")
    saved = {}
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(
        git_sync, "_prefs", lambda: {"answer": "always", "key_file": str(key)}
    )
    monkeypatch.setattr(git_sync, "_key_passphrase_free", lambda k: True)
    monkeypatch.setattr(
        git_sync, "_save_prefs", lambda **kw: saved.update(kw)
    )
    monkeypatch.setattr(git_sync, "_ensure_clean_and_pushed", lambda s, i: True)
    monkeypatch.setattr(git_sync, "_stage_key", lambda ds, root, k: k.name)

    got = git_sync.maybe_collect(object(), "Users/lex")
    assert got == {
        "root_rel_cwd": "",
        "remotes": {"origin": "git@github.com:org/repo.git"},
        "remote": "origin",
        "branch": "main",
        "head": "deadbeef",
        "key_name": "id_rsa",
    }
    assert saved["key_file"] == str(key)


def test_always_dirty_tree_non_interactive_skips(monkeypatch, tmp_path):
    key = tmp_path / "id_rsa"
    key.write_text("open")
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(
        git_sync, "_prefs", lambda: {"answer": "always", "key_file": str(key)}
    )
    monkeypatch.setattr(git_sync, "_key_passphrase_free", lambda k: True)
    monkeypatch.setattr(git_sync, "_save_prefs", lambda **kw: None)
    monkeypatch.setattr(git_sync, "_ensure_clean_and_pushed", lambda s, i: False)
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_missing_key_file_skips(monkeypatch, tmp_path):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(
        git_sync,
        "_prefs",
        lambda: {"answer": "always", "key_file": str(tmp_path / "absent")},
    )
    monkeypatch.setattr(
        git_sync, "_default_key", lambda host: tmp_path / "also-absent"
    )
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: False)
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_stage_key_skips_upload_when_present(tmp_path, monkeypatch):
    key = tmp_path / "id_ed25519"
    key.write_text("k")
    monkeypatch.setattr(
        git_sync, "_public_key_line", lambda k: "ssh-ed25519 AAAA me@host"
    )

    class _DS:
        uploads = []

        def list(self, path):
            return [f"{path}/id_ed25519", f"{path}/id_ed25519.pub"]

        def download(self, remote, local_path, overwrite=False):
            Path(local_path).write_text("ssh-ed25519 AAAA staged@host")

        def upload(self, local_path, upload_path, overwrite=False):
            self.uploads.append(Path(local_path).name)

    ds = _DS()
    assert git_sync._stage_key(ds, "Users/lex", key) == "id_ed25519"
    assert ds.uploads == []


def test_stage_key_uploads_key_and_pub_when_absent(tmp_path, monkeypatch):
    key = tmp_path / "id_ed25519"
    key.write_text("k")
    monkeypatch.setattr(
        git_sync, "_public_key_line", lambda k: "ssh-ed25519 AAAA me@host"
    )

    class _DS:
        def __init__(self):
            self.uploads = []

        def list(self, path):
            return []

        def mkdir(self, path):
            pass

        def upload(self, local_path, upload_path, overwrite=False):
            self.uploads.append(Path(local_path).name)

    ds = _DS()
    assert git_sync._stage_key(ds, "Users/lex", key) == "id_ed25519"
    assert ds.uploads == ["id_ed25519", "id_ed25519.pub"]
