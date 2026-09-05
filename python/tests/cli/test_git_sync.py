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


def test_https_remote_non_interactive_without_token_skips(monkeypatch, capsys):
    monkeypatch.setattr(
        git_sync, "_repo_state", lambda: _state("https://github.com/org/repo.git")
    )
    monkeypatch.setattr(git_sync, "_prefs", lambda: {"answer": "always"})
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(git_sync.git_cmd, "find_provider", lambda p, h: None)
    assert git_sync.maybe_collect(object(), "Users/lex") is None
    assert "no GitHub token registered for github.com" in capsys.readouterr().err


def test_non_git_remote_is_unsupported(monkeypatch, capsys):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state("/srv/git/repo.git"))
    assert git_sync.maybe_collect(object(), "Users/lex") is None
    assert "git sync needs an SSH or HTTPS remote" in capsys.readouterr().err


def test_url_helpers():
    assert git_sync._is_https_url("https://github.com/o/r.git") is True
    assert git_sync._is_https_url("git@github.com:o/r.git") is False
    assert (
        git_sync._https_host("https://user@gitlab.example:8443/o/r.git")
        == "gitlab.example"
    )
    assert (
        git_sync._ssh_to_https("git@github.com:org/repo.git")
        == "https://github.com/org/repo.git"
    )
    assert (
        git_sync._ssh_to_https("ssh://git@host:2222/org/repo.git")
        == "https://host/org/repo.git"
    )
    assert (
        git_sync._ssh_to_https("https://github.com/org/repo.git")
        == "https://github.com/org/repo.git"
    )


def test_key_generation_unavailable_on_native_windows(monkeypatch):
    monkeypatch.setattr(git_sync.platform, "system", lambda: "Windows")
    monkeypatch.setattr(git_sync.shutil, "which", lambda n: "/usr/bin/ssh-keygen")
    assert git_sync._can_generate_key() is False
    monkeypatch.setattr(git_sync.platform, "system", lambda: "Linux")
    assert git_sync._can_generate_key() is True
    monkeypatch.setattr(git_sync.shutil, "which", lambda n: None)
    assert git_sync._can_generate_key() is False


def test_token_method_rewrites_remotes_to_https_and_ships_no_key(monkeypatch):
    # An SSH remote with the token preference: the pod only has the token, so it
    # gets the HTTPS form of every remote and no key travels.
    state = _state("git@github.com:org/repo.git")
    state["remotes"]["upstream"] = "git@github.com:upstream/repo.git"
    monkeypatch.setattr(git_sync, "_repo_state", lambda: state)
    monkeypatch.setattr(
        git_sync, "_prefs", lambda: {"answer": "always", "method": "token"}
    )
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: False)
    monkeypatch.setattr(
        git_sync.git_cmd,
        "find_provider",
        lambda p, h: type("P", (), {"username": "jim"})(),
    )
    monkeypatch.setattr(git_sync, "_ensure_clean_and_pushed", lambda s, i: True)
    staged = []
    monkeypatch.setattr(git_sync, "_stage_key", lambda *a: staged.append(a))

    got = git_sync.maybe_collect(object(), "Users/lex")
    assert got == {
        "root_rel_cwd": "",
        "remote": "origin",
        "branch": "main",
        "head": "deadbeef",
        "auth": "token",
        "remotes": {
            "origin": "https://github.com/org/repo.git",
            "upstream": "https://github.com/upstream/repo.git",
        },
    }
    assert "key_name" not in got
    assert staged == []


def test_https_remote_registers_token_interactively(monkeypatch):
    monkeypatch.setattr(
        git_sync, "_repo_state", lambda: _state("https://github.com/org/repo.git")
    )
    monkeypatch.setattr(git_sync, "_prefs", lambda: {"answer": "always"})
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(git_sync.git_cmd, "find_provider", lambda p, h: None)
    registered = []
    monkeypatch.setattr(
        git_sync.git_cmd,
        "register_provider",
        lambda p, u, t, h: registered.append((p, u, t, h)),
    )
    answers = iter([True])  # "Register a GitHub token for github.com now?"
    monkeypatch.setattr(git_sync.click, "confirm", lambda *a, **k: next(answers))
    prompts = iter(["jim", "ghp_secret"])
    monkeypatch.setattr(git_sync.click, "prompt", lambda *a, **k: next(prompts))
    monkeypatch.setattr(git_sync, "_ensure_clean_and_pushed", lambda s, i: True)

    got = git_sync.maybe_collect(object(), "Users/lex")
    assert registered == [("GitHub", "jim", "ghp_secret", "github.com")]
    assert got["auth"] == "token"
    assert got["remotes"] == {"origin": "https://github.com/org/repo.git"}


def test_new_key_method_generates_stages_and_offers_gh(monkeypatch, tmp_path):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(git_sync, "_prefs", lambda: {"answer": "always"})
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(git_sync.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(git_sync, "_can_generate_key", lambda: True)
    # menu: choose [2] a new key
    monkeypatch.setattr(git_sync.click, "prompt", lambda *a, **k: "2")
    saved = {}
    monkeypatch.setattr(git_sync, "_save_prefs", lambda **kw: saved.update(kw))

    def fake_generate(path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("private")
        path.with_suffix(".pub").write_text("ssh-ed25519 AAAA hopsworks-teleport")
        return True

    monkeypatch.setattr(git_sync, "_generate_key", fake_generate)
    gh_calls = []
    monkeypatch.setattr(
        git_sync, "_gh_add_key", lambda pub: gh_calls.append(pub) or True
    )
    monkeypatch.setattr(git_sync, "_key_passphrase_free", lambda k: True)
    monkeypatch.setattr(git_sync, "_ensure_clean_and_pushed", lambda s, i: True)
    monkeypatch.setattr(git_sync, "_stage_key", lambda ds, root, k: k.name)

    got = git_sync.maybe_collect(object(), "Users/lex")
    key = tmp_path / ".ssh" / "hopsworks_teleport_ed25519"
    assert got["auth"] == "ssh" and got["key_name"] == key.name
    assert gh_calls == [key.with_suffix(".pub")]
    assert saved["method"] == "ssh" and saved["key_file"] == str(key)


def test_never_preference_skips_without_prompt(monkeypatch):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(git_sync, "_prefs", lambda: {"answer": "never"})
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_non_interactive_without_always_skips(monkeypatch):
    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(git_sync, "_prefs", dict)
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: False)
    assert git_sync.maybe_collect(object(), "Users/lex") is None


def test_consent_default_is_always(monkeypatch):
    """Enter at the consent prompt persists "always"; later pushes stop asking."""
    saved = {}
    seen = {}

    def fake_prompt(text, **kw):
        seen["default"] = kw["default"]
        return kw["default"]

    monkeypatch.setattr(git_sync, "_repo_state", lambda: _state())
    monkeypatch.setattr(git_sync, "_prefs", dict)
    monkeypatch.setattr(git_sync.sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(git_sync.output, "JSON_MODE", False)
    monkeypatch.setattr(git_sync.click, "prompt", fake_prompt)
    monkeypatch.setattr(git_sync, "_save_prefs", lambda **kw: saved.update(kw))
    monkeypatch.setattr(git_sync, "_choose_method", lambda *a: None)
    assert git_sync.maybe_collect(object(), "Users/lex") is None
    assert seen["default"] == "a"
    assert saved == {"answer": "always"}


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
    monkeypatch.setattr(git_sync, "_save_prefs", lambda **kw: saved.update(kw))
    monkeypatch.setattr(git_sync, "_ensure_clean_and_pushed", lambda s, i: True)
    monkeypatch.setattr(git_sync, "_stage_key", lambda ds, root, k: k.name)

    got = git_sync.maybe_collect(object(), "Users/lex")
    assert got == {
        "root_rel_cwd": "",
        "remote": "origin",
        "branch": "main",
        "head": "deadbeef",
        "auth": "ssh",
        "remotes": {"origin": "git@github.com:org/repo.git"},
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
    monkeypatch.setattr(git_sync, "_default_key", lambda host: tmp_path / "also-absent")
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


def test_forget_prefs_drops_only_the_gitsync_table(tmp_path, monkeypatch):
    cfg = tmp_path / "hops.toml"
    cfg.write_text(
        '[default]\nhost = "https://h"\n\n[gitsync]\nanswer = "always"\nmethod = "ssh"\n'
    )
    monkeypatch.setattr(git_sync.config, "CONFIG_PATH", cfg)

    assert git_sync.forget_prefs() is True
    assert git_sync._prefs() == {}
    assert 'host = "https://h"' in cfg.read_text()
    # Nothing left to forget: the second call reports so and changes nothing.
    assert git_sync.forget_prefs() is False


def test_forget_prefs_without_a_config_file_is_nothing_to_forget(tmp_path, monkeypatch):
    monkeypatch.setattr(git_sync.config, "CONFIG_PATH", tmp_path / "absent.toml")
    assert git_sync.forget_prefs() is False


def test_staged_keys_are_listed_and_removed_by_name():
    class _DS:
        files = {"Users/lex/.ssh/id_ed25519", "Users/lex/.ssh/id_ed25519.pub"}

        def list(self, path):
            return sorted(self.files)

        def remove(self, path):
            self.files.remove(path)

    ds = _DS()
    assert git_sync.staged_keys(ds, "Users/lex") == ["id_ed25519", "id_ed25519.pub"]
    assert git_sync.unstage_keys(ds, "Users/lex", ["id_ed25519", "id_ed25519.pub"]) == [
        "id_ed25519",
        "id_ed25519.pub",
    ]
    assert ds.files == set()
