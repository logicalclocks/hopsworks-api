import os
import subprocess

import pytest
from hopsworks_agents.eval import agent_source as src


def bundle(files=None, entry="agent/main.py"):
    files = files or {
        "agent/main.py": "from .tools import lookup_customer\nimport agent.memory\n\ndef run():\n    lookup_customer()\n",
        "agent/tools.py": "def lookup_customer(key):\n    return db.find(name=key)  # ignores the key\n",
        "agent/memory.py": "STATE = {}\n",
        "agent/unrelated.py": "def noop():\n    pass\n",
        "README.md": "# The agent\nUses recall_interests for history.\n",
        "tests/test_tools.py": "def test_x(): pass\n",
    }
    return src.SourceBundle(files=files, entry=entry, origin="git https://x/y@main")


class TestReadingADirectory:
    def test_keeps_source_files_and_skips_what_is_not_code(self, tmp_path):
        (tmp_path / "agent").mkdir()
        (tmp_path / "agent" / "main.py").write_text("print(1)\n")
        (tmp_path / "agent" / "data.bin").write_bytes(b"\x00" * 10)
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "config.txt").write_text("ignored\n")
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "__pycache__" / "x.py").write_text("ignored\n")
        (tmp_path / "big.py").write_text("x" * (src.MAX_FILE_BYTES + 1))

        result = src.bundle_from_directory(tmp_path, entry="main.py", origin="test")

        assert set(result.files) == {"agent/main.py"}
        assert result.entry == "agent/main.py"
        assert any("big.py" in s and "too large" in s for s in result.skipped)

    def test_the_entry_is_found_under_a_hopsfs_or_git_relative_name(self):
        files = {"chinook/support_agent.py": "", "chinook/tools.py": ""}
        assert (
            src._resolve_entry(files, "chinook/support_agent.py")
            == "chinook/support_agent.py"
        )
        assert (
            src._resolve_entry(
                files, "/Projects/g1/Models/agent/1/Files/chinook/support_agent.py"
            )
            == "chinook/support_agent.py"
        )
        assert (
            src._resolve_entry(files, "support_agent.py") == "chinook/support_agent.py"
        )
        assert src._resolve_entry(files, "nothing.py") == ""


class TestWhereTheCodeIs:
    def test_comes_off_the_serving_view(self):
        location = src.CodeLocation.from_serving(
            {
                "gitUrl": "https://github.com/o/r",
                "gitBranch": "main",
                "gitResolvedBranch": "release",
                "gitCurrentCommit": "abc123",
                "predictor": "chinook/support_agent.py",
                "modelPath": None,
            }
        )
        assert location.git_url == "https://github.com/o/r"
        assert location.git_branch == "release" and location.git_commit == "abc123"
        assert location.script_file == "chinook/support_agent.py"
        assert location.known()

    def test_an_override_is_a_git_url_with_an_optional_ref_or_a_hopsfs_path(self):
        git = src.CodeLocation.from_override("https://github.com/o/r#v2")
        assert git.git_url == "https://github.com/o/r" and git.git_branch == "v2"
        fs = src.CodeLocation.from_override("/Projects/g1/Models/agent/1")
        assert fs.model_path == "/Projects/g1/Models/agent/1" and not fs.git_url
        assert not src.CodeLocation.from_override("").known()

    def test_the_credential_for_a_host_comes_off_what_hopsworks_injected(self):
        entries = [
            "https://alice:tok1@github.com",
            "https://bob:tok2@gitlab.example.com",
        ]
        assert src._credential_for("https://github.com/o/r", entries) == (
            "alice",
            "tok1",
        )
        assert src._credential_for("https://GitLab.example.com/g/p.git", entries) == (
            "bob",
            "tok2",
        )
        assert src._credential_for("https://bitbucket.org/x/y", entries) == ("", "")


class TestCloning:
    def test_fetches_the_running_commit_shallowly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(src.shutil, "which", lambda name: "/usr/bin/git")
        calls = []

        def run(command, check, capture_output, env=None):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0)

        target = src.clone_repository(
            "https://github.com/o/r", "main", "abc123", str(tmp_path), run=run
        )
        assert target == os.path.join(str(tmp_path), "repo")
        joined = [" ".join(c) for c in calls]
        assert any("fetch -q --depth 1 origin abc123" in c for c in joined)
        assert any("checkout -q FETCH_HEAD" in c for c in joined)

    def test_the_clone_url_never_carries_a_token(self, tmp_path, monkeypatch):
        monkeypatch.setattr(src.shutil, "which", lambda name: "/usr/bin/git")
        monkeypatch.setenv("GIT_CREDENTIALS", "https://alice:secret@github.com")
        monkeypatch.setenv("HOME", str(tmp_path))
        calls = []
        src.clone_repository(
            "https://github.com/o/r",
            "main",
            "",
            str(tmp_path),
            run=lambda command, **kwargs: calls.append(command),
        )
        clone = next(c for c in calls if "clone" in c)
        assert "https://github.com/o/r" in clone
        # the container's credential store, not the url, is what authenticates
        assert not any("secret" in part for part in clone)

    def test_clones_the_branch_tip_when_no_commit_is_recorded(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(src.shutil, "which", lambda name: "/usr/bin/git")
        calls = []
        monkeypatch.delenv("GIT_CREDENTIALS", raising=False)
        src.clone_repository(
            "https://github.com/o/r",
            "dev",
            "",
            str(tmp_path),
            run=lambda command, **kwargs: calls.append(command),
        )
        assert "--branch" in calls[0] and "dev" in calls[0] and "--depth" in calls[0]

    def test_an_unreadable_location_is_an_empty_bundle_with_the_reason(
        self, tmp_path, monkeypatch
    ):
        def boom(*args, **kwargs):
            raise RuntimeError("no network")

        monkeypatch.setattr(src, "clone_repository", boom)
        result = src.load_agent_source(
            src.CodeLocation(git_url="https://github.com/o/r"), workdir=str(tmp_path)
        )
        assert not result
        assert "no network" in result.origin

    def test_an_unknown_location_is_an_empty_bundle(self):
        assert not src.load_agent_source(src.CodeLocation())


class TestChoosingFiles:
    def test_the_entry_and_what_it_imports_come_first(self):
        chosen = [path for path, _ in src.relevant_files(bundle())]
        assert chosen[:3] == ["agent/main.py", "agent/tools.py", "agent/memory.py"]
        # nothing calls for the unrelated module or the tests, so they are not shown
        assert "agent/unrelated.py" not in chosen
        assert "tests/test_tools.py" not in chosen

    def test_files_that_mention_the_tools_called_or_the_failure_are_added(self):
        chosen = [
            path
            for path, _ in src.relevant_files(bundle(), tool_names=["recall_interests"])
        ]
        assert "README.md" in chosen
        chosen = [
            path
            for path, _ in src.relevant_files(bundle(), clues=["noop returned nothing"])
        ]
        assert "agent/unrelated.py" in chosen

    def test_the_budget_clips_and_says_so(self):
        files = {"a.py": "x" * 500, "b.py": "y" * 500}
        chosen = src.relevant_files(
            src.SourceBundle(files=files, entry="a.py"),
            clues=["yyyy"],
            budget_chars=800,
        )
        assert [path for path, _ in chosen] == ["a.py", "b.py"]
        assert chosen[1][1].endswith("[clipped: file continues]")
        assert sum(len(text) for _, text in chosen) <= 800 + 40

    def test_without_an_entry_or_a_match_the_smallest_python_files_stand_in(self):
        chosen = [path for path, _ in src.relevant_files(bundle(entry=""))]
        assert chosen[0] == "agent/memory.py"
        assert all(path.endswith(".py") for path in chosen)

    def test_nothing_is_shown_from_an_empty_bundle(self):
        assert src.relevant_files(src.SourceBundle(files={}), tool_names=["x"]) == []

    def test_rendering_numbers_the_lines_under_each_path(self):
        text = src.render_source(
            [("agent/tools.py", "def f():\n    pass\n")], origin="git x@main"
        )
        assert '<agent_source origin="git x@main">' in text
        assert '<file path="agent/tools.py">' in text
        assert "   1  def f():" in text and "   2      pass" in text
        assert src.render_source([]) == ""


class TestImports:
    def test_resolves_relative_and_absolute_imports_inside_the_repository(self):
        files = bundle().files
        assert src.imported_files(files, "agent/main.py") == [
            "agent/main.py",
            "agent/tools.py",
            "agent/memory.py",
        ]

    def test_ignores_an_entry_that_is_not_in_the_bundle(self):
        assert src.imported_files({"a.py": "import os"}, "missing.py") == []

    @pytest.mark.parametrize(
        "module,expected",
        [
            (".tools", ["agent/tools.py"]),
            ("agent.memory", ["agent/memory.py"]),
            ("os", []),
        ],
    )
    def test_module_candidates(self, module, expected):
        assert (
            src._module_candidates(bundle().files, "agent/main.py", module) == expected
        )
