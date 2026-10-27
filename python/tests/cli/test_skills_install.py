"""Tests for ``hops skills install``: materializing skills into a repository."""

from __future__ import annotations

import json

from click.testing import CliRunner
from hopsworks.cli.commands import skills as skills_mod
from hopsworks.cli.main import cli


def _run(*args):
    return CliRunner().invoke(cli, ["skills", "install", *args])


def test_materializes_every_shipped_skill_flat(tmp_path):
    from hopsworks.cli.commands import skills as skills_cmd

    result = _run("--dir", str(tmp_path))
    assert result.exit_code == 0, result.output

    shipped = skills_cmd._collect_skills(skills_cmd._skills_dir())
    for skill in shipped:
        # Flat, not <bucket>/<name>: an agent looks for <skills>/<name>/SKILL.md.
        assert (tmp_path / ".claude/skills" / skill["name"] / "SKILL.md").is_file()
    assert not (tmp_path / ".claude/skills/ml").exists()


def test_brings_each_skills_supporting_files_along(tmp_path):
    assert _run("--dir", str(tmp_path)).exit_code == 0
    # hops-app links its references/ by relative path from SKILL.md.
    assert (
        tmp_path / ".claude/skills/hops-app/references/monitoring_dashboard.md"
    ).is_file()
    assert (tmp_path / ".claude/skills/hops-eda/scripts/fv-eda.py").is_file()


def test_defaults_to_claude_only(tmp_path):
    assert _run("--dir", str(tmp_path)).exit_code == 0
    assert (tmp_path / ".claude/skills").is_dir()
    for other in (".codex/skills", ".agents/skills", ".opencode/skills"):
        assert not (tmp_path / other).exists()


def test_writes_each_requested_agents_own_directory(tmp_path):
    result = _run("--dir", str(tmp_path), "--agent", "codex", "--agent", "copilot")
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".codex/skills/hops-fg/SKILL.md").is_file()
    assert (tmp_path / ".agents/skills/hops-fg/SKILL.md").is_file()
    assert not (tmp_path / ".claude/skills").exists()


def test_opencode_gets_a_config_pointer(tmp_path):
    assert _run("--dir", str(tmp_path), "--agent", "opencode").exit_code == 0
    config = json.loads((tmp_path / "opencode.json").read_text())
    assert config["skills"]["paths"] == [".opencode/skills"]


def test_opencode_config_is_not_duplicated_or_clobbered(tmp_path):
    (tmp_path / "opencode.json").write_text(
        json.dumps({"model": "some/model", "skills": {"paths": ["./mine"]}})
    )
    assert _run("--dir", str(tmp_path), "--agent", "opencode").exit_code == 0
    assert _run("--dir", str(tmp_path), "--agent", "opencode").exit_code == 0

    config = json.loads((tmp_path / "opencode.json").read_text())
    assert config["model"] == "some/model"
    assert config["skills"]["paths"] == ["./mine", ".opencode/skills"]


def test_rerun_changes_nothing(tmp_path):
    assert _run("--dir", str(tmp_path)).exit_code == 0
    result = _run("--dir", str(tmp_path), "--json")
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)["claude"]
    assert payload["written"] == []
    assert payload["refreshed"] == []
    assert payload["removed"] == []


def test_an_edited_skill_is_kept(tmp_path):
    assert _run("--dir", str(tmp_path)).exit_code == 0
    edited = tmp_path / ".claude/skills/hops-fg/SKILL.md"
    edited.write_text("my own version\n")

    result = _run("--dir", str(tmp_path), "--json")
    assert result.exit_code == 0, result.output
    assert (
        ".claude/skills/hops-fg/SKILL.md" in json.loads(result.stdout)["claude"]["kept"]
    )
    assert edited.read_text() == "my own version\n"


def test_force_replaces_an_edited_skill(tmp_path):
    assert _run("--dir", str(tmp_path)).exit_code == 0
    edited = tmp_path / ".claude/skills/hops-fg/SKILL.md"
    edited.write_text("my own version\n")

    assert _run("--dir", str(tmp_path), "--force").exit_code == 0
    assert edited.read_text(encoding="utf-8") != "my own version\n"


def test_a_withdrawn_skill_is_removed_when_unedited(tmp_path, monkeypatch):
    assert _run("--dir", str(tmp_path)).exit_code == 0
    assert (tmp_path / ".claude/skills/hops-fg/SKILL.md").is_file()

    shipped = skills_mod._read_skills(skills_mod._skills_dir())
    kept = {k: v for k, v in shipped.items() if not k.startswith("hops-fg/")}
    monkeypatch.setattr(skills_mod, "_read_skills", lambda _source: kept)

    result = _run("--dir", str(tmp_path), "--json")
    assert result.exit_code == 0, result.output
    assert not (tmp_path / ".claude/skills/hops-fg").exists()
    assert (tmp_path / ".claude/skills/hops-fv/SKILL.md").is_file()


def test_the_manifest_lives_beside_the_skills_it_records(tmp_path):
    """Per-agent, so `hops setup`'s manifest never sees these as withdrawn."""
    assert _run("--dir", str(tmp_path)).exit_code == 0
    manifest = tmp_path / ".claude/skills" / skills_mod.SKILLS_MANIFEST
    assert manifest.is_file()
    assert not (tmp_path / ".claude/.hops-manifest.json").exists()


def test_setup_and_skills_install_do_not_delete_each_others_files(tmp_path):
    """The two commands share a directory and must not collide.

    `hops setup` writes .claude/skills/hops/SKILL.md; `hops skills install` fills the
    rest of that directory. A shared manifest would make each read the other's
    files as content it had stopped shipping.
    """
    from hopsworks.cli import scaffold

    scaffold.scaffold(tmp_path, scaffold.build_files(internal=True, project="p"))
    assert _run("--dir", str(tmp_path)).exit_code == 0
    scaffold.scaffold(tmp_path, scaffold.build_files(internal=True, project="p"))

    assert (tmp_path / ".claude/skills/hops/SKILL.md").is_file()
    assert (tmp_path / ".claude/skills/hops-fg/SKILL.md").is_file()


def test_sources_the_packaged_skills_not_the_agents_merged_view(tmp_path, monkeypatch):
    """Inside a terminal HOPS_SKILLS_DIR points at a flat merged directory.

    Sourcing from it found nothing and failed the command on a real cluster.
    """
    merged = tmp_path / "agent-skills"
    (merged / "someone-elses").mkdir(parents=True)
    (merged / "someone-elses" / "SKILL.md").write_text("---\nname: x\n---\n")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(merged))

    target = tmp_path / "repo"
    target.mkdir()
    result = _run("--dir", str(target))
    assert result.exit_code == 0, result.output
    assert (target / ".claude/skills/hops-fg/SKILL.md").is_file()
    assert not (target / ".claude/skills/someone-elses").exists()


def test_reads_a_flat_skills_layout_too(tmp_path):
    flat = tmp_path / "flat"
    (flat / "hops-demo").mkdir(parents=True)
    (flat / "hops-demo" / "SKILL.md").write_text("body\n")
    assert list(skills_mod._read_skills(flat)) == ["hops-demo/SKILL.md"]


def test_a_missing_target_directory_is_an_error(tmp_path):
    result = _run("--dir", str(tmp_path / "nope"))
    assert result.exit_code != 0
    assert "does not exist" in result.output


def test_hops_init_still_works_as_a_hidden_deprecated_alias(tmp_path):
    result = CliRunner().invoke(cli, ["init", "--dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".claude/skills/hops-fg/SKILL.md").is_file()
    assert "deprecated" in result.output
    assert "hops skills install" in result.output

    listed = CliRunner().invoke(cli, ["--help"]).output
    assert "skills" in listed
    assert "\n  init" not in listed
