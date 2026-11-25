"""Deployment configuration and filesystem behavior shared by AgentApp and custom servers."""

import re
from pathlib import Path

import pytest
import yaml
from hopsworks import _skill_catalogue
from hopsworks_agents.protocol import skills as skills_module
from hopsworks_agents.protocol.skills import DeploymentSkills


@pytest.mark.parametrize("value", ["", "  ", ",,", "ALL", " all "])
def test_blank_or_all_env_loads_default_claude_skills(
    monkeypatch, value, skill_catalogue
):
    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", value)
    skills = DeploymentSkills(framework="claude_agents")
    assert "orders" in skills.names


def test_normalizes_selection_and_preserves_precedence(monkeypatch, skill_catalogue):
    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", " orders, ,")
    assert DeploymentSkills("   ", framework="custom").names == ["orders"]
    assert DeploymentSkills(("jobs",), framework="custom").names == ["jobs"]
    assert DeploymentSkills((), framework="custom").names == []
    assert DeploymentSkills(" NONE ", framework="custom").names == []
    with pytest.raises(
        ValueError, match="Available packaged skills: jobs, orders, unused"
    ):
        DeploymentSkills("ORDERS,", framework="custom")


@pytest.mark.parametrize("selection", ["none", "orders", "all"])
def test_claude_stages_readable_files_without_tool_grants(skill_catalogue, selection):
    source = skill_catalogue / "data/orders/SKILL.md"
    source.chmod(0o444)
    skills = DeploymentSkills(selection, framework="claude_agents")
    options = skills.claude_options()
    assert options == {
        "setting_sources": ["project"],
        "skills": "all",
        "add_dirs": skills.claude_dirs,
    }
    if selection == "none":
        assert options["add_dirs"] == []
        return

    project = Path(options["add_dirs"][0])
    settings = project / ".claude"
    assert [p.name for p in settings.iterdir()] == ["skills"]
    directory = settings / "skills"
    assert {p.name for p in directory.iterdir()} == set(skills.names)
    assert not any(p.is_symlink() for p in project.rglob("*"))
    for entry in project.rglob("SKILL.md"):
        assert set(yaml.safe_load(entry.read_text().split("---", 2)[1])) == {
            "name",
            "description",
        }
    assert "allowed-tools:" in source.read_text()
    guide = directory / "orders/references/guide.md"
    target = re.search(r"\[Details\]\(([^#]+)#usage\)", guide.read_text())[1]
    assert (guide.parent / target).read_text() == "Job details"
    if selection == "orders":
        assert not list(project.rglob("unused"))
        backlink = (project / "references/jobs/SKILL.md").read_text()
        assert "[Orders](../../.claude/skills/orders/SKILL.md)" in backlink


def test_read_skill_accepts_local_and_linked_references(skill_catalogue):
    skills = DeploymentSkills("orders", framework="custom")
    assert "[Guide]" in skills.read_skill("orders")
    assert "[Details]" in skills.read_skill("orders", "references/guide.md")
    assert "Job details" in skills.read_skill("orders", "../../agents/jobs/details.md")
    for path in (
        "../../agents/unused/SKILL.md",
        "../../../outside",
        str(skill_catalogue.resolve()),
    ):
        with pytest.raises(ValueError):
            skills.read_skill("orders", path)
    with pytest.raises(ValueError, match="not enabled"):
        skills.read_skill("jobs")


def test_packaged_claude_entries_have_no_tool_grants():
    skills = DeploymentSkills("all", framework="claude_agents")
    entries = list(Path(skills.claude_dirs[0]).rglob("SKILL.md"))
    assert len(entries) == len(skills.names) > 0
    for entry in entries:
        assert "allowed-tools" not in yaml.safe_load(
            entry.read_text(encoding="utf-8").split("---", 2)[1]
        )


@pytest.mark.parametrize("failure", ["missing", "tempdir", "copy", "metadata"])
def test_setup_failure_warns_only_for_implicit_selection(
    monkeypatch, tmp_path, caplog, failure, skill_catalogue
):
    def denied(*args, **kwargs):
        raise PermissionError("read-only filesystem")

    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    error = OSError
    if failure == "missing":
        monkeypatch.setattr(_skill_catalogue, "_packaged_skills_dir", lambda: None)
    elif failure == "tempdir":
        monkeypatch.setattr(skills_module, "TemporaryDirectory", denied)
    elif failure == "copy":
        monkeypatch.setattr(skills_module.shutil, "copytree", denied)
    else:
        (skill_catalogue / "data/orders/SKILL.md").write_text(
            "---\nname: orders\n---\n"
        )
        error = ValueError

    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", "  ")
    skills = DeploymentSkills(framework="claude_agents")
    assert skills.names == skills.claude_dirs == []
    assert "continuing without" in caplog.text

    for selection in ("all", ["orders"]):
        with pytest.raises(error):
            DeploymentSkills(selection, framework="claude_agents")
    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", "all")
    with pytest.raises(error):
        DeploymentSkills(framework="claude_agents")
    assert not list(tmp_path.glob("hopsworks-agent-skills-*"))
