"""Deployment configuration and filesystem behavior shared by AgentApp and custom servers."""

import os
from pathlib import Path

import pytest
from hopsworks import _skill_catalogue
from hopsworks_agents.protocol import skills as skills_module
from hopsworks_agents.protocol.skills import DeploymentSkills


@pytest.mark.parametrize("value", ["", "  ", ",,", "ALL", " all "])
def test_blank_or_all_env_loads_default_claude_skills(monkeypatch, value):
    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", value)
    skills = DeploymentSkills(framework="claude_agents")
    assert "hops-analytics" in skills.names


def test_normalizes_selection_and_preserves_precedence(monkeypatch):
    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", " hops-analytics, ,")
    assert DeploymentSkills("   ", framework="custom").names == ["hops-analytics"]
    assert DeploymentSkills(("hops-fg",), framework="custom").names == ["hops-fg"]
    assert DeploymentSkills((), framework="custom").names == []
    assert DeploymentSkills(" NONE ", framework="custom").names == []
    with pytest.raises(ValueError, match="Unknown Hopsworks skill"):
        DeploymentSkills("HOPS-ANALYTICS,", framework="custom")


@pytest.mark.parametrize("selection", ["none", "hops-agent-deployment"])
def test_claude_options_keep_project_skills_discoverable(selection):
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

    directory = Path(options["add_dirs"][0]) / ".claude" / "skills"
    assert [p.name for p in directory.iterdir()] == [selection]
    # A Markdown reader can normalize before or after resolving the symlink.
    target = directory / selection / "../../scaling/hops-autoscaling/SKILL.md"
    assert target.is_file()
    assert Path(os.path.normpath(target)).samefile(target)


@pytest.mark.parametrize("failure", ["missing", "tempdir", "link"])
def test_setup_failure_warns_only_for_implicit_selection(
    monkeypatch, tmp_path, caplog, failure
):
    def denied(*args, **kwargs):
        raise PermissionError("read-only filesystem")

    if failure == "missing":
        monkeypatch.setattr(_skill_catalogue, "packaged_skills_dir", lambda: None)
    elif failure == "tempdir":
        monkeypatch.setattr(skills_module, "TemporaryDirectory", denied)
    else:
        monkeypatch.setenv("TMPDIR", str(tmp_path))
        monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
        monkeypatch.setattr(Path, "symlink_to", denied)

    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", "  ")
    skills = DeploymentSkills(framework="claude_agents")
    assert skills.names == skills.claude_dirs == []
    assert "continuing without" in caplog.text

    for selection in ("all", ["hops-analytics"]):
        with pytest.raises(OSError):
            DeploymentSkills(selection, framework="claude_agents")
    monkeypatch.setenv("HOPSWORKS_AGENT_SKILLS", "all")
    with pytest.raises(OSError):
        DeploymentSkills(framework="claude_agents")
    assert not list(tmp_path.iterdir())
