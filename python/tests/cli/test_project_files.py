"""Project files go through one rule: the sandbox's mount when it is up, else `hops files`."""

from __future__ import annotations

from pathlib import Path

import pytest
from hopsworks.cli import scaffold


TEMPLATES = Path(scaffold.__file__).resolve().parent / "templates"


REPO_SKILL = Path(scaffold.__file__).resolve().parents[3] / ".claude" / "skills" / "hops" / "SKILL.md"


def read(name: str) -> str:
    return " ".join((TEMPLATES / name).read_text(encoding="utf-8").split())


def project_files(skill: str) -> str:
    rule = skill[skill.index("## Project files") :]
    return rule[: rule.index(" ## ")] if " ## " in rule else rule


def test_the_repository_skill_carries_the_same_rule():
    repo = " ".join(REPO_SKILL.read_text(encoding="utf-8").split())
    assert project_files(repo) == project_files(read("SKILL.md"))


def test_the_rule_trusts_only_a_mount_of_this_project_that_is_up():
    rule = project_files(read("SKILL.md"))
    assert "cat /run/hopsfs/status.json" in rule
    assert '"state": "mounted"' in rule
    assert '"mountpoint": "/Projects/$HOPSWORKS_PROJECT"' in rule
    assert "Any other state, no such file, or another mountpoint: use `hops files`" in rule
    assert "A `/Projects` directory alone is not a mount" in rule


@pytest.mark.parametrize(
    "playbook",
    ["hops.md", "hops-ml.md", "hops-app-builder.md", "hops-silver.md", "hops-gold.md"],
)
def test_each_playbook_points_to_the_rule_without_copying_it(playbook):
    text = read(playbook)
    assert "the **hops** skill's *Project files* rule" in text
    assert "status.json" not in text
