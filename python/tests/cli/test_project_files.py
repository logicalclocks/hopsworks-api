"""Project files go through one rule: the sandbox's mount when it is up, else `hops files`."""

from __future__ import annotations

from pathlib import Path

import pytest
from hopsworks.cli import scaffold


TEMPLATES = Path(scaffold.__file__).resolve().parent / "templates"


def read(name: str) -> str:
    return " ".join((TEMPLATES / name).read_text(encoding="utf-8").split())


def test_the_rule_trusts_only_a_mount_of_this_project_that_is_up():
    skill = read("SKILL.md")
    rule = skill[skill.index("## Project files") :]
    rule = rule[: rule.index(" ## ")] if " ## " in rule else rule
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
