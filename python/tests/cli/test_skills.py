"""Tests for ``hops skills``: skill discovery, and the shipped skills themselves."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from click.testing import CliRunner
from hopsworks.cli.main import cli


SKILLS_ROOT = Path(__file__).resolve().parents[3] / "skills"


SKILL = """---
name: hops-demo
description: Demonstrates a thing.
---

Body.
"""


def _write(root, relative, body=SKILL):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")
    return path


def test_list_reads_the_bucketed_layout(tmp_path, monkeypatch):
    _write(tmp_path, "data/hops-demo/SKILL.md")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))

    result = CliRunner().invoke(cli, ["skills", "list"])

    assert result.exit_code == 0, result.output
    assert "hops-demo" in result.output
    assert "data" in result.output


def test_list_reads_the_flat_layout(tmp_path, monkeypatch):
    # A user's project home holds skills flat, because one level is what a
    # coding agent discovers. Pointed at that directory, the listing has to
    # show what they actually have rather than nothing at all.
    _write(tmp_path, "hops-demo/SKILL.md")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))

    result = CliRunner().invoke(cli, ["skills", "list", "--json"])

    assert result.exit_code == 0, result.output
    skills = json.loads(result.output)
    assert [s["name"] for s in skills] == ["hops-demo"]
    assert skills[0]["bucket"] == ""


@pytest.mark.parametrize(
    "skill_md", sorted(SKILLS_ROOT.glob("*/*/SKILL.md")), ids=lambda p: p.parent.name
)
def test_shipped_skill_frontmatter_loads_in_every_agent(skill_md):
    # Agents disagree about how strict a YAML parser to use, and the lenient
    # ones hide breakage. An unquoted value containing ": " is a nested mapping
    # to a strict parser, so a description like "Knowledge skill: the sidebar"
    # loads under claude and fails the whole file under copilot with
    # "mapping values are not allowed in this context".
    #
    # Checked by rule rather than by parsing, because no YAML library is a hard
    # dependency of this package: PyYAML is only present transitively and only
    # on some interpreter versions, so importing it would make this test pass by
    # being skipped on exactly the runs that matter.
    text = skill_md.read_text(encoding="utf-8")
    match = re.match(r"---\n(.*?)\n---\n", text, re.S)
    assert match, f"{skill_md} has no frontmatter block"

    fields = {}
    for line in match.group(1).split("\n"):
        field = re.match(r"([A-Za-z][\w-]*): (.*)$", line)
        if not field:
            continue
        key, value = field.group(1), field.group(2).strip()
        fields[key] = value
        quoted = len(value) > 1 and value[0] == value[-1] and value[0] in "\"'"
        assert quoted or ": " not in value, (
            f"{skill_md} field {key!r} is an unquoted value containing ': ', "
            f"which a strict YAML parser reads as a nested mapping. Rephrase it "
            f"or quote the value."
        )

    assert fields.get("name"), f"{skill_md} has no name"
    assert fields.get("description"), f"{skill_md} has no description"
    # The directory name is how every agent addresses the skill.
    assert fields["name"] == skill_md.parent.name, (
        f"{skill_md} declares name={fields['name']!r} but lives in "
        f"{skill_md.parent.name!r}"
    )
