"""Tests for ``hops skills``: skill discovery across both layouts."""

from __future__ import annotations

import json

from click.testing import CliRunner
from hopsworks.cli.main import cli


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
