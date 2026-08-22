"""Tests for ``hops skills``: layout discovery and the published manifest."""

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


def test_manifest_lists_every_skill_with_a_digest(tmp_path, monkeypatch):
    _write(tmp_path, "data/hops-demo/SKILL.md")
    _write(tmp_path, "ml/hops-other/SKILL.md", SKILL.replace("hops-demo", "hops-other"))
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))

    result = CliRunner().invoke(cli, ["skills", "manifest"])

    assert result.exit_code == 0, result.output
    manifest = json.loads(result.output)
    assert manifest["digest"].startswith("sha256:")
    assert manifest["version"]
    assert [(s["bucket"], s["name"]) for s in manifest["skills"]] == [
        ("data", "hops-demo"),
        ("ml", "hops-other"),
    ]
    assert manifest["skills"][0]["description"] == "Demonstrates a thing."


def test_manifest_digest_tracks_content_and_layout(tmp_path, monkeypatch):
    # The digest is what decides whether a user is offered an upgrade, so it has
    # to move when a skill's text changes and when a skill is renamed or moved.
    _write(tmp_path, "data/hops-demo/SKILL.md")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))
    runner = CliRunner()

    first = json.loads(runner.invoke(cli, ["skills", "manifest"]).output)["digest"]

    _write(tmp_path, "data/hops-demo/SKILL.md", SKILL + "More.\n")
    edited = json.loads(runner.invoke(cli, ["skills", "manifest"]).output)["digest"]
    assert edited != first

    (tmp_path / "data/hops-demo/SKILL.md").rename(tmp_path / "data/SKILL.md")
    (tmp_path / "data/hops-demo").rmdir()
    moved = json.loads(runner.invoke(cli, ["skills", "manifest"]).output)["digest"]
    assert moved != edited


def test_manifest_ignores_a_previous_manifest(tmp_path, monkeypatch):
    # The manifest is published into the tree it describes, so re-running the
    # publish over an already-published directory must produce the same digest
    # rather than a new one every time.
    _write(tmp_path, "data/hops-demo/SKILL.md")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))
    runner = CliRunner()

    first = runner.invoke(cli, ["skills", "manifest"])
    (tmp_path / "MANIFEST.json").write_text(first.output, encoding="utf-8")

    second = runner.invoke(cli, ["skills", "manifest"])
    assert json.loads(second.output)["digest"] == json.loads(first.output)["digest"]


def test_manifest_writes_to_a_file(tmp_path, monkeypatch):
    _write(tmp_path, "data/hops-demo/SKILL.md")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))
    target = tmp_path / "out.json"

    result = CliRunner().invoke(cli, ["skills", "manifest", "--output", str(target)])

    assert result.exit_code == 0, result.output
    assert json.loads(target.read_text())["skills"][0]["name"] == "hops-demo"


def test_manifest_digest_ignores_byte_compiled_helpers(tmp_path, monkeypatch):
    # Some skills ship .py helpers, which installing the SDK byte-compiles in
    # place. If those counted, the digest would differ between a source checkout
    # and a wheel install of identical skills, and between Python versions, so
    # every user would be offered an upgrade to what they already have.
    _write(tmp_path, "ml/hops-demo/SKILL.md")
    (tmp_path / "ml/hops-demo/scripts").mkdir(parents=True)
    (tmp_path / "ml/hops-demo/scripts/helper.py").write_text("print(1)\n")
    monkeypatch.setenv("HOPS_SKILLS_DIR", str(tmp_path))
    runner = CliRunner()

    before = json.loads(runner.invoke(cli, ["skills", "manifest"]).output)["digest"]

    cache = tmp_path / "ml/hops-demo/scripts/__pycache__"
    cache.mkdir()
    (cache / "helper.cpython-312.pyc").write_bytes(b"\x00compiled\x00")

    after = json.loads(runner.invoke(cli, ["skills", "manifest"]).output)["digest"]
    assert after == before

    # The helper itself still counts: it is part of the skill.
    (tmp_path / "ml/hops-demo/scripts/helper.py").write_text("print(2)\n")
    edited = json.loads(runner.invoke(cli, ["skills", "manifest"]).output)["digest"]
    assert edited != before
