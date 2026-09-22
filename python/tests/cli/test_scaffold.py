"""Tests for the agent-instruction template renderer and the scaffolder."""

from __future__ import annotations

import json

import pytest
from hopsworks.cli import scaffold


TEMPLATE = """Shared line.
<!-- hopsworks:only cluster -->
Cluster line for {{PROJECT}}.
<!-- /hopsworks:only -->
<!-- hopsworks:only external -->
External line at {{SDK_PATH}}.
<!-- /hopsworks:only -->
Trailing line.
"""


# region render


def test_render_keeps_only_the_matching_block():
    out = scaffold.render(TEMPLATE, ["cluster"], {"PROJECT": "fraud"})
    assert "Cluster line for fraud." in out
    assert "External line" not in out
    assert out.startswith("Shared line.")
    assert out.endswith("Trailing line.\n")


def test_render_drops_every_block_when_no_tag_matches():
    out = scaffold.render(TEMPLATE, ["other"], {})
    assert out == "Shared line.\nTrailing line.\n"


def test_render_collapses_the_gap_left_by_a_dropped_block():
    assert "\n\n\n" not in scaffold.render(TEMPLATE, [], {})


def test_render_leaves_an_unknown_placeholder_verbatim():
    out = scaffold.render("a {{NOPE}} b\n", [], {})
    assert out == "a {{NOPE}} b\n"


def test_render_does_not_need_variables_for_a_dropped_block():
    out = scaffold.render(TEMPLATE, ["cluster"], {"PROJECT": "p"})
    assert "SDK_PATH" not in out


# endregion
# region scaffold


def test_scaffold_writes_and_records_a_manifest(tmp_path):
    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    assert result.written == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "v1\n"

    manifest = json.loads((tmp_path / scaffold.MANIFEST_PATH).read_text())
    assert manifest["version"] == scaffold.MANIFEST_VERSION
    assert "AGENTS.md" in manifest["files"]


def test_scaffold_refreshes_an_unedited_file(tmp_path):
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v2\n"})
    assert result.refreshed == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "v2\n"


def test_scaffold_keeps_an_edited_file(tmp_path):
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    (tmp_path / "AGENTS.md").write_text("mine\n")

    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v2\n"})
    assert result.kept == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "mine\n"


def test_scaffold_never_reclaims_a_file_after_the_user_edits_it(tmp_path):
    """An edited file must stay the user's across any number of later runs."""
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    (tmp_path / "AGENTS.md").write_text("mine\n")
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v2\n"})

    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v3\n"})
    assert result.kept == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "mine\n"


def test_scaffold_leaves_a_file_it_never_wrote(tmp_path):
    (tmp_path / "AGENTS.md").write_text("pre-existing\n")
    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    assert result.kept == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "pre-existing\n"


def test_scaffold_never_adopts_a_file_it_did_not_write(tmp_path):
    """Two runs over a pre-existing file must both leave it alone.

    Recording the file's own digest when keeping it would make the second run
    read it as ours and overwrite the user's content.
    """
    (tmp_path / "AGENTS.md").write_text("pre-existing\n")
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})

    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    assert result.kept == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "pre-existing\n"


def test_scaffold_never_deletes_a_file_it_did_not_write(tmp_path):
    """A kept file must not become a removal candidate once it stops shipping."""
    (tmp_path / "AGENTS.md").write_text("pre-existing\n")
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})

    result = scaffold.scaffold(tmp_path, {})
    assert result.removed == []
    assert (tmp_path / "AGENTS.md").read_text() == "pre-existing\n"


def test_scaffold_writes_through_a_symlink_only_when_it_owns_the_target(tmp_path):
    """A repo whose AGENTS.md points at its own CLAUDE.md must keep both."""
    (tmp_path / "CLAUDE.md").write_text("the project's own instructions\n")
    (tmp_path / "AGENTS.md").symlink_to("CLAUDE.md")

    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v2\n"})

    assert (tmp_path / "CLAUDE.md").read_text() == "the project's own instructions\n"


def test_scaffold_removes_an_unedited_file_that_is_no_longer_shipped(tmp_path):
    scaffold.scaffold(tmp_path, {".claude/skills/gone/SKILL.md": "old\n"})
    result = scaffold.scaffold(tmp_path, {})
    assert result.removed == [".claude/skills/gone/SKILL.md"]
    assert not (tmp_path / ".claude/skills/gone").exists()


def test_scaffold_keeps_an_edited_file_that_is_no_longer_shipped(tmp_path):
    scaffold.scaffold(tmp_path, {".claude/skills/gone/SKILL.md": "old\n"})
    (tmp_path / ".claude/skills/gone/SKILL.md").write_text("mine\n")

    result = scaffold.scaffold(tmp_path, {})
    assert result.orphaned == [".claude/skills/gone/SKILL.md"]
    assert (tmp_path / ".claude/skills/gone/SKILL.md").read_text() == "mine\n"


def test_scaffold_force_overwrites_an_edited_file(tmp_path):
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    (tmp_path / "AGENTS.md").write_text("mine\n")

    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v2\n"}, force=True)
    assert result.refreshed == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "v2\n"


def test_scaffold_is_idempotent(tmp_path):
    files = {"AGENTS.md": "v1\n"}
    scaffold.scaffold(tmp_path, files)
    result = scaffold.scaffold(tmp_path, files)
    assert not result.changed()


def test_scaffold_degrades_to_create_if_missing_on_a_corrupt_manifest(tmp_path):
    scaffold.scaffold(tmp_path, {"AGENTS.md": "v1\n"})
    (tmp_path / scaffold.MANIFEST_PATH).write_text("not json {")

    result = scaffold.scaffold(tmp_path, {"AGENTS.md": "v2\n"})
    assert result.kept == ["AGENTS.md"]
    assert (tmp_path / "AGENTS.md").read_text() == "v1\n"


# endregion
# region bundles


def test_internal_mode_leaves_the_agent_files_to_the_backend():
    files = scaffold.build_files(internal=True, project="fraud")
    assert scaffold.AGENTS_PATH not in files
    assert set(files) == set(scaffold.CLI_BUNDLE)


def test_external_mode_adds_a_rendered_agents_file():
    files = scaffold.build_files(internal=False, project="fraud")
    agents = files[scaffold.AGENTS_PATH]
    assert "You are connected to the Hopsworks project fraud." in agents
    assert "$HOME/hopsworks-api" not in agents
    assert "{{" not in agents


def test_the_always_loaded_file_does_not_list_the_skills():
    """Skills are discovered from disk by `hops skills install`, not from this file.

    A catalogue here would cost every agent its full size on every start,
    which is what the skill frontmatter already does more cheaply.
    """
    agents = scaffold.build_files(internal=False, project="p")[scaffold.AGENTS_PATH]
    assert "hops-fg" not in agents
    assert len(agents) < 4000


def test_ensure_permission_preserves_other_keys(tmp_path):
    settings = tmp_path / "settings.local.json"
    settings.write_text(
        json.dumps({"permissions": {"allow": ["Bash(git *)"]}, "other": 42})
    )
    assert scaffold.ensure_permission(settings) is True

    data = json.loads(settings.read_text())
    assert data["other"] == 42
    assert "Bash(git *)" in data["permissions"]["allow"]
    assert scaffold.HOPS_PERMISSION in data["permissions"]["allow"]
    assert scaffold.ensure_permission(settings) is False


def test_ensure_permission_rejects_malformed_json(tmp_path):
    settings = tmp_path / "settings.local.json"
    settings.write_text("not json {")
    with pytest.raises(ValueError, match="Could not parse"):
        scaffold.ensure_permission(settings)


# endregion


def test_setup_points_at_skills_install_until_the_skills_are_there(tmp_path, capsys):
    """The materialize step is not guessable from what setup prints."""
    from hopsworks.cli.commands import setup as setup_mod

    setup_mod._suggest_skills_install(tmp_path)
    # output.info writes to stderr.
    assert "hops skills install" in capsys.readouterr().err


def test_setup_stops_pointing_once_the_skills_are_there(tmp_path, capsys):
    from hopsworks.cli.commands import setup as setup_mod
    from hopsworks.cli.commands import skills as skills_mod

    materialized = tmp_path / skills_mod.AGENT_SKILL_DIRS["claude"] / "hops-fg"
    materialized.mkdir(parents=True)

    setup_mod._suggest_skills_install(tmp_path)
    assert "hops skills install" not in capsys.readouterr().err
