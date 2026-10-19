"""Every command name follows hopsworks/cli/ONTOLOGY.md."""

from __future__ import annotations

import re
from pathlib import Path

import click
from hopsworks.cli.deprecations import RENAMED_COMMANDS
from hopsworks.cli.main import cli


_ONTOLOGY = Path(__file__).parents[2] / "hopsworks" / "cli" / "ONTOLOGY.md"


def _section(title):
    text = _ONTOLOGY.read_text()
    body = text.split(f"## {title}\n", 1)[1].split("\n## ", 1)[0]
    return set(re.findall(r"`([a-z-]+)`", body))


def _forbidden():
    return _section("Not used") - _section("Vocabulary")


def _tree():
    root = click.Context(cli, info_name="hops", **cli.context_settings)

    def walk(cmd, path, ctx):
        yield path, cmd
        if isinstance(cmd, click.Group):
            for name in cmd.list_commands(ctx):
                sub = cmd.get_command(ctx, name)
                if sub is not None and not sub.hidden:
                    yield from walk(
                        sub,
                        (*path, name),
                        click.Context(sub, parent=ctx, info_name=name),
                    )

    return list(walk(cli, (), root))


def test_every_leaf_uses_the_vocabulary():
    vocabulary = _section("Vocabulary")
    assert not vocabulary & _forbidden()
    outside = [
        " ".join(path)
        for path, cmd in _tree()
        if path
        and not isinstance(cmd, click.Group)
        and path[:2] != ("datasource", "create")
        and path[-1] not in vocabulary
    ]
    assert not outside, f"Add these to ONTOLOGY.md or rename them: {outside}"


def test_no_group_is_named_by_a_verb():
    verbs = _section("Vocabulary") - {"schema", "status", "sql", "context"}
    groups = [
        " ".join(path)
        for path, cmd in _tree()
        if path and isinstance(cmd, click.Group) and path[-1] in verbs - {"create"}
    ]
    assert not groups


def test_no_command_uses_a_retired_name():
    assert not [path for path, _ in _tree() if path and path[-1] in _forbidden()]


def test_old_names_are_not_registered_twice():
    paths = {path for path, _ in _tree()}
    assert not [old for old in RENAMED_COMMANDS if old in paths]


def _options(cmd):
    return {o for p in cmd.params for o in [*p.opts, *getattr(p, "secondary_opts", [])]}


def _retired_flags():
    body = _ONTOLOGY.read_text().split("## Retired flags\n", 1)[1].split("\n## ", 1)[0]
    # A form with a value (`--wait SECONDS`) is checked by the wait test below.
    return set(re.findall(r"^- `([^` ]+)`:", body, re.M))


def test_no_command_uses_a_retired_flag():
    retired = _retired_flags()
    assert "--n" in retired
    used = [
        (" ".join(path), sorted(_options(cmd) & retired))
        for path, cmd in _tree()
        if _options(cmd) & retired
    ]
    assert not used


def test_no_playbook_or_skill_uses_a_retired_flag():
    root = _ONTOLOGY.parent
    retired = "|".join(re.escape(flag) for flag in _retired_flags())
    pattern = re.compile(rf"hops .*({retired})(?![a-z0-9-])")
    used = [
        f"{path.relative_to(root.parent)}:{number}"
        for folder in (root / "templates", root.parent / "skills")
        for path in folder.rglob("*.md")
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1)
        if pattern.search(line)
    ]
    assert not used


def test_every_wait_is_a_yes_no_switch():
    for path, cmd in _tree():
        for param in cmd.params:
            if "--wait" in param.opts:
                assert param.is_flag and "--no-wait" in param.secondary_opts, path


def test_td_names_every_version():
    assert not [
        " ".join(path)
        for path, cmd in _tree()
        if path[:1] == ("td",) and "--version" in _options(cmd)
    ]
