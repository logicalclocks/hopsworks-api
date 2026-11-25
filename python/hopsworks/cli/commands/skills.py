"""``hops skills`` — discover the Hopsworks skills available to agents.

Skills are Markdown playbooks (``SKILL.md``) shipped in the repository and
grouped into bucket folders (``ml``, ``data``, ``dashboards``, ``agents``, ...).
This command discovers them dynamically from the skills directory, so the
listing can never drift from what actually ships — unlike a hand-maintained
table. ``hops context`` renders the same catalogue for LLM ingestion.
"""

from __future__ import annotations

import json
import os
import shlex
from pathlib import Path

import click
from hopsworks._skill_catalogue import _collect_skills, _packaged_skills_dir
from hopsworks.cli import output, scaffold


def _skills_dir() -> Path | None:
    """Resolve the directory that holds the skill buckets.

    Resolution order: the ``HOPS_SKILLS_DIR`` override, then the source tree
    cloned into the terminal images, then the copy shipped inside this package.
    Inside a Hopsworks terminal the override points at the directory that merges
    the platform skills with the user's own, so the listing matches what the
    agents there actually load.
    The packaged copy is what an external ``pip install`` resolves to, and is
    the same tree a source checkout sees through the repo-root ``skills``
    symlink.

    Returns:
        The skills directory, or ``None`` when none can be found.
    """
    env = os.environ.get("HOPS_SKILLS_DIR")
    if env:
        candidate = Path(env).expanduser()
        return candidate if candidate.is_dir() else None

    checkout = Path("/opt/hopsworks-api/python/hopsworks/skills")
    return checkout if checkout.is_dir() else _packaged_skills_dir()


@click.group("skills")
def skills_group() -> None:
    """Discover the Hopsworks skills (Markdown playbooks) available to agents."""


@skills_group.command("list")
@click.option("--bucket", default=None, help="Only show skills in this bucket.")
def skills_list(bucket: str | None) -> None:
    """List available skills as a table (or JSON with ``--json``).

    Args:
        bucket: When given, restrict the listing to this bucket folder.
    """
    skills_dir = _skills_dir()
    if skills_dir is None:
        output.error("No skills directory found; set HOPS_SKILLS_DIR to override.")
        raise SystemExit(1)

    skills = _collect_skills(skills_dir)
    if bucket:
        skills = [s for s in skills if s["bucket"] == bucket]

    if output.JSON_MODE:
        output.print_json(skills)
        return

    output.print_table(
        ["BUCKET", "SKILL", "DESCRIPTION"],
        [(s["bucket"], s["name"], output.first_line(s["description"])) for s in skills],
    )


@skills_group.command("show")
@click.argument("name")
def skills_show(name: str) -> None:
    """Print a skill's ``SKILL.md`` so an agent can load it.

    Args:
        name: Skill name (frontmatter ``name``) or folder name.
    """
    skills_dir = _skills_dir()
    if skills_dir is None:
        output.error("No skills directory found; set HOPS_SKILLS_DIR to override.")
        raise SystemExit(1)

    match = next(
        (
            s
            for s in _collect_skills(skills_dir)
            if name in (s["name"], s["frontmatter_name"])
        ),
        None,
    )
    if match is None:
        output.error("Skill %r not found; run `hops skills list`.", name)
        raise SystemExit(1)

    if output.JSON_MODE:
        output.print_json({**match, "body": Path(match["path"]).read_text("utf-8")})
        return

    output.info("# %s  (%s)\n# %s\n", match["name"], match["bucket"], match["path"])
    click.echo(Path(match["path"]).read_text("utf-8"))


# region install

# Inside a Hopsworks terminal the skills are served from the image and symlinked
# into every agent's skills directory. An external ``pip install hopsworks`` has
# no such wiring, so `hops skills install` copies the skills the package ships
# into the directory the chosen agent already looks in, under the repository
# being worked on. Skills rather than a table of contents in AGENTS.md: an agent
# loads a skill's name and description at startup and its body only when the
# task calls for it, whereas anything in AGENTS.md is loaded in full, every time.

# Where each agent looks for project-local skills. The directory-based ones
# mirror what the terminal images link in the user's home; copilot, pi and
# Hermes share the cross-agent .agents/skills, so one copy serves all three.
# opencode reads a config array instead of a conventional directory, so it gets
# a directory of its own plus a pointer to it in the project's opencode.json.
AGENT_SKILL_DIRS = {
    "claude": ".claude/skills",
    "codex": ".codex/skills",
    "copilot": ".agents/skills",
    "hermes": ".agents/skills",
    "opencode": ".opencode/skills",
    "pi": ".agents/skills",
}
OPENCODE_CONFIG = "opencode.json"

# Per-agent so that removing one agent's directory takes its bookkeeping with
# it, and so `hops setup`'s manifest never sees these paths as files it has
# stopped shipping.
SKILLS_MANIFEST = ".hops-skills.json"


@skills_group.command("install")
@click.option(
    "--agent",
    "agents",
    type=click.Choice(sorted(AGENT_SKILL_DIRS)),
    multiple=True,
    default=("claude",),
    show_default=True,
    help="Coding agent to materialize skills for. Repeat for several.",
)
@click.option(
    "--dir",
    "target_dir",
    type=click.Path(file_okay=False),
    default=".",
    show_default=True,
    help="Repository root to materialize into.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite skills you have edited.",
)
def skills_install(agents: tuple[str, ...], target_dir: str, force: bool) -> None:
    """Copy the Hopsworks skills into this repository for a coding agent.

    Re-running updates a skill you have not edited, leaves one you have, and
    removes one this release no longer ships.

    Args:
        agents: Agents to materialize for.
        target_dir: Repository root to write into.
        force: Replace skills that differ from the shipped copy.

    Raises:
        ClickException: The target does not exist, or no skills are installed.
    """
    root = Path(target_dir).resolve()
    if not root.is_dir():
        raise click.ClickException(f"Target directory does not exist: {root}")

    # Deliberately the packaged copy, not `_skills_dir()`: inside a Hopsworks
    # terminal that resolves to the merged platform-and-user directory the
    # agents read, which is both flat and not ours to copy back out.
    source = _packaged_skills_dir()
    if source is None:
        raise click.ClickException(
            "This installation ships no skills. Reinstall with "
            "`uv pip install --upgrade hopsworks`."
        )

    payload = _read_skills(source)
    if not payload:
        raise click.ClickException(f"No skills found under {source}.")

    results = {}
    by_dir: dict[str, scaffold.ScaffoldResult] = {}
    for agent in dict.fromkeys(agents):
        base = AGENT_SKILL_DIRS[agent]
        if base not in by_dir:
            by_dir[base] = scaffold.scaffold(
                root,
                {f"{base}/{rel}": body for rel, body in payload.items()},
                manifest=f"{base}/{SKILLS_MANIFEST}",
                force=force,
            )
        results[agent] = by_dir[base]
        if agent == "opencode":
            _register_opencode_path(root, base)

    for base, result in by_dir.items():
        _report([a for a in results if AGENT_SKILL_DIRS[a] == base], result)
    if "hermes" in results:
        # Hermes loads a repository's skills only once the repository is trusted.
        output.info(
            "Run `hermes skills trust %s` so Hermes loads them.", shlex.quote(str(root))
        )

    if output.JSON_MODE:
        output.print_json(
            {
                agent: {
                    "written": r.written,
                    "refreshed": r.refreshed,
                    "kept": r.kept,
                    "removed": r.removed,
                    "orphaned": r.orphaned,
                }
                for agent, r in results.items()
            }
        )


def _read_skills(source: Path) -> dict[str, str]:
    """Collect every shipped skill as agent-relative path to content.

    The bucket level is dropped: agents discover ``<skills>/<name>/SKILL.md``,
    so a directory of bucket directories finds nothing.
    Both the bucketed layout the package ships and a flat one are accepted,
    matching what ``hops skills list`` already tolerates.
    Each skill's ``references/`` and ``scripts/`` come along, since a SKILL.md
    links to them by relative path.

    Args:
        source: Directory holding the shipped skills.

    Returns:
        Mapping of ``<name>/<relative path>`` to file content.
    """
    payload: dict[str, str] = {}
    found = list(source.glob("*/*/SKILL.md")) + list(source.glob("*/SKILL.md"))
    for skill_md in sorted(found):
        skill_root = skill_md.parent
        for path in sorted(skill_root.rglob("*")):
            if not path.is_file():
                continue
            try:
                # Explicit, because the platform default is not UTF-8 everywhere:
                # on Windows it silently dropped every skill with a non-ASCII
                # character, which is most of them.
                body = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                # Skills are Markdown and Python; anything else is not ours to
                # copy blindly into someone's repository.
                continue
            rel = path.relative_to(skill_root).as_posix()
            payload[f"{skill_root.name}/{rel}"] = body
    return payload


def _register_opencode_path(root: Path, skills_dir: str) -> bool:
    """Add ``skills_dir`` to ``skills.paths`` in the project's opencode.json.

    opencode takes skill locations from a config array rather than a
    conventional directory, so materializing the files is not enough on its
    own. Other keys in the file are preserved.

    Args:
        root: Repository root.
        skills_dir: Root-relative directory the skills were written to.

    Returns:
        True when the file was modified.

    Raises:
        ClickException: The file exists but is not a JSON object.
    """
    config_path = root / OPENCODE_CONFIG
    data: dict = {}
    if config_path.exists():
        try:
            data = json.loads(config_path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError as exc:
            raise click.ClickException(
                f"Could not parse {OPENCODE_CONFIG}: {exc}. Fix or delete it and retry."
            ) from exc
    if not isinstance(data, dict):
        raise click.ClickException(f"{OPENCODE_CONFIG} must be a JSON object.")

    skills = data.setdefault("skills", {})
    paths = skills.setdefault("paths", [])
    if skills_dir in paths:
        return False
    paths.append(skills_dir)
    config_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    output.success("✓ Registered %s in %s", skills_dir, OPENCODE_CONFIG)
    return True


def _report(agents: list[str], result: scaffold.ScaffoldResult) -> None:
    """Print one line per skills directory, naming only the counts that are non-zero."""
    parts = []
    for label, paths in (
        ("added", result.written),
        ("updated", result.refreshed),
        ("removed", result.removed),
        ("kept yours", result.kept + result.orphaned),
    ):
        if paths:
            parts.append(f"{len(paths)} {label}")
    names = ", ".join(agents)
    target = AGENT_SKILL_DIRS[agents[0]]
    if parts:
        output.success("✓ %s: %s in %s/", names, ", ".join(parts), target)
    else:
        output.info("= %s: already up to date in %s/", names, target)


# endregion
