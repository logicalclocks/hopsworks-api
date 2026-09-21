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
from pathlib import Path

import click
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

    for candidate in (
        Path("/opt/hopsworks-api/python/hopsworks/skills"),
        Path(__file__).resolve().parents[2] / "skills",
    ):
        if candidate.is_dir():
            return candidate

    return None


def _packaged_skills_dir() -> Path | None:
    """The skills this installation ships, ignoring any environment override.

    ``_skills_dir`` answers "what do the agents here load", which inside a
    terminal is a merged directory of platform and user skills. This answers
    "what did this package ship", which is what gets copied into a repository.

    Returns:
        The packaged skills directory, or None when it is missing.
    """
    packaged = Path(__file__).resolve().parents[2] / "skills"
    return packaged if packaged.is_dir() else None


def _parse_frontmatter(skill_md: Path) -> dict[str, str]:
    """Extract ``name`` and ``description`` from a ``SKILL.md`` YAML header.

    A deliberately small parser: it reads only the two scalar fields needed for
    a listing and folds an indented multi-line ``description`` back into one
    line, avoiding a YAML dependency for what is a fixed, simple shape.

    Args:
        skill_md: Path to the ``SKILL.md`` file.

    Returns:
        A mapping with ``name`` and ``description`` keys (values may be empty).
    """
    empty = {"name": "", "description": ""}
    try:
        lines = skill_md.read_text(encoding="utf-8").splitlines()
    except OSError:
        return empty
    if not lines or lines[0].strip() != "---":
        return empty

    block: list[str] = []
    for line in lines[1:]:
        if line.strip() == "---":
            break
        block.append(line)

    name = ""
    description = ""
    i = 0
    while i < len(block):
        key, sep, value = block[i].partition(":")
        field = key.strip()
        if sep and field == "name":
            name = value.strip()
        elif sep and field == "description":
            parts = [value.strip()]
            j = i + 1
            while j < len(block) and block[j][:1] in (" ", "\t") and block[j].strip():
                parts.append(block[j].strip())
                j += 1
            description = " ".join(p for p in parts if p)
            i = j
            continue
        i += 1
    return {"name": name, "description": description}


def _collect_skills(skills_dir: Path) -> list[dict[str, str]]:
    """Scan ``skills_dir`` and return one record per skill.

    Both layouts are accepted.
    The package ships skills grouped into buckets
    (``<bucket>/<skill>/SKILL.md``), while a user's project home holds them
    flat (``<skill>/SKILL.md``), because one level is what a coding agent
    discovers.
    A flat skill reports an empty bucket.

    Args:
        skills_dir: Directory containing the skills, in either layout.

    Returns:
        Records sorted by bucket then name, each with ``bucket``, ``name`` (the
        folder, which is how skills are invoked), ``frontmatter_name`` (the
        declared ``name``, shown only when it differs), ``description`` and
        ``path`` keys.
    """
    root = skills_dir.resolve()
    found = list(skills_dir.glob("*/*/SKILL.md")) + list(skills_dir.glob("*/SKILL.md"))
    skills: list[dict[str, str]] = []
    for skill_md in found:
        skill_root = skill_md.parent.parent.resolve()
        front = _parse_frontmatter(skill_md)
        declared = front["name"]
        folder = skill_md.parent.name
        skills.append(
            {
                "bucket": "" if skill_root == root else skill_md.parent.parent.name,
                "name": folder,
                "frontmatter_name": declared if declared and declared != folder else "",
                "description": front["description"],
                "path": str(skill_md),
            }
        )
    skills.sort(key=lambda s: (s["bucket"], s["name"]))
    return skills


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

# Where each agent looks for project-local skills. The three directory-based
# ones mirror what the terminal images link in the user's home. opencode reads
# a config array instead of a conventional directory, so it gets a directory of
# its own plus a pointer to it in the project's opencode.json.
AGENT_SKILL_DIRS = {
    "claude": ".claude/skills",
    "codex": ".codex/skills",
    "copilot": ".agents/skills",
    "opencode": ".opencode/skills",
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
    for agent in agents:
        base = AGENT_SKILL_DIRS[agent]
        results[agent] = scaffold.scaffold(
            root,
            {f"{base}/{rel}": body for rel, body in payload.items()},
            manifest=f"{base}/{SKILLS_MANIFEST}",
            force=force,
        )
        if agent == "opencode":
            _register_opencode_path(root, base)

    for agent, result in results.items():
        _report(agent, result)

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


def _report(agent: str, result: scaffold.ScaffoldResult) -> None:
    """Print one line per agent, naming only the counts that are non-zero."""
    parts = []
    for label, paths in (
        ("added", result.written),
        ("updated", result.refreshed),
        ("removed", result.removed),
        ("kept yours", result.kept + result.orphaned),
    ):
        if paths:
            parts.append(f"{len(paths)} {label}")
    target = AGENT_SKILL_DIRS[agent]
    if parts:
        output.success("✓ %s: %s in %s/", agent, ", ".join(parts), target)
    else:
        output.info("= %s: already up to date in %s/", agent, target)


# endregion
