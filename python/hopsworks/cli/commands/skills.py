"""``hops skills`` — discover the Hopsworks skills available to agents.

Skills are Markdown playbooks (``SKILL.md``) shipped with this package and
grouped into bucket folders (``ml``, ``data``, ``dashboards``, ``agents``, ...).
This command discovers them dynamically from the skills directory, so the
listing can never drift from what actually ships — unlike a hand-maintained
table. ``hops context`` renders the same catalogue for LLM ingestion.
"""

from __future__ import annotations

import os
from pathlib import Path

import click
from hopsworks.cli import output


def _skills_dir() -> Path | None:
    """Resolve the directory that holds the skill buckets.

    Resolution order: the ``HOPS_SKILLS_DIR`` override, then the copy packaged
    next to the installed ``hopsworks`` package, then a repo checkout located by
    walking up from this file.

    Returns:
        The skills directory, or ``None`` when none can be found.
    """
    env = os.environ.get("HOPS_SKILLS_DIR")
    if env:
        candidate = Path(env).expanduser()
        return candidate if candidate.is_dir() else None

    try:
        from importlib.resources import files

        packaged = Path(str(files("hopsworks"))) / "skills"
        if packaged.is_dir():
            return packaged
    except (ImportError, ModuleNotFoundError, TypeError):
        pass

    for parent in Path(__file__).resolve().parents:
        candidate = parent / "skills"
        if candidate.is_dir() and any(candidate.glob("*/*/SKILL.md")):
            return candidate
    return None


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

    Args:
        skills_dir: Directory containing ``<bucket>/<skill>/SKILL.md`` files.

    Returns:
        Records sorted by bucket then name, each with ``bucket``, ``name`` (the
        folder, which is how skills are invoked), ``frontmatter_name`` (the
        declared ``name``, shown only when it differs), ``description`` and
        ``path`` keys.
    """
    skills: list[dict[str, str]] = []
    for skill_md in skills_dir.glob("*/*/SKILL.md"):
        folder = skill_md.parent.name
        front = _parse_frontmatter(skill_md)
        declared = front["name"]
        skills.append(
            {
                "bucket": skill_md.parent.parent.name,
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
