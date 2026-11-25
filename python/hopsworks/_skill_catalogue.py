"""Discover the Markdown skills shipped with Hopsworks."""

from __future__ import annotations

from pathlib import Path

from hopsworks.cli.output import first_line


def _packaged_skills_dir() -> Path | None:
    """The skills this installation ships, ignoring any environment override.

    Returns:
        The packaged skills directory, or None when it is missing.
    """
    packaged = Path(__file__).resolve().parent / "skills"
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


def _render_catalogue(skills: list[dict[str, str]]) -> str:
    """Render the skill summaries for a system prompt or ``hops context``.

    Args:
        skills: Skill records containing the bucket, name, and description.

    Returns:
        A Markdown table, or an empty string when there are no skills.
    """
    if not skills:
        return ""
    rows = [
        "## Available skills",
        "",
        "| Bucket | Skill | When to use |",
        "|---|---|---|",
    ]
    for skill in skills:
        rows.append(
            f"| {skill['bucket']} | {skill['name']} | {first_line(skill['description'])} |"
        )
    return "\n".join(rows) + "\n\n"
