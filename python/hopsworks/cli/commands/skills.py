"""``hops skills`` — discover the Hopsworks skills available to agents.

Skills are Markdown playbooks (``SKILL.md``) shipped with this package and
grouped into bucket folders (``ml``, ``data``, ``dashboards``, ``agents``, ...).
This command discovers them dynamically from the skills directory, so the
listing can never drift from what actually ships — unlike a hand-maintained
table. ``hops context`` renders the same catalogue for LLM ingestion.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output, session


MANIFEST_FILE = "MANIFEST.json"


def _skills_dir() -> Path | None:
    """Resolve the directory that holds the skill buckets.

    Resolution order: the ``HOPS_SKILLS_DIR`` override, then the copy packaged
    inside the ``hopsworks`` package.
    Inside a Hopsworks terminal the override points at the user's own skills
    directory in their project home, so the listing reflects what they can edit
    rather than what the SDK happens to ship.

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


def _digest(skills_dir: Path) -> str:
    """Hash the contents of a skills tree.

    Names every file by its path relative to the tree and hashes paths and
    bytes together in sorted order, so the result changes when a file is
    renamed or moved, not only when its contents change.
    ``MANIFEST.json`` is excluded because it carries the digest itself.

    Args:
        skills_dir: Root of the skills tree.

    Returns:
        A ``sha256:<hex>`` string.
    """
    digest = hashlib.sha256()
    files = sorted(
        p for p in skills_dir.rglob("*") if p.is_file() and p.name != MANIFEST_FILE
    )
    for path in files:
        digest.update(str(path.relative_to(skills_dir)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


@skills_group.command("manifest")
@click.option(
    "--output",
    "output_path",
    type=click.Path(dir_okay=False),
    default=None,
    help="Write the manifest here instead of to stdout.",
)
def skills_manifest(output_path: str | None) -> None:
    """Describe the shipped skill set as JSON.

    Written for the installer that publishes skills into a cluster: it reads
    the version, the content digest and one entry per skill from here rather
    than re-parsing ``SKILL.md`` frontmatter itself, so the published manifest
    and ``hops skills list`` can never disagree.

    Args:
        output_path: File to write to; stdout when omitted.
    """
    skills_dir = _skills_dir()
    if skills_dir is None:
        output.error("No skills directory found; set HOPS_SKILLS_DIR to override.")
        raise SystemExit(1)

    from hopsworks_common import version

    manifest = {
        "version": version.__version__,
        "digest": _digest(skills_dir),
        "skills": [
            {
                "name": s["name"],
                "bucket": s["bucket"],
                "description": s["description"],
            }
            for s in _collect_skills(skills_dir)
        ],
    }
    body = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if output_path:
        Path(output_path).write_text(body, encoding="utf-8")
        output.info("Wrote %s (%d skills)", output_path, len(manifest["skills"]))
        return
    click.echo(body, nl=False)


def _skills_status(ctx: click.Context) -> dict[str, Any]:
    """Fetch the cluster's view of this user's skills.

    Args:
        ctx: Click context.

    Returns:
        The status payload from the backend.
    """
    from hopsworks_common.core import rest

    session.get_project(ctx)
    try:
        return rest._send_request("GET", rest._project_path("agentskills"))
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not read the skills status: {exc}") from exc


@skills_group.command("status")
@click.pass_context
def skills_status(ctx: click.Context) -> None:
    """Compare the skills in your project home with the installed version.

    Args:
        ctx: Click context.
    """
    payload = _skills_status(ctx)
    if output.JSON_MODE:
        output.print_json(payload)
        return

    installed = payload.get("installedVersion") or "none published"
    mine = payload.get("homeVersion") or "none installed"
    output.print_table(
        ["FIELD", "VALUE"],
        [
            ["Cluster", installed],
            ["Your home", mine],
            ["Path", payload.get("skillsPath", "?")],
            ["Upgrade available", "yes" if payload.get("upgradeAvailable") else "no"],
        ],
    )


@skills_group.command("upgrade")
@click.pass_context
def skills_upgrade(ctx: click.Context) -> None:
    """Replace the platform skills in your project home with the installed set.

    Only the skills the platform published are replaced; anything you wrote
    yourself is left alone. The copy is performed by the cluster, because the
    directory the skills are published to is not readable by project users, so
    this queues the work and returns rather than waiting for it.

    Args:
        ctx: Click context.
    """
    from hopsworks_common.core import rest

    payload = _skills_status(ctx)
    if not payload.get("upgradeAvailable"):
        output.info("Skills are already up to date (%s).", payload.get("homeVersion"))
        return

    try:
        rest._send_request("POST", rest._project_path("agentskills", "upgrade"))
    except Exception as exc:  # noqa: BLE001
        raise click.ClickException(f"Could not request the upgrade: {exc}") from exc
    output.info(
        "Queued an upgrade to %s. It lands in %s shortly; your own skills are untouched.",
        payload.get("installedVersion"),
        payload.get("skillsPath"),
    )
