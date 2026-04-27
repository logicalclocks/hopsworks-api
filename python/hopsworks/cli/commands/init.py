"""``hops init`` — scaffold Claude Code skill, slash command, and sub-agent.

Writes three files into the current repo under ``.claude/`` and grants
``Bash(hops *)`` in ``.claude/settings.local.json`` so Claude can invoke the
CLI without per-command prompts:

* ``.claude/skills/hops/SKILL.md`` — CLI command reference
* ``.claude/commands/hops.md`` — ``/hops`` slash command
* ``.claude/agents/hops-fti.md`` — FTI-pattern sub-agent

The operation is idempotent: if a target file already exists and is identical
we skip it; if it differs we leave the user's copy untouched and print a note
so they can diff at their leisure.
"""

from __future__ import annotations

import json
from importlib import resources
from pathlib import Path
from typing import Any

import click
from hopsworks.cli import output


TEMPLATES = {
    ".claude/skills/hops/SKILL.md": "SKILL.md",
    ".claude/commands/hops.md": "hops.md",
    ".claude/agents/hops-fti.md": "hops-fti.md",
}
SETTINGS_PATH = Path(".claude/settings.local.json")
HOPS_PERMISSION = "Bash(hops *)"


@click.command("init")
@click.option(
    "--dir",
    "target_dir",
    type=click.Path(file_okay=False),
    default=".",
    show_default=True,
    help="Project root to scaffold into.",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite existing files that differ from the template.",
)
def init_cmd(target_dir: str, force: bool) -> None:
    """Scaffold the Hopsworks Claude Code skill bundle.

    Args:
        target_dir: Project root in which ``.claude/`` will be created.
        force: Overwrite existing files when they differ from the template.
    """
    root = Path(target_dir).resolve()
    if not root.exists():
        raise click.ClickException(f"Target directory does not exist: {root}")

    written: list[str] = []
    skipped: list[str] = []
    conflicts: list[str] = []

    for rel_path, template_name in TEMPLATES.items():
        template_text = _load_template(template_name)
        target = root / rel_path
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            current = target.read_text()
            if current == template_text:
                skipped.append(rel_path)
                continue
            if not force:
                conflicts.append(rel_path)
                continue
        target.write_text(template_text)
        written.append(rel_path)

    settings_changed = _ensure_permission(root / SETTINGS_PATH)

    for path in written:
        output.success("✓ Wrote %s", path)
    for path in skipped:
        output.info("= %s already up to date", path)
    for path in conflicts:
        output.warn(
            "! %s exists and differs; re-run with --force to overwrite",
            path,
        )
    if settings_changed:
        output.success("✓ Allowed Bash(hops *) in %s", SETTINGS_PATH)
    else:
        output.info("= Bash(hops *) already allowed in %s", SETTINGS_PATH)

    if output.JSON_MODE:
        output.print_json(
            {
                "written": written,
                "skipped": skipped,
                "conflicts": conflicts,
                "settings_changed": settings_changed,
            }
        )


def _load_template(name: str) -> str:
    """Read a packaged template file into a string.

    Args:
        name: Template filename under ``hopsworks.cli.templates``.

    Returns:
        The template contents.
    """
    pkg = resources.files("hopsworks.cli.templates")
    return (pkg / name).read_text()


def _ensure_permission(settings_path: Path) -> bool:
    """Add ``Bash(hops *)`` to ``.claude/settings.local.json`` if missing.

    Preserves any other keys the user has configured. Returns True when the
    file was modified, False when the permission was already present.

    Args:
        settings_path: Absolute path to the settings file.

    Returns:
        True when a write happened; False when nothing changed.
    """
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {}
    if settings_path.exists():
        try:
            data = json.loads(settings_path.read_text() or "{}")
        except json.JSONDecodeError as exc:
            raise click.ClickException(
                f"Could not parse {settings_path}: {exc}. Fix or delete the file and retry."
            ) from exc
    if not isinstance(data, dict):
        raise click.ClickException(f"{settings_path} must be a JSON object.")

    permissions = data.setdefault("permissions", {})
    allow_list = permissions.setdefault("allow", [])
    if HOPS_PERMISSION in allow_list:
        return False

    allow_list.append(HOPS_PERMISSION)
    settings_path.write_text(json.dumps(data, indent=2) + "\n")
    return True
