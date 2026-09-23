"""Render the packaged agent-instruction template and write it into a project.

Two concerns live here.
``render`` turns the template shipped at ``hopsworks/cli/templates/AGENTS.md``
into the text for one audience, and ``scaffold`` writes a set of rendered files
into a directory without ever destroying something the user edited.

Ownership is split with the backend.
Inside a Hopsworks terminal the project home's ``CLAUDE.md`` and ``AGENTS.md``
are written by ``AgentHomeScaffolder`` in hopsworks-ee, and this module only
writes the ``hops`` CLI bundle.
Outside a cluster there is no backend, so this module writes both.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping


MANIFEST_PATH = ".claude/.hops-manifest.json"
MANIFEST_VERSION = 1

_BLOCK_RE = re.compile(
    r"^[ \t]*<!--[ \t]*hopsworks:only[ \t]+(?P<tags>[\w \t]+?)[ \t]*-->[ \t]*\n"
    r"(?P<body>.*?)"
    r"^[ \t]*<!--[ \t]*/hopsworks:only[ \t]*-->[ \t]*\n",
    re.DOTALL | re.MULTILINE,
)
_PLACEHOLDER_RE = re.compile(r"\{\{(\w+)\}\}")


@dataclass
class ScaffoldResult:
    """What one ``scaffold`` call did, as lists of root-relative paths."""

    written: list[str] = field(default_factory=list)
    refreshed: list[str] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    orphaned: list[str] = field(default_factory=list)

    def changed(self) -> bool:
        """Whether the call touched the filesystem.

        Returns:
            True when anything was created, updated or deleted.
        """
        return bool(self.written or self.refreshed or self.removed)


def load_template(name: str = "AGENTS.md") -> str:
    """Read a packaged template file.

    Args:
        name: Filename under ``hopsworks.cli.templates``.

    Returns:
        The raw template text, placeholders and conditional blocks intact.
    """
    return (resources.files("hopsworks.cli.templates") / name).read_text(
        encoding="utf-8"
    )


def render(template: str, tags: Iterable[str], variables: Mapping[str, str]) -> str:
    """Resolve conditional blocks and placeholders in a template.

    A ``<!-- hopsworks:only a b -->`` block survives when any of its tags is in
    ``tags``, and is dropped whole otherwise.
    Placeholders are substituted after the blocks are resolved, so a variable
    only needed by a dropped block may be absent from ``variables``.
    An unknown placeholder is left verbatim rather than raising, so a template
    that gains a variable still renders against an older caller.

    Args:
        template: Raw template text.
        tags: Audience tags that select which blocks survive.
        variables: Placeholder name to replacement text.

    Returns:
        The rendered text.
    """
    active = set(tags)

    def keep(match: re.Match[str]) -> str:
        block_tags = set(match.group("tags").split())
        return match.group("body") if block_tags & active else ""

    text = _BLOCK_RE.sub(keep, template)
    text = _PLACEHOLDER_RE.sub(
        lambda m: variables.get(m.group(1), m.group(0)),
        text,
    )
    # Dropping a block leaves the blank lines that surrounded it, and two
    # adjacent drops leave a visible gap in the rendered file.
    return re.sub(r"\n{3,}", "\n\n", text).strip() + "\n"


def scaffold(
    root: Path,
    files: Mapping[str, str],
    *,
    manifest: str = MANIFEST_PATH,
    force: bool = False,
) -> ScaffoldResult:
    """Write ``files`` under ``root``, preserving anything the user edited.

    The manifest records the digest of every file this function wrote.
    Each caller owns a separate one: a manifest shared between two callers
    would leave each treating the other's entries as files it had stopped
    shipping, and deleting them.
    On a later call a file whose digest still matches is refreshed to the
    current content, and one that differs is left alone: the difference is the
    user's edit.
    A path in the manifest that ``files`` no longer supplies is deleted when
    unedited, which is what lets a removed skill disappear from a project
    instead of lingering forever.

    A file that exists but is absent from the manifest is treated as the user's,
    never overwritten without ``force``.

    Args:
        root: Directory to scaffold into.
        files: Root-relative path to desired content.
        manifest: Root-relative path of the digest record for this caller.
        force: Overwrite edited and unmanaged files too.

    Returns:
        A ScaffoldResult naming what happened to each path.
    """
    manifest_file = root / manifest
    recorded = _read_manifest(manifest_file)
    result = ScaffoldResult()
    written_digests: dict[str, str] = {}

    for rel, content in sorted(files.items()):
        target = root / rel
        digest = _digest(content)
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
            result.written.append(rel)
        else:
            current = _digest(target.read_text(encoding="utf-8"))
            if current == digest:
                pass
            elif force or recorded.get(rel) == current:
                target.write_text(content, encoding="utf-8")
                result.refreshed.append(rel)
            else:
                result.kept.append(rel)
                # Carry the recorded digest forward untouched, and record nothing
                # at all when there is none. Storing the file's current digest
                # here instead would say we wrote what the user wrote, and the
                # next run would find a match and overwrite them.
                previous = recorded.get(rel)
                if previous is not None:
                    written_digests[rel] = previous
                continue
        written_digests[rel] = digest

    for rel, previous in sorted(recorded.items()):
        if rel in files:
            continue
        target = root / rel
        if not target.exists():
            continue
        if force or _digest(target.read_text(encoding="utf-8")) == previous:
            target.unlink()
            _prune_empty_dirs(target.parent, root)
            result.removed.append(rel)
        else:
            result.orphaned.append(rel)
            written_digests[rel] = previous

    _write_manifest(manifest_file, written_digests)
    return result


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _read_manifest(path: Path) -> dict[str, str]:
    """Return the recorded path-to-digest map, empty when unreadable.

    A corrupt or hand-edited manifest must not brick the command: an empty map
    degrades to create-if-missing, which never destroys anything.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict) or data.get("version") != MANIFEST_VERSION:
        return {}
    entries = data.get("files")
    if not isinstance(entries, dict):
        return {}
    return {k: v for k, v in entries.items() if isinstance(v, str)}


def _write_manifest(path: Path, digests: Mapping[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"version": MANIFEST_VERSION, "files": dict(sorted(digests.items()))}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _prune_empty_dirs(start: Path, stop: Path) -> None:
    """Remove directories left empty by a deletion, up to but excluding ``stop``."""
    current = start
    while current != stop and stop in current.parents:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


# region Bundles

# The `hops` CLI bundle, written in every mode. Values are template filenames
# under ``hopsworks.cli.templates``.
CLI_BUNDLE = {
    ".claude/skills/hops/SKILL.md": "SKILL.md",
    ".claude/commands/hops.md": "hops.md",
    ".claude/agents/hops-train-agent.md": "hops-train-agent.md",
    ".claude/agents/hops-infer-agent.md": "hops-infer-agent.md",
}
AGENTS_PATH = "AGENTS.md"
HOPS_PERMISSION = "Bash(hops *)"


def build_files(*, internal: bool, project: str | None) -> dict[str, str]:
    """Assemble the path-to-content map for one scaffold call.

    In internal mode the project home's ``CLAUDE.md`` and ``AGENTS.md`` belong
    to hopsworks-ee, so only the CLI bundle is produced.
    Externally there is no backend to write them, and the rendered ``AGENTS.md``
    carries the SDK source map an external agent has no other way to find.
    The skills themselves are materialized separately by ``hops skills install``, which
    puts them where each agent already looks rather than listing them in a file
    every agent loads in full.

    Args:
        internal: True when running inside a Hopsworks pod.
        project: Active project name, used to address the agent.

    Returns:
        Root-relative path to file content.
    """
    files = {path: load_template(name) for path, name in CLI_BUNDLE.items()}
    if internal:
        return files

    files[AGENTS_PATH] = render(
        load_template(),
        tags=["external"],
        variables={
            "PROJECT": project or "(none selected)",
            "SDK_PATH": str(_sdk_path()),
        },
    )
    return files


def _sdk_path() -> Path:
    """Directory holding the installed SDK packages.

    This is the parent of ``hopsworks/`` itself, so the table of package names
    in the template resolves against it.
    """
    return Path(__file__).resolve().parents[2]


def ensure_permission(settings_path: Path) -> bool:
    """Add ``Bash(hops *)`` to a Claude settings file if missing.

    Other keys the user configured are preserved.
    Left out of the manifest deliberately: this file is merged rather than
    owned, so it is never refreshed or removed.

    Args:
        settings_path: Absolute path to ``.claude/settings.local.json``.

    Returns:
        True when the file was modified.

    Raises:
        ValueError: The file exists but is not a JSON object.
    """
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    data: dict = {}
    if settings_path.exists():
        try:
            data = json.loads(settings_path.read_text(encoding="utf-8") or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError(f"Could not parse {settings_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"{settings_path} must be a JSON object.")

    allow_list = data.setdefault("permissions", {}).setdefault("allow", [])
    if HOPS_PERMISSION in allow_list:
        return False
    allow_list.append(HOPS_PERMISSION)
    settings_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return True


# endregion
