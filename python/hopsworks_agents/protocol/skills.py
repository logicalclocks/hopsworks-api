"""Packaged Hopsworks skills available to one agent deployment."""

from __future__ import annotations

import logging
import os
import re
import shutil
import stat
from collections.abc import Iterator, Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

import yaml
from hopsworks_apigen import public


_log = logging.getLogger(__name__)


def _parse_selection(
    value: Sequence[str] | None,
) -> Literal["all", "none"] | tuple[str, ...] | None:
    """Return None for an unset string, and an empty tuple for an explicit empty list."""
    if value is None:
        return None
    if isinstance(value, str):
        names = tuple(name.strip() for name in value.split(",") if name.strip())
        if not names:
            return None
        if len(names) == 1:
            keyword = names[0].lower()
            if keyword in ("all", "none"):
                return keyword
        return names
    if not isinstance(value, Sequence) or not all(
        isinstance(name, str) for name in value
    ):
        raise ValueError(
            "platform_skills must be 'all', 'none', or a sequence of skill names."
        )
    return tuple(name.strip() for name in value if name.strip())


# Relative Markdown links that can leave the skill's own folder.
_PARENT_LINK = re.compile(r"(?<=\]\()(\.\./[^\s)#]+)(?=[)#])")
_HEADER = re.compile(r"\A---[ \t]*\n(.*?)^---[ \t]*(?:\n|\Z)", re.MULTILINE | re.DOTALL)
# Markdown file -> link text -> (target skill directory, path within that skill).
_SkillLinks = dict[Path, dict[str, tuple[Path, Path]]]


def _walk_skills(
    start: set[Path], roots: set[Path]
) -> Iterator[tuple[Path, _SkillLinks]]:
    """Visit the selected skill trees and the other packaged trees their Markdown links to."""
    pending = list(start)
    visited = set()
    while pending:
        root = pending.pop()
        if root in visited:
            continue
        visited.add(root)
        links: _SkillLinks = {}
        for md in root.rglob("*.md"):
            if not md.is_file() or not md.resolve().is_relative_to(root):
                continue
            for link in _PARENT_LINK.findall(md.read_text(encoding="utf-8")):
                target = (md.parent / link).resolve()
                owner = next((p for p in (target, *target.parents) if p in roots), None)
                if owner is not None and owner != root:
                    links.setdefault(md, {})[link] = (owner, target.relative_to(owner))
                    pending.append(owner)
        yield root, links


def _discovery_header(body: str, source: Path) -> str:
    """Keep the metadata Claude needs to discover this playbook."""
    header = _HEADER.match(body)
    if header is None:
        raise ValueError(f"Missing skill front matter: {source}")
    fields = yaml.safe_load(header[1])
    keys = ("name", "description")
    if not isinstance(fields, dict) or not all(
        isinstance(fields.get(key), str) for key in keys
    ):
        raise ValueError(f"Expected skill name and description in {source}")
    metadata = yaml.safe_dump({key: fields[key] for key in keys}, sort_keys=False)
    return f"---\n{metadata}---\n{body[header.end() :]}"


def _stage_claude_skills(project: Path, selected: set[Path], roots: set[Path]) -> None:
    # Only selected skills belong in a Claude settings directory. Linked
    # playbooks are ordinary files that the agent can read as references.
    destinations = {
        root: project
        / (".claude/skills" if root in selected else "references")
        / root.name
        for root in roots
    }
    for root, links in _walk_skills(selected, roots):
        dest = destinations[root]
        shutil.copytree(
            root, dest, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
        )
        for md in {*root.rglob("SKILL.md"), *links}:
            staged = dest / md.relative_to(root)
            body = staged.read_text(encoding="utf-8")
            if md.name == "SKILL.md":
                # The deployment controls tool permissions. Publish discovery
                # metadata only, even if the terminal playbook allows tools.
                body = _discovery_header(body, md)
            if md in links:
                replacements = {}
                for link, (owner, relative) in links[md].items():
                    staged_target = destinations[owner] / relative
                    replacements[link] = Path(
                        os.path.relpath(staged_target, staged.parent)
                    ).as_posix()
                body = _PARENT_LINK.sub(
                    lambda m, paths=replacements: paths.get(m[1], m[1]), body
                )
            # copytree preserves the installed package's read-only mode bits.
            staged.chmod(staged.stat().st_mode | stat.S_IWUSR)
            staged.write_text(body, encoding="utf-8")


class DeploymentSkills:
    """Select playbooks for the agent and prepare Claude's project skill directory."""

    def __init__(self, selection: Sequence[str] | None = None, *, framework: str):
        self._skills: dict[str, dict[str, str]] = {}
        self._roots: set[Path] = set()
        self._project: TemporaryDirectory | None = None
        selected = _parse_selection(selection)
        if selected is None:
            selected = _parse_selection(os.environ.get("HOPSWORKS_AGENT_SKILLS"))
        implicit = selected is None
        if selected is None:
            selected = "all" if framework == "claude_agents" else "none"
        if selected == "none" or not selected:
            return

        from hopsworks._skill_catalogue import _collect_skills, _packaged_skills_dir

        try:
            source = _packaged_skills_dir()
            records = _collect_skills(source) if source else []
            if not records:
                raise FileNotFoundError(
                    "This installation of hopsworks has no packaged skills."
                )
            available = {skill["name"]: skill for skill in records}
            if selected != "all":
                for name in selected:
                    if name not in available:
                        raise ValueError(
                            f"Unknown Hopsworks skill {name!r}. Available packaged skills: {', '.join(sorted(available))}."
                        )
                selected_skills = {name: available[name] for name in selected}
            else:
                selected_skills = available

            self._roots = {Path(s["path"]).resolve().parent for s in records}
            if framework == "claude_agents":
                self._project = TemporaryDirectory(
                    prefix="hopsworks-agent-skills-", ignore_cleanup_errors=True
                )
                _stage_claude_skills(
                    Path(self._project.name),
                    {
                        Path(s["path"]).resolve().parent
                        for s in selected_skills.values()
                    },
                    self._roots,
                )

            self._skills = selected_skills
        except (OSError, ValueError, yaml.YAMLError) as exc:
            if self._project is not None:
                self._project.cleanup()
                self._project = None
            if not implicit:
                raise
            _log.warning(
                "Could not prepare the default Hopsworks skills; continuing without them: %s",
                exc,
            )

    @property
    def names(self) -> list[str]:
        """Selected Hopsworks skill names."""
        return list(self._skills)

    @property
    def claude_dirs(self) -> list[str]:
        """Additional Claude project directories to pass to ClaudeAgentOptions.add_dirs."""
        return [self._project.name] if self._project else []

    @public
    def claude_options(self) -> dict[str, Any]:
        """Configure Claude to discover the selected skills alongside project skills.

        Returns:
            Keyword arguments for ``ClaudeAgentOptions``.
        """
        return {
            "setting_sources": ["project"],
            "add_dirs": self.claude_dirs,
            # The directory already contains only selected Hopsworks skills.
            # Filtering by name here would also hide the project's own skills.
            "skills": "all",
        }

    @public
    def catalogue(self) -> str:
        """Summarize the selected skills for the agent's system prompt.

        Returns:
            A Markdown catalogue, or an empty string if no skills are selected.
        """
        from hopsworks._skill_catalogue import _render_catalogue

        return _render_catalogue(list(self._skills.values()))

    @public
    def read_skill(self, name: str, path: str = "SKILL.md") -> str:
        """Read a selected skill and identify the source of its relative references.

        Args:
            name: Folder name of a selected Hopsworks skill.
            path: Text file relative to that skill folder, including references in linked playbooks.

        Returns:
            The source path followed by the Markdown instructions.

        Raises:
            ValueError: The skill is disabled or the path is outside its reference trees.
            FileNotFoundError: The file does not exist.
        """
        if name not in self._skills:
            raise ValueError(f"Hopsworks skill {name!r} is not enabled.")
        root = Path(self._skills[name]["path"]).resolve().parent
        relative = Path(path)
        if relative.anchor:
            raise ValueError("Expected a path relative to the selected skill.")
        target = (root / relative).resolve()
        if not target.is_relative_to(root):
            owner = next((p for p in target.parents if p in self._roots), None)
            if owner is None or not any(
                r == owner for r, _ in _walk_skills({root}, self._roots)
            ):
                raise ValueError("Path is outside this skill and its linked playbooks.")
        return f"# Source: {target}\n\n{target.read_text(encoding='utf-8')}"
