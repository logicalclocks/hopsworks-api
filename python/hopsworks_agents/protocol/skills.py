"""Packaged Hopsworks skills available to one agent deployment."""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal

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


def _link_claude_skills(
    project: Path, selected: dict[str, dict[str, str]], records: list[dict[str, str]]
) -> None:
    settings = project / ".claude"
    directory = settings / "skills"
    directory.mkdir(parents=True)
    for name, skill in selected.items():
        (directory / name).symlink_to(
            Path(skill["path"]).parent, target_is_directory=True
        )

    # Markdown readers can normalize ../../<bucket>/... before following
    # the skill symlink. Keep the buckets outside the discovery directory.
    buckets = {Path(skill["path"]).parents[1] for skill in records if skill["bucket"]}
    for bucket in buckets:
        (settings / bucket.name).symlink_to(bucket, target_is_directory=True)


class DeploymentSkills:
    """Select playbooks for the agent and prepare Claude's project skill directory."""

    def __init__(self, selection: Sequence[str] | None = None, *, framework: str):
        self._skills: dict[str, dict[str, str]] = {}
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
                            f"Unknown Hopsworks skill {name!r}. Use 'hops skills list' to see the names."
                        )
                selected_skills = {name: available[name] for name in selected}
            else:
                selected_skills = available

            if framework == "claude_agents":
                self._project = TemporaryDirectory(
                    prefix="hopsworks-agent-skills-", ignore_cleanup_errors=True
                )
                _link_claude_skills(Path(self._project.name), selected_skills, records)

            self._skills = selected_skills
        except OSError as exc:
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
    def read_skill(self, name: str) -> str:
        """Read a selected skill and identify the source of its relative references.

        Args:
            name: Folder name of a selected Hopsworks skill.

        Returns:
            The source path followed by the Markdown instructions.

        Raises:
            ValueError: The skill is not enabled for this deployment.
        """
        if name not in self._skills:
            raise ValueError(f"Hopsworks skill {name!r} is not enabled.")
        path = Path(self._skills[name]["path"])
        return f"# Source: {path}\n\n{path.read_text(encoding='utf-8')}"
