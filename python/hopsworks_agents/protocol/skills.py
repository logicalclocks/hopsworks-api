"""Packaged Hopsworks skills available to one agent deployment."""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import TemporaryDirectory


class DeploymentSkills:
    """Select playbooks for the agent and prepare Claude's project skill directory."""

    def __init__(self, selection: str | list[str] | None = None, *, framework: str):
        claude = framework == "claude_agents"
        if selection is None:
            selection = os.environ.get(
                "HOPSWORKS_AGENT_SKILLS", "all" if claude else "none"
            )

        self._skills: dict[str, dict[str, str]] = {}
        self._project: TemporaryDirectory | None = None
        if selection == "none" or selection == []:
            return

        from hopsworks._skill_catalogue import collect_skills, packaged_skills_dir

        source = packaged_skills_dir()
        if source is None:
            raise ValueError("This installation of hopsworks has no packaged skills.")
        available = {skill["name"]: skill for skill in collect_skills(source)}
        if selection == "all":
            self._skills = available
        else:
            if isinstance(selection, str):
                selection = [name.strip() for name in selection.split(",")]
            if not isinstance(selection, list) or not all(
                isinstance(name, str) for name in selection
            ):
                raise ValueError(
                    "platform_skills must be 'all', 'none', or a list of skill names."
                )
            for name in selection:
                if name not in available:
                    raise ValueError(
                        f"Unknown Hopsworks skill {name!r}. Use 'hops skills list' to see the names."
                    )
                self._skills[name] = available[name]

        if claude and self._skills:
            # The entry script can be in a read-only mount or a shared checkout.
            # Each process gets its own project view, including references and scripts.
            self._project = TemporaryDirectory(prefix="hopsworks-agent-skills-")
            directory = Path(self._project.name) / ".claude" / "skills"
            directory.mkdir(parents=True)
            for name, skill in self._skills.items():
                (directory / name).symlink_to(
                    Path(skill["path"]).parent, target_is_directory=True
                )

    @property
    def names(self) -> list[str]:
        """Names to pass to ClaudeAgentOptions.skills."""
        return list(self._skills)

    @property
    def claude_dirs(self) -> list[str]:
        """Additional Claude project directories to pass to ClaudeAgentOptions.add_dirs."""
        return [self._project.name] if self._project else []

    def catalogue(self) -> str:
        """Summaries of the selected skills for the agent's system prompt."""
        from hopsworks._skill_catalogue import render_catalogue

        return render_catalogue(list(self._skills.values()))

    def read_skill(self, name: str) -> str:
        """Read a selected Hopsworks skill by name, including the path for its relative references."""
        if name not in self._skills:
            raise ValueError(f"Hopsworks skill {name!r} is not enabled.")
        path = Path(self._skills[name]["path"])
        return f"# Source: {path}\n\n{path.read_text(encoding='utf-8')}"
