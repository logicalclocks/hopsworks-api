# ruff: noqa: INP001
"""Copy the system template into a new ML system directory.

    python <skills>/hops-reqs/references/new_system.py <repo>/<slug> [--example <name>]

Copies system_template/ to the target, renames `src/slug_pkg` to the package
named after the slug (`telco-churn` gives `telco_churn`), replaces the word
`slug_pkg` in the copied Python and TOML files, and installs `gitignore` as
`.gitignore`. Refuses a target that already holds a system.yaml, so an
existing system is never overwritten.

With `--example`, also writes the system.yaml of that entry of
example-systems.yaml, as a draft whose requirements are still pending.

Also puts AGENTS.md (this directory's copy) at the root of the repository the
system is in, or beside the system outside one, with a CLAUDE.md that imports
it, so a coding agent there knows the system is built from system.yaml and
checks what a changed system.yaml means downstream. The section is a marked
block: an existing AGENTS.md keeps its text, and in a Hopsworks home the block
also goes into `.claude/CLAUDE.md` instead of a new CLAUDE.md.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from pathlib import Path

import yaml


TEMPLATE = Path(__file__).resolve().parent / "system_template"
EXAMPLES = Path(__file__).resolve().parent / "example-systems.yaml"
AGENTS = Path(__file__).resolve().parent / "AGENTS.md"
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")


def examples() -> dict:
    """The example systems, by slug."""
    return yaml.safe_load(EXAMPLES.read_text(encoding="utf-8"))


def example_doc(name: str, slug: str) -> dict:
    """The draft system.yaml of example `name` for a system called `slug`."""
    entry = examples().get(name)
    if entry is None:
        raise SystemExit(f"no example {name!r}; one of {sorted(examples())}")
    doc = {key: value for key, value in entry.items() if key != "label"}
    doc["system"] = {
        **doc.get("system", {}),
        "slug": slug,
        "example": name,
        "status": "draft",
    }
    doc["requirements"] = {"status": "pending", **doc.get("requirements", {})}
    return {"schema_version": 1, **doc}


def repo_root(directory: Path) -> Path:
    """The top of the git work tree `directory` is in, or `directory` outside one."""
    found = subprocess.run(
        ["git", "-C", str(directory), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    return Path(found) if found else directory


# The Hopsworks home scaffolder and the terminal images keep any
# `<!-- X_AUTO_BEGIN -->` block out of their digests and put it back when they
# rewrite the file, so the section survives in a home's AGENTS.md.
BEGIN = "<!-- ML_SYSTEMS_AUTO_BEGIN -->"
END = "<!-- ML_SYSTEMS_AUTO_END -->"


def add_section(path: Path) -> None:
    """Write the AGENTS.md section into `path` as a marked block, replacing an earlier one."""
    block = f"{BEGIN}\n{AGENTS.read_text(encoding='utf-8').strip()}\n{END}\n"
    if not path.exists():
        path.write_text(block, encoding="utf-8")
        return
    text = path.read_text(encoding="utf-8")
    if BEGIN in text and END in text:
        head, rest = text.split(BEGIN, 1)
        text = head + block + rest.split(END, 1)[1].lstrip("\n")
    else:
        text = text.rstrip("\n") + "\n\n" + block
    path.write_text(text, encoding="utf-8")


def install_agents(root: Path) -> None:
    """Give coding agents at `root` the AGENTS.md section.

    In a Hopsworks home, whose `.claude/CLAUDE.md` is what Claude reads, the
    section goes into it and into the home's AGENTS.md. Elsewhere it goes into
    AGENTS.md, with a CLAUDE.md that imports it unless one exists.
    """
    add_section(root / "AGENTS.md")
    home_claude = root / ".claude" / "CLAUDE.md"
    if home_claude.exists():
        add_section(home_claude)
    elif not (root / "CLAUDE.md").exists():
        (root / "CLAUDE.md").write_text("@AGENTS.md\n", encoding="utf-8")


def create(target: Path, example: str | None = None) -> Path:
    """Materialize the template at `target`, whose name is the system's slug."""
    slug = target.name
    if not SLUG.match(slug):
        raise SystemExit(
            f"{slug!r} is not a slug: lowercase letters, digits and hyphens"
        )
    if (target / "system.yaml").exists():
        raise SystemExit(f"{target} already holds a system.yaml; resume it instead")
    package = slug.replace("-", "_")
    shutil.copytree(
        TEMPLATE,
        target,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", ".pytest_cache"),
    )
    (target / "src" / "slug_pkg").rename(target / "src" / package)
    (target / "gitignore").rename(target / ".gitignore")
    for path in target.rglob("*"):
        if path.suffix in (".py", ".toml") and path.is_file():
            text = path.read_text(encoding="utf-8")
            if "slug_pkg" in text:
                path.write_text(
                    re.sub(r"\bslug_pkg\b", package, text), encoding="utf-8"
                )
    if example:
        (target / "system.yaml").write_text(
            yaml.safe_dump(
                example_doc(example, slug),
                sort_keys=False,
                allow_unicode=True,
                width=100,
            ),
            encoding="utf-8",
        )
    install_agents(repo_root(target.parent))
    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("target", type=Path)
    parser.add_argument("--example", choices=sorted(examples()))
    args = parser.parse_args()
    print(create(args.target.resolve(), args.example))
