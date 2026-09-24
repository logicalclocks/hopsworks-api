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
"""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

import yaml


TEMPLATE = Path(__file__).resolve().parent / "system_template"
EXAMPLES = Path(__file__).resolve().parent / "example-systems.yaml"
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
    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("target", type=Path)
    parser.add_argument("--example", choices=sorted(examples()))
    args = parser.parse_args()
    print(create(args.target.resolve(), args.example))
