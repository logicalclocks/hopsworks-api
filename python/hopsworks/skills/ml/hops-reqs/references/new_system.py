# ruff: noqa: INP001
"""Copy the system template into a new ML system directory.

    python <skills>/hops-reqs/references/new_system.py <repo>/<slug>

Copies system_template/ to the target, renames `src/slug_pkg` to the package
named after the slug (`telco-churn` gives `telco_churn`), replaces the word
`slug_pkg` in the copied Python and TOML files, and installs `gitignore` as
`.gitignore`. Refuses a target that already holds a system.yaml, so an
existing system is never overwritten.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path


TEMPLATE = Path(__file__).resolve().parent / "system_template"
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")


def create(target: Path) -> Path:
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
    return target


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    print(create(Path(sys.argv[1]).resolve()))
