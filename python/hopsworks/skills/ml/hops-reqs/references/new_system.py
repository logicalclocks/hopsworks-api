# ruff: noqa: INP001
"""Copy the system template into a new ML system directory, and each build phase's stubs when it starts.

    python <hops-reqs>/references/new_system.py <dir>/<slug> [--example <name>]
    python <hops-reqs>/references/new_system.py <dir>/<slug> --phase features|train|infer

Copies system_template/ to the target without the stubs a build phase fills,
renames `src/slug_pkg` to the package named after the slug (`telco-churn`
gives `telco_churn`), replaces the word `slug_pkg` in the copied Python and
TOML files, and installs `gitignore` as `.gitignore`. Refuses a target that
already holds a system.yaml, so an existing system is never overwritten.

A target outside any git work tree becomes a git repository on `main`: a system
kept in Hopsworks only commits there, and a new GitHub repository is created
from it. A target inside an existing repository stays part of it.

With `--phase`, copies that phase's stubs (PHASES) into an existing system the
same way, skipping any file it already has.

With `--example`, also writes the system.yaml of that entry of
example-systems.yaml, as a draft whose requirements are still pending.

The copy includes AGENTS.md, so a coding agent started in the system's
directory knows the system is built from
system.yaml and checks what a changed system.yaml means downstream.
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
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
# The stubs each build phase fills, created when it starts, so a system holds only what it uses.
PHASES = {
    "features": (
        "src/slug_pkg/feature_pipeline.py",
        "tests/unit/test_features.py",
        "benchmarks/benchmark_features.py",
    ),
    "train": (
        "src/slug_pkg/training_pipeline.py",
        "src/slug_pkg/evaluate.py",
        "src/slug_pkg/charts.py",
        "tests/unit/test_training.py",
    ),
    "infer": (
        "src/slug_pkg/inference_pipeline.py",
        "src/slug_pkg/predictor.py",
        "src/slug_pkg/parity.py",
        "tests/unit/test_inference.py",
        "tests/integration/test_parity.py",
        "benchmarks/benchmark_inference.py",
    ),
}
STUBS = {stub for stubs in PHASES.values() for stub in stubs}
JUNK = ("__pycache__", ".pytest_cache", ".ruff_cache")


def examples() -> dict:
    """The example systems, by slug."""
    return yaml.safe_load(EXAMPLES.read_text(encoding="utf-8"))


def example_doc(name: str, slug: str) -> dict:
    """The draft system.yaml of example `name` for a system called `slug`."""
    entry = examples().get(name)
    if entry is None:
        raise SystemExit(f"no example {name!r}; one of {sorted(examples())}")
    doc = _renamed(
        {key: value for key, value in entry.items() if key != "label"}, name, slug
    )
    doc["system"] = {
        **doc.get("system", {}),
        "slug": slug,
        "example": name,
        "version": "0.1.0",
        "status": "draft",
    }
    doc["requirements"] = {"status": "pending", **doc.get("requirements", {})}
    return {"schema_version": 1, **doc}


def _renamed(node, example: str, slug: str):
    """`node` with the example's own asset names (`<example>-app`, `example_pkg_x`, `examplename`) named after `slug`.

    Teardown takes a name carrying the slug as the system's own, and the example system may own the
    example's names.
    Generic names (`customers`, `query_model`) stay: teardown keeps those unless their description
    names the system.
    """
    if isinstance(node, dict):
        return {key: _renamed(value, example, slug) for key, value in node.items()}
    if isinstance(node, list):
        return [_renamed(value, example, slug) for value in node]
    if not isinstance(node, str) or example == slug:
        return node
    for old, new in (
        (example, slug),
        (example.replace("-", "_"), slug.replace("-", "_")),
        (example.replace("-", ""), slug.replace("-", "")),
    ):
        node = re.sub(rf"(?<![A-Za-z0-9]){re.escape(old)}", new, node)
    return node


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

    def ignore(directory: str, names: list[str]) -> set[str]:
        rel = Path(directory).relative_to(TEMPLATE)
        return {
            name
            for name in names
            if name in JUNK or name.endswith(".pyc") or (rel / name).as_posix() in STUBS
        }

    shutil.copytree(TEMPLATE, target, dirs_exist_ok=True, ignore=ignore)
    # A directory holding only stubs (benchmarks/) waits for its phase too.
    for directory in sorted(TEMPLATE.rglob("*"), reverse=True):
        copied = target / directory.relative_to(TEMPLATE)
        if copied.is_dir() and not any(copied.iterdir()):
            copied.rmdir()
    (target / "src" / "slug_pkg").rename(target / "src" / package)
    (target / "gitignore").rename(target / ".gitignore")
    _init_repository(target)
    for path in target.rglob("*"):
        if path.suffix in (".py", ".toml") and path.is_file():
            _name_package(path, package)
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


def _init_repository(target: Path) -> None:
    inside = subprocess.run(
        ["git", "-C", str(target), "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        text=True,
        check=False,
    )
    if inside.returncode != 0:
        subprocess.run(["git", "init", "-q", "-b", "main", str(target)], check=True)


def add_phase(target: Path, phase: str) -> list[Path]:
    """Copy `phase`'s stubs into the system at `target`; returns the files it created.

    A file the system already has is kept, so a resumed phase never loses its work.
    """
    if not (target / "system.yaml").exists():
        raise SystemExit(f"{target} holds no system.yaml; create the system first")
    package = target.name.replace("-", "_")
    created = []
    for stub in PHASES[phase]:
        path = target / stub.replace("slug_pkg", package)
        if path.exists():
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(TEMPLATE / stub, path)
        _name_package(path, package)
        created.append(path)
    return created


def _name_package(path: Path, package: str) -> None:
    text = path.read_text(encoding="utf-8")
    if "slug_pkg" in text:
        path.write_text(re.sub(r"\bslug_pkg\b", package, text), encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("target", type=Path)
    parser.add_argument("--example", choices=sorted(examples()))
    parser.add_argument("--phase", choices=sorted(PHASES))
    args = parser.parse_args()
    if args.phase and args.example:
        parser.error("--phase adds to an existing system; --example creates one")
    if args.phase:
        for path in add_phase(args.target.resolve(), args.phase):
            print(path)
    else:
        print(create(args.target.resolve(), args.example))
