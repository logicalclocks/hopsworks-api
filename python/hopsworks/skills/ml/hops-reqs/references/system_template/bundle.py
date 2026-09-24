# ruff: noqa: INP001
"""Build the immutable run bundle a job fetches at start.

A bundle is what crosses from the repository into a job: system.yaml as it
stands, pyproject.toml and src/, plus tests/ and benchmarks/ for the tests and
benchmark jobs, and manifest.json with the run id, the commit and the sha256 of
every file. The entrypoint prelude refuses a bundle whose files do not match
their manifest. A bundle is never rewritten; a new run gets a new id.

    python <slug>/bundle.py make train-3-1                # writes runs/train-3-1/bundle.tar.gz
    python <slug>/bundle.py make tests-features-1 --with-tests
    hops files upload runs/train-3-1/bundle.tar.gz Resources/<slug>/runs/train-3-1/

The contract is hops-reqs/references/bundle.md.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
import sys
import tarfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ALWAYS = ["system.yaml", "pyproject.toml", "src"]
WITH_TESTS = ["tests", "benchmarks"]
SKIP_PARTS = {"__pycache__", ".pytest_cache"}


def _files(root: Path, entries: list[str]) -> list[Path]:
    found: list[Path] = []
    for entry in entries:
        path = root / entry
        if path.is_file():
            found.append(path)
        elif path.is_dir():
            found.extend(
                p
                for p in sorted(path.rglob("*"))
                if p.is_file()
                and not SKIP_PARTS.intersection(p.parts)
                and p.suffix != ".pyc"
            )
    return found


def _commit(root: Path) -> str:
    """The HEAD commit, refusing uncommitted changes to what goes in the bundle.

    A run must be reproducible from its commit, so the files in the bundle have
    to be the files at that commit.
    """
    head = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--", *ALWAYS, *WITH_TESTS],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if dirty:
        raise SystemExit(f"uncommitted changes would go into the bundle:\n{dirty}")
    return head


def make(
    run_id: str,
    *,
    root: Path = ROOT,
    with_tests: bool = False,
    commit: str | None = None,
    slug: str | None = None,
    out: Path | None = None,
) -> Path:
    """Write runs/<run_id>/bundle.tar.gz and return its path.

    Refuses to overwrite an existing bundle: a run id names one bundle forever.
    """
    target = out or root / "runs" / run_id / "bundle.tar.gz"
    if target.exists():
        raise SystemExit(
            f"{target} exists; a bundle is never rewritten, use a new run id"
        )
    entries = ALWAYS + (WITH_TESTS if with_tests else [])
    files = _files(root, entries)
    manifest = {
        "run_id": run_id,
        "commit": commit or _commit(root),
        "slug": slug or root.name,
        "files": {
            p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in files
        },
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(target, "w:gz") as tar:
        for path in files:
            tar.add(path, arcname=path.relative_to(root).as_posix())
        payload = json.dumps(manifest, indent=2, sort_keys=True).encode()
        info = tarfile.TarInfo("manifest.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return target


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point: `make <run_id> [--with-tests]`."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    make_parser = sub.add_parser("make", help="build runs/<run_id>/bundle.tar.gz")
    make_parser.add_argument("run_id")
    make_parser.add_argument("--with-tests", action="store_true")
    args = parser.parse_args(argv)
    print(make(args.run_id, with_tests=args.with_tests))
    return 0


if __name__ == "__main__":
    sys.exit(main())
