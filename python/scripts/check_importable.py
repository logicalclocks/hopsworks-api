# ruff: noqa
#
#   Copyright 2026 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
"""Importability lint check for the four hopsworks packages.

Walks ``hopsworks``, ``hopsworks_common``, ``hsfs`` and ``hsml`` and attempts to
``importlib.import_module`` every module and subpackage.

The check is targeted at the HWORKS-2849 PEP-8 privatization refactor, where
mass ``sed`` renames (``foo`` -> ``_foo``) can leave behind stale re-exports
that the pytest suite never exercises, for example:

  * ``from hopsworks_common.util import convert_to_abs`` after the symbol became
    ``_convert_to_abs`` (raises ``ImportError: cannot import name ...``),
  * an ``__all__`` entry naming a symbol that no longer exists (an importer
    using ``from module import *`` then fails, and tooling/docs break),
  * a re-export shim (``hsfs/client/__init__.py``, ``hsfs/usage.py``, ...)
    importing a renamed name.

These are import-time ``ImportError``/``AttributeError``/``NameError`` that only
blow up when the offending module is first imported, so the regular test suite
can miss them entirely.

Run it as::

    uv run --project python python python/scripts/check_importable.py

It exits non-zero and prints a report (module path + exception type + message)
when any module is broken, so it can gate CI.

Optional-dependency gaps (e.g. ``polars``, ``great_expectations``,
``confluent_kafka``, ``pyspark`` not installed) are reported as skips, NOT
failures: a bare ``ModuleNotFoundError`` for a third-party package that is not
part of this repo is environment noise. A ``ModuleNotFoundError`` for one of
the four packages themselves, or any ``ImportError: cannot import name ...``,
is treated as a real failure.
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path


# The four packages that ship in the `hopsworks` distribution.
PACKAGES = ["hopsworks", "hopsworks_common", "hsfs", "hsml"]

# Path fragments to skip when walking for modules.
SKIP_DIR_NAMES = {"build", ".venv", "tests", "__pycache__", "templates", ".ruff_cache"}

# Generated protobuf modules: skip by default (noisy, machine generated, and not
# part of the rename surface). Toggle with --include-protobuf.
PROTOBUF_SUFFIXES = ("_pb2.py", "_pb2_grpc.py")

# Third-party optional dependencies. A *bare* `ModuleNotFoundError` naming one
# of these (or any name not owned by this repo) is classified as an optional-dep
# skip rather than a real failure. This list is informational/documentation;
# the actual rule keys off whether the missing module is one of our own
# packages -- see `_classify`.
OPTIONAL_DEPS = {
    "polars",
    "great_expectations",
    "confluent_kafka",
    "pyspark",
    "pyarrow",
    "delta",
    "deltalake",
    "trino",
    "sqlalchemy",
    "fastmcp",
    "uvicorn",
    "uvloop",
    "httptools",
    "pydantic",
    "moto",
    "typeguard",
    "tensorflow",
    "torch",
    "great_expectations",
}


@dataclass
class Report:
    imported: list[str] = field(default_factory=list)
    failures: list[tuple[str, str, str]] = field(default_factory=list)  # module, exc type, msg
    skips: list[tuple[str, str]] = field(default_factory=list)  # module, missing dep
    all_failures: list[tuple[str, str]] = field(default_factory=list)  # module, missing name


def _iter_modules(package_root: Path, package_name: str, include_protobuf: bool):
    """Yield dotted module names for every .py file under a package root."""
    for dirpath, dirnames, filenames in os.walk(package_root):
        # Prune skipped directories in place.
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]
        rel_dir = Path(dirpath).relative_to(package_root)
        for filename in sorted(filenames):
            if not filename.endswith(".py"):
                continue
            if not include_protobuf and filename.endswith(PROTOBUF_SUFFIXES):
                continue
            if filename == "__init__.py":
                parts = rel_dir.parts
                dotted = ".".join([package_name, *parts]) if parts else package_name
            else:
                stem = filename[: -len(".py")]
                parts = (*rel_dir.parts, stem)
                # rel_dir == "." for the top-level dir; filter the "." part.
                parts = tuple(p for p in parts if p not in (".", ""))
                dotted = ".".join([package_name, *parts])
            yield dotted


def _missing_module_name(exc: ModuleNotFoundError) -> str | None:
    """Return the top-level name that a ModuleNotFoundError reports as missing."""
    name = getattr(exc, "name", None)
    if name:
        return name.split(".")[0]
    # Fall back to parsing the message.
    msg = str(exc)
    marker = "No module named "
    if marker in msg:
        return msg.split(marker, 1)[1].strip().strip("'\"").split(".")[0]
    return None


def _classify(module: str, exc: BaseException) -> str:
    """Classify an import exception as 'skip' or 'fail'.

    A missing *third-party optional dependency* is a skip. Anything that points
    at our own code being broken (stale `from X import Y`, NameError,
    AttributeError, a ModuleNotFoundError for one of our own packages) is a
    real failure.
    """
    # `ImportError: cannot import name 'X' from 'Y'` is a real failure even
    # though ImportError is the base of ModuleNotFoundError. Check this first.
    if isinstance(exc, ImportError) and not isinstance(exc, ModuleNotFoundError):
        return "fail"

    if isinstance(exc, ModuleNotFoundError):
        missing = _missing_module_name(exc)
        if missing is None:
            return "fail"
        # Missing one of our own packages/submodules => the repo is broken.
        if missing in PACKAGES:
            return "fail"
        # Otherwise it's a third-party dependency that isn't installed. Treat as
        # an optional-dependency skip regardless of whether it's in our curated
        # OPTIONAL_DEPS set -- a missing external package is never this repo's
        # import-correctness bug.
        return "skip"

    # AttributeError / NameError / anything else at import time = broken module.
    return "fail"


def _format_exc(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


def check(include_protobuf: bool, verbose: bool) -> Report:
    report = Report()

    for package_name in PACKAGES:
        package_root = REPO_PYTHON / package_name
        if not package_root.is_dir():
            report.failures.append(
                (package_name, "MissingPackage", f"{package_root} does not exist")
            )
            continue

        for dotted in _iter_modules(package_root, package_name, include_protobuf):
            try:
                module = importlib.import_module(dotted)
            except ModuleNotFoundError as exc:
                verdict = _classify(dotted, exc)
                if verdict == "skip":
                    report.skips.append((dotted, _missing_module_name(exc) or "?"))
                    if verbose:
                        print(f"SKIP {dotted} (missing {_missing_module_name(exc)})")
                else:
                    report.failures.append(
                        (dotted, type(exc).__name__, str(exc))
                    )
                continue
            except BaseException as exc:  # noqa: BLE001 - we want everything
                verdict = _classify(dotted, exc)
                if verdict == "skip":
                    report.skips.append((dotted, "optional dep"))
                else:
                    report.failures.append((dotted, type(exc).__name__, str(exc)))
                    if verbose:
                        traceback.print_exc()
                continue

            report.imported.append(dotted)
            if verbose:
                print(f"OK   {dotted}")

            # Validate __all__ entries resolve as attributes.
            declared = getattr(module, "__all__", None)
            if declared is not None:
                for name in declared:
                    if not hasattr(module, name):
                        report.all_failures.append((dotted, name))

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-protobuf",
        action="store_true",
        help="Also import generated *_pb2.py / *_pb2_grpc.py modules.",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Print every module as it is checked."
    )
    args = parser.parse_args()

    report = check(include_protobuf=args.include_protobuf, verbose=args.verbose)

    print()
    print("=" * 72)
    print("IMPORTABILITY CHECK SUMMARY")
    print("=" * 72)
    print(f"  imported OK : {len(report.imported)}")
    print(f"  skipped     : {len(report.skips)} (missing optional dependency)")
    print(f"  import fails: {len(report.failures)}")
    print(f"  __all__ fails: {len(report.all_failures)}")

    if report.skips:
        # Aggregate skips by missing dependency for a compact view.
        by_dep: dict[str, int] = {}
        for _module, dep in report.skips:
            by_dep[dep] = by_dep.get(dep, 0) + 1
        print()
        print("Skipped (optional dependency not installed):")
        for dep, count in sorted(by_dep.items()):
            print(f"  - {dep}: {count} module(s)")

    if report.failures:
        print()
        print("-" * 72)
        print("IMPORT FAILURES (these are real bugs -- stale imports / broken code):")
        print("-" * 72)
        for module, exc_type, msg in report.failures:
            print(f"  [{exc_type}] {module}")
            print(f"      {msg}")

    if report.all_failures:
        print()
        print("-" * 72)
        print("__all__ FAILURES (name in __all__ does not resolve after import):")
        print("-" * 72)
        for module, name in report.all_failures:
            print(f"  {module}: __all__ entry '{name}' is not defined")

    ok = not report.failures and not report.all_failures
    print()
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


# Resolve the python/ source root relative to this file: python/scripts/<here>.
REPO_PYTHON = Path(__file__).resolve().parent.parent

# Make the four packages importable regardless of how the script is invoked.
if str(REPO_PYTHON) not in sys.path:
    sys.path.insert(0, str(REPO_PYTHON))


if __name__ == "__main__":
    raise SystemExit(main())
