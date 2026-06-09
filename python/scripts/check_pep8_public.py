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

r"""Enforce the PEP-8 / ``@public`` invariant across the four Hopsworks packages.

Every function and method in ``hopsworks``, ``hopsworks_common``, ``hsfs`` and
``hsml`` must be one of:

* PEP-8 private: the name starts with ``_`` (this also covers dunders), OR
* annotated ``@public`` (from ``hopsworks_apigen``), OR
* annotated ``@deprecated`` (from ``hopsworks_apigen``), OR
* a recognized carve-out (properties, serialization helpers, framework hooks,
  polymorphic overrides of public/abstract base methods, ...).

Any public-named symbol (no leading ``_``) that is none of these is a
violation: it is an internal symbol that should be ``_``-prefixed, or a
genuinely public one that is missing its ``@public`` annotation.

The check parses the packages statically with griffe and never imports them.

Run it with::

    uv run --no-project --with griffe --with hopsworks-apigen \\
        python python/scripts/check_pep8_public.py

(``--with /path/to/hopsworks-apigen`` for a local checkout.)

Exits non-zero if any violation is found.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

import griffe
from hopsworks_apigen.griffe import HopsworksApigenGriffe


# The four packages whose public surface is governed by the invariant.
PACKAGES = ["hopsworks_common", "hopsworks", "hsfs", "hsml"]

# Path fragments (relative to the python root, using "/" separators) whose
# subtrees are excluded from the check entirely. ``istio`` is vendored from
# KServe and mirrors its API verbatim; ``cli``/``mcp`` are not part of the
# governed surface.
EXCLUDED_PATH_PARTS = {
    "cli",
    "mcp",
    "build",
    ".venv",
    "tests",
    "__pycache__",
    "templates",
}
# The istio subtree is vendored: match on the joined path.
EXCLUDED_PATH_SEGMENTS = ["client/istio/"]

# Methods that serialize/deserialize an object and are deliberately public-named
# without ``@public`` (they are part of the internal JSON protocol).
SERIALIZATION_NAMES = {
    "json",
    "to_dict",
    "to_json",
    "to_json_dict",
    "from_response_json",
    "from_response_json_single",
    "from_response_json_list",
    "from_json",
    "from_dict",
    "update_from_response_json",
    "extract_fields_from_json",
    "from_string",
    "from_user_input",
}

# importlib loader protocol hooks.
IMPORTLIB_HOOK_NAMES = {"create_module", "exec_module", "find_spec"}

# Fallback allowlist of fully-qualified ``Class.method`` overrides that are part
# of a public interface but whose base method cannot be resolved structurally
# (e.g. the base lives behind an alias griffe could not follow). Structural
# detection via the polymorphism rule is strongly preferred and currently
# rescues every known interface override (``StorageConnector`` subclasses'
# ``spark_options``/``read``, ``AsyncFeatureLogger.init``/``log``), so this set
# is intentionally empty. Add precise ``Class.method`` entries here only if a
# genuine public override ever starts showing up as a false positive — never
# bare method names, which would mask unrelated internal helpers (e.g.
# ``VectorDbClient.read``, ``TrainingDatasetEngine.read``).
POLYMORPHISM_FALLBACK_QUALNAMES: set[str] = set()


def _is_excluded_filepath(filepath: Path, root: Path) -> bool:
    """Whether a module file lies in an excluded subtree or is generated."""
    try:
        rel = filepath.relative_to(root)
    except ValueError:
        rel = filepath
    parts = rel.parts
    if any(part in EXCLUDED_PATH_PARTS for part in parts):
        return True
    rel_str = rel.as_posix()
    if any(seg in rel_str for seg in EXCLUDED_PATH_SEGMENTS):
        return True
    return filepath.name.endswith(("_pb2.py", "_pb2_grpc.py"))


def _apigen_extra(obj: griffe.Object) -> dict:
    """Return the hopsworks_apigen extra dict, or an empty one if absent."""
    extra = obj.extra.get("hopsworks_apigen")
    return extra if isinstance(extra, dict) else {}


def _has_public(obj: griffe.Object) -> bool:
    return bool(_apigen_extra(obj).get("is_public"))


def _has_deprecated(obj: griffe.Object) -> bool:
    return _apigen_extra(obj).get("deprecated") is not None


def _decorator_paths(func: griffe.Function) -> list[str]:
    return [d.callable_path for d in (func.decorators or [])]


def _is_property_like(func: griffe.Function) -> bool:
    """Property getters, setters, deleters and cached properties."""
    labels = func.labels
    if "property" in labels:
        return True
    for path in _decorator_paths(func):
        # e.g. "<name>.setter", "<name>.deleter", "functools.cached_property"
        if path.endswith((".setter", ".deleter")):
            return True
        if path.split(".")[-1] in {"property", "cached_property"}:
            return True
    return False


def _base_name_matches(canonical: str, simple: str, suffixes: tuple[str, ...]) -> bool:
    last = canonical.rsplit(".", 1)[-1] if canonical else simple
    return last in suffixes or any(canonical.endswith(s) for s in suffixes)


def _class_base_names(cls: griffe.Class) -> list[tuple[str, str]]:
    """Return (canonical_path, simple_name) for each declared base."""
    out = []
    for base in cls.bases:
        canonical = getattr(base, "canonical_path", None) or ""
        simple = getattr(base, "name", None) or str(base)
        out.append((canonical, simple))
    return out


def _is_json_encoder_hook(func: griffe.Function, cls: griffe.Class | None) -> bool:
    if func.name != "default" or cls is None:
        return False
    for canonical, simple in _class_base_names(cls):
        if _base_name_matches(canonical, simple, ("JSONEncoder",)):
            return True
    return False


def _is_thread_hook(func: griffe.Function, cls: griffe.Class | None) -> bool:
    if func.name != "run" or cls is None:
        return False
    for canonical, simple in _class_base_names(cls):
        if _base_name_matches(canonical, simple, ("Thread", "threading.Thread")):
            return True
    return False


def _resolve_base_classes(
    cls: griffe.Class, collection: griffe.ModulesCollection
) -> list[griffe.Class]:
    """Resolve a class's declared bases to griffe Class objects, if loaded."""
    resolved = []
    for base in cls.bases:
        canonical = getattr(base, "canonical_path", None)
        if not canonical:
            continue
        try:
            obj = collection[canonical]
        except (KeyError, ValueError):
            continue
        if isinstance(obj, griffe.Alias):
            try:
                obj = obj.final_target
            except Exception:
                continue
        if getattr(obj, "kind", None) and obj.kind.value == "class":
            resolved.append(obj)
    return resolved


def _is_abstract(func: griffe.Function) -> bool:
    for path in _decorator_paths(func):
        if path.split(".")[-1] == "abstractmethod":
            return True
    return "abstractmethod" in func.labels


def _overrides_public_or_abstract(
    func: griffe.Function,
    cls: griffe.Class | None,
    collection: griffe.ModulesCollection,
    _seen: set[str] | None = None,
) -> bool:
    """Whether ``func`` overrides a base-class method that is public/abstract.

    Walks the resolved base classes (transitively) looking for a member of the
    same name that is either ``@public`` or marked ``@abstractmethod``.
    """
    if cls is None:
        return False
    if _seen is None:
        _seen = set()
    for base in _resolve_base_classes(cls, collection):
        if base.path in _seen:
            continue
        _seen.add(base.path)
        member = base.members.get(func.name)
        if (
            member is not None
            and getattr(member, "kind", None)
            and member.kind.value == "function"
            and (_has_public(member) or _is_abstract(member))
        ):
            return True
        if _overrides_public_or_abstract(func, base, collection, _seen):
            return True
    return False


def _compliance(
    func: griffe.Function,
    cls: griffe.Class | None,
    module: griffe.Module,
    collection: griffe.ModulesCollection,
) -> str | None:
    """Return None if compliant, else a short violation reason string."""
    name = func.name

    # Rule 1: PEP-8 private (also covers dunders).
    if name.startswith("_"):
        return None

    # Rule 2/3: @public / @deprecated.
    if _has_public(func):
        return None
    if _has_deprecated(func):
        return None

    # Rule 4: property / accessor.
    if _is_property_like(func):
        return None

    # Rule 5: serialization carve-out.
    if name in SERIALIZATION_NAMES:
        return None

    # Rule 6: JSON encoder hook.
    if _is_json_encoder_hook(func, cls):
        return None

    # Rule 7: threading hook.
    if _is_thread_hook(func, cls):
        return None

    # Rule 8: importlib protocol hooks.
    if name in IMPORTLIB_HOOK_NAMES:
        return None

    # Rule 9: warnings formatwarning hook (module-level function).
    if cls is None and name.endswith("_formatwarning"):
        return None

    # Rule 10: polymorphic override of a public/abstract base method.
    if cls is not None and _overrides_public_or_abstract(func, cls, collection):
        return None

    # Rule 10 fallback allowlist for known interface overrides that cannot be
    # resolved structurally (normally empty; see the constant's docstring).
    if cls is not None and f"{cls.name}.{name}" in POLYMORPHISM_FALLBACK_QUALNAMES:
        return None

    # Not compliant.
    if cls is None:
        return "public-named module-level function without @public/@deprecated"
    return "public-named method without @public/@deprecated"


def _walk(
    obj: griffe.Object,
    root: Path,
    collection: griffe.ModulesCollection,
    cls: griffe.Class | None,
    violations: list[tuple[str, int, str, str]],
    verbose: bool,
) -> None:
    for member in obj.members.values():
        if getattr(member, "is_alias", False):
            continue
        kind = member.kind.value
        if kind == "module":
            fp = member.filepath
            if isinstance(fp, list):  # namespace package
                fp = fp[0] if fp else None
            if fp is not None and _is_excluded_filepath(Path(fp), root):
                if verbose:
                    print(f"  [skip excluded module] {member.path}", file=sys.stderr)
                continue
            _walk(member, root, collection, None, violations, verbose)
        elif kind == "class":
            _walk(member, root, collection, member, violations, verbose)
        elif kind == "function":
            reason = _compliance(member, cls, member.module, collection)
            if reason is not None:
                fp = member.filepath
                if isinstance(fp, list):
                    fp = fp[0] if fp else None
                fp_str = str(fp) if fp else "<unknown>"
                violations.append((fp_str, member.lineno or 0, member.path, reason))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        default=None,
        help="Path to the python source root (default: auto-detected).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
    args = parser.parse_args()

    if args.root:
        root = Path(args.root).resolve()
    else:
        # scripts/ lives directly under the python root.
        root = Path(__file__).resolve().parent.parent

    exts = griffe.Extensions()
    exts.add(HopsworksApigenGriffe())
    loader = griffe.GriffeLoader(extensions=exts, search_paths=[str(root)])

    for name in PACKAGES:
        loader.load(name, submodules=True)
    # Resolve aliases so base-class canonical paths point at real objects;
    # keep it within the loaded packages (no external imports).
    try:
        loader.resolve_aliases(external=False, implicit=False)
    except Exception as e:  # pragma: no cover - best effort
        if args.verbose:
            print(f"alias resolution warning: {e}", file=sys.stderr)

    collection = loader.modules_collection
    violations: list[tuple[str, int, str, str]] = []
    for name in PACKAGES:
        pkg = collection[name]
        _walk(pkg, root, collection, None, violations, args.verbose)

    if not violations:
        print("PEP-8 / @public check passed: no violations found.")
        return 0

    # De-duplicate (a symbol can be reached once) and group by file.
    by_file: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    seen: set[str] = set()
    for filepath, lineno, path, reason in violations:
        if path in seen:
            continue
        seen.add(path)
        by_file[filepath].append((lineno, path, reason))

    total = sum(len(v) for v in by_file.values())
    print(f"PEP-8 / @public check FAILED: {total} violation(s).\n")
    for filepath in sorted(by_file):
        try:
            shown = str(Path(filepath).resolve().relative_to(root))
        except ValueError:
            shown = filepath
        print(f"{shown}:")
        for lineno, path, reason in sorted(by_file[filepath]):
            print(f"  L{lineno}: {path} — {reason}")
        print()

    return 1


if __name__ == "__main__":
    sys.exit(main())
