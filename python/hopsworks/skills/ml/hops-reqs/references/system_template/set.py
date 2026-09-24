# ruff: noqa: INP001
"""Set fields of this system's system.yaml, validated, in one atomic write.

The requirements interview records each answer the moment it is given:

    python <slug>/set.py requirements.problem.description="predict churn next month" \
        requirements.system_type=batch 'requirements.sla.batch={"cadence": "daily"}'

A value is parsed as YAML, so `3`, `true`, `[a, b]` and `{k: v}` keep their types; anything
else is a string. `key+=value` appends to a list. The result is checked by
tests/unit/test_system_yaml.py (draft rules while `requirements.status` is `pending`) and
renamed into place only when it passes, so an invalid write never replaces a valid file.

In a Hopsworks terminal every write also registers the system under
`$HOPSFS_USER_HOME_DIR/.hops/builds/<slug>.json`, which is how the Hopsworks UI finds
the ML systems being built and tracks their progress. A system outside HopsFS is not
registered, since the UI cannot read it.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parent


def _validator():
    import importlib.util

    path = ROOT / "tests" / "unit" / "test_system_yaml.py"
    spec = importlib.util.spec_from_file_location("test_system_yaml", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def apply(doc: dict, assignment: str) -> None:
    """Apply one `dotted.key=value` or `dotted.key+=value` to the document."""
    key, sep, raw = assignment.partition("=")
    if not key or not sep:
        raise SystemExit(f"not an assignment: {assignment!r}")
    append = key.endswith("+")
    key = key.removesuffix("+")
    value = yaml.safe_load(raw) if raw.strip() else ""
    parts = key.split(".")
    node = doc
    for part in parts[:-1]:
        node = node.setdefault(part, {})
        if not isinstance(node, dict):
            raise SystemExit(f"{key}: {part} is not a mapping")
    last = parts[-1]
    if append:
        node.setdefault(last, [])
        if not isinstance(node[last], list):
            raise SystemExit(f"{key} is not a list")
        node[last].append(value)
    else:
        node[last] = value


def _write_atomically(path: Path, text: str) -> None:
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        out.write(text)
    os.replace(tmp, path)


def register(doc: dict, root: Path = ROOT) -> Path | None:
    """Record this system in the user's HopsFS home so the Hopsworks UI can track it.

    Returns the registry file, or None outside a Hopsworks terminal or outside HopsFS.
    """
    home = os.environ.get("HOPSFS_USER_HOME_DIR")
    if not home or "/Users/" not in home:
        return None
    mount = Path(home.split("/Users/", 1)[0])
    try:
        relative = root.resolve().relative_to(mount.resolve())
    except ValueError:
        return None
    slug = (doc.get("system") or {}).get("slug") or root.name
    builds = Path(home) / ".hops" / "builds"
    builds.mkdir(parents=True, exist_ok=True)
    entry = {
        "slug": slug,
        "path": f"{relative.as_posix()}/system.yaml",
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    target = builds / f"{slug}.json"
    _write_atomically(target, json.dumps(entry) + "\n")
    return target


def main(argv: list[str]) -> int:
    """Apply every assignment, validate, and write the file atomically."""
    if not argv:
        sys.exit(__doc__)
    path = ROOT / "system.yaml"
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    doc = doc or {}
    for assignment in argv:
        apply(doc, assignment)
    problems = _validator().validate(doc)
    if problems:
        for line in problems:
            print(line, file=sys.stderr)
        return 1
    _write_atomically(
        path, yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100)
    )
    try:
        register(doc)
    except OSError as exc:
        # The UI's view is a convenience; the system.yaml write already succeeded.
        print(f"not registered for the Hopsworks UI: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
