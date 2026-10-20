# ruff: noqa: INP001
"""Set fields of this system's system.yaml, validated, in one atomic write.

The requirements interview records each answer the moment it is given:

    python <slug>/set.py requirements.problem.description="predict churn next month" \
        requirements.system_type=batch 'requirements.sla.batch={"cadence": "daily"}'

A value is parsed as YAML, so `3`, `true`, `[a, b]` and `{k: v}` keep their types; anything
else is a string. `key+=value` appends to a list. The result is checked by
tests/unit/test_system_yaml.py (draft rules while `requirements.status` is `pending`) and
renamed into place only when it passes, so an invalid write never replaces a valid file. With
the hops CLI installed the write also takes the system's write lock and is redone on the newer
file when another writer changed it meanwhile (hopsworks.cli.system_doc).
"""

from __future__ import annotations

import os
import sys
import tempfile
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


def main(argv: list[str]) -> int:
    """Apply every assignment and validate, then write the file the way every writer of it does."""
    if not argv:
        sys.exit(__doc__)
    validate = _validator().validate
    try:
        from hopsworks.cli import system_doc
    except ImportError:
        # Without the hops CLI: no lock or conflict check, but still never a partial file.
        return _write_alone(argv, validate)

    def change(doc: dict) -> None:
        for assignment in argv:
            apply(doc, assignment)

    try:
        system_doc.update(ROOT, change, validate)
    except system_doc.Invalid as exc:
        print(exc.message, file=sys.stderr)
        return 1
    return 0


def _write_alone(argv: list[str], validate) -> int:
    path = ROOT / "system.yaml"
    doc = (yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}) or {}
    for assignment in argv:
        apply(doc, assignment)
    problems = validate(doc)
    if problems:
        for line in problems:
            print(line, file=sys.stderr)
        return 1
    fd, tmp = tempfile.mkstemp(dir=ROOT, prefix=".system.yaml.")
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        out.write(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100))
        out.flush()
        os.fsync(out.fileno())
    os.replace(tmp, path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
