"""Factory definitions (apiVersion hopsworks.ai/factory/v1): reading, checking, and what a factory writes.

The rules match hopsworks-ee's FactoryDefinitions, which checks every definition the cluster
stores; `hops factory validate` runs them here, without a cluster.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import yaml


if TYPE_CHECKING:
    from pathlib import Path


API_VERSION = "hopsworks.ai/factory/v1"
# Bytes of UTF-8, the size of the cluster's column for a definition.
MAX_LENGTH = 29000
BUILTINS = ("mlsystem", "medallion")
COMPONENTS = ("mlsystem.requirements", "medallion.silver", "medallion.gold")
FIELD_TYPES = (
    "slug",
    "text",
    "textarea",
    "number",
    "boolean",
    "choice",
    "multichoice",
    "feature_groups",
    "component",
)
NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
ID = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
PATH = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)*$")
# Blocks of system.yaml a phase cannot be named after.
RESERVED_PHASES = ("system", "factory", "schema_version")


class _UniqueKeyLoader(yaml.SafeLoader):
    """A safe loader that refuses a key given twice, as the backend's does."""


def _mapping(loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False):
    keys = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in keys:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark
            )
        keys.add(key)
    return loader.construct_mapping(node, deep)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping
)


def parse(text: str) -> dict:
    """The definition as a dict; raises ValueError when it is empty, too long, not YAML, or not a mapping."""
    if not text or not text.strip():
        raise ValueError("The definition is empty.")
    size = len(text.encode("utf-8"))
    if size > MAX_LENGTH:
        raise ValueError(f"The definition is {size} bytes; the limit is {MAX_LENGTH}.")
    try:
        doc = yaml.load(text, Loader=_UniqueKeyLoader)  # noqa: S506 - a SafeLoader subclass
    except yaml.YAMLError as exc:
        raise ValueError(f"The definition is not valid YAML: {exc}") from exc
    if not isinstance(doc, dict):
        raise ValueError("The definition must be a YAML mapping.")
    return doc


def problems(doc: dict) -> list[str]:
    """What is wrong with a parsed definition, one line each; empty when it is valid."""
    found: list[str] = []
    if doc.get("apiVersion") != API_VERSION:
        found.append(f"apiVersion must be {API_VERSION}.")
    if doc.get("kind") != "Factory":
        found.append("kind must be Factory.")
    if not isinstance(doc.get("name"), str) or not NAME.match(doc["name"]):
        found.append(
            "name must be lowercase letters, digits and hyphens, starting with a letter, at most 63."
        )
    title = doc.get("title")
    if not _text(title) or len(title) > 255:
        found.append("title must be text of at most 255 characters.")
    if "description" in doc and not isinstance(doc["description"], str):
        found.append("description must be text.")
    has_component = _check_form(doc.get("form"), found)
    _check_phases(doc.get("phases"), found)
    _check_build(doc.get("build"), found)
    _check_list(doc.get("list"), found)
    if (
        not has_component
        and "form.sections must be a non-empty list." not in found
        and not any(f.get("type") == "slug" for f in fields(doc))
    ):
        found.append(
            "The form needs a field of type slug, which names the system's directory."
        )
    return found


def text_problems(text: str) -> list[str]:
    """The problems of YAML text, including a parse failure."""
    try:
        return problems(parse(text))
    except ValueError as exc:
        return [str(exc)]


def fields(doc: dict) -> list[dict]:
    """Every field of the form, in order."""
    sections = (doc.get("form") or {}).get("sections") or []
    return [
        f
        for s in sections
        if isinstance(s, dict)
        for f in (s.get("fields") or [])
        if isinstance(f, dict)
    ]


def _check_form(form: Any, found: list[str]) -> bool:
    sections = form.get("sections") if isinstance(form, dict) else None
    if not isinstance(sections, list) or not sections:
        found.append("form.sections must be a non-empty list.")
        return False
    section_ids: set[str] = set()
    earlier: set[str] = set()
    has_component = False
    for s, section in enumerate(sections):
        where = f"form.sections[{s}]"
        if not isinstance(section, dict):
            found.append(f"{where} must be a mapping.")
            continue
        sid = section.get("id")
        if not isinstance(sid, str) or not ID.match(sid) or sid in section_ids:
            found.append(f"{where}.id must be a unique lowercase identifier.")
        else:
            section_ids.add(sid)
        if not _text(section.get("title")):
            found.append(f"{where}.title is required.")
        if "collapsed" in section and not isinstance(section["collapsed"], bool):
            found.append(f"{where}.collapsed must be true or false.")
        items = section.get("fields")
        if not isinstance(items, list) or not items:
            found.append(f"{where}.fields must be a non-empty list.")
            continue
        for f, field in enumerate(items):
            has_component |= _check_field(field, f"{where}.fields[{f}]", earlier, found)
    return has_component


def _check_field(field: Any, where: str, earlier: set[str], found: list[str]) -> bool:
    if not isinstance(field, dict):
        found.append(f"{where} must be a mapping.")
        return False
    fid, kind = field.get("id"), field.get("type")
    if not isinstance(fid, str) or not ID.match(fid) or fid in earlier:
        found.append(f"{where}.id must be a unique lowercase identifier.")
    if kind not in FIELD_TYPES:
        found.append(f"{where}.type must be one of {', '.join(sorted(FIELD_TYPES))}.")
    if not _text(field.get("label")):
        found.append(f"{where}.label is required.")
    if "required" in field and not isinstance(field["required"], bool):
        found.append(f"{where}.required must be true or false.")
    if kind in ("choice", "multichoice"):
        options = field.get("options")
        if (
            not isinstance(options, list)
            or not options
            or not all(_text(o) for o in options)
        ):
            found.append(f"{where}.options must be a non-empty list of text.")
    for bound in ("min", "max"):
        if bound in field and (
            isinstance(field[bound], bool) or not isinstance(field[bound], (int, float))
        ):
            found.append(f"{where}.{bound} must be a number.")
    if kind == "component" and field.get("component") not in COMPONENTS:
        found.append(
            f"{where}.component must be one of {', '.join(sorted(COMPONENTS))}."
        )
    when = field.get("when")
    if when is not None and (
        not isinstance(when, dict)
        or when.get("field") not in earlier
        or "equals" not in when
    ):
        found.append(
            f"{where}.when must name an earlier field and the value it equals."
        )
    if isinstance(fid, str):
        earlier.add(fid)
    return kind == "component"


def _check_phases(phases: Any, found: list[str]) -> None:
    if not isinstance(phases, list) or not phases:
        found.append("phases must be a non-empty list.")
        return
    keys: set[str] = set()
    for p, phase in enumerate(phases):
        phase = phase if isinstance(phase, dict) else {}
        key = phase.get("key")
        if not isinstance(key, str) or not ID.match(key) or key in keys:
            found.append(f"phases[{p}].key must be a unique lowercase identifier.")
        elif key in RESERVED_PHASES:
            found.append(f"phases[{p}].key cannot be {key}, a block of system.yaml.")
        else:
            keys.add(key)
        if not _text(phase.get("label")):
            found.append(f"phases[{p}].label is required.")
        if "minutes" in phase and (
            isinstance(phase["minutes"], bool)
            or not isinstance(phase["minutes"], (int, float))
        ):
            found.append(f"phases[{p}].minutes must be a number.")


def _check_build(build: Any, found: list[str]) -> None:
    if not isinstance(build, dict):
        found.append("build is required: builtin or instructions.")
        return
    builtin = build.get("builtin")
    if builtin is not None and builtin not in BUILTINS:
        found.append(f"build.builtin must be one of {', '.join(sorted(BUILTINS))}.")
    if builtin is None and not _text(build.get("instructions")):
        found.append("build.instructions is required when build.builtin is not set.")
    if "instructions" in build and not isinstance(build["instructions"], str):
        found.append("build.instructions must be text.")
    skills = build.get("skills")
    if skills is not None and (
        not isinstance(skills, list)
        or not all(isinstance(s, str) and NAME.match(s) for s in skills)
    ):
        found.append("build.skills must be a list of skill names.")


def _check_list(listing: Any, found: list[str]) -> None:
    if listing is None:
        return
    columns = listing.get("columns") if isinstance(listing, dict) else None
    if not isinstance(columns, list):
        found.append("list.columns must be a list.")
        return
    for c, column in enumerate(columns):
        column = column if isinstance(column, dict) else {}
        if (
            not _text(column.get("label"))
            or not isinstance(column.get("from"), str)
            or not PATH.match(column["from"])
        ):
            found.append(f"list.columns[{c}] needs a label and a dotted path in from.")


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


# region Answers


def visible(doc: dict, answers: dict) -> list[dict]:
    """The fields the form shows for these answers: those without `when`, or whose condition holds."""
    shown = []
    for field in fields(doc):
        when = field.get("when")
        if when and answers.get(when.get("field")) != when.get("equals"):
            continue
        shown.append(field)
    return shown


def answer_problems(doc: dict, answers: dict) -> list[str]:
    """What keeps the answers from creating a system, one line each, as the form checks them."""
    found = []
    for field in visible(doc, answers):
        fid, kind, label = field["id"], field["type"], field["label"]
        value = answers.get(fid)
        empty = value is None or value == "" or value == []
        if empty:
            if field.get("required") or kind == "slug":
                found.append(f"{label} is required.")
            continue
        if kind == "slug" and not (isinstance(value, str) and SLUG.match(value)):
            found.append(
                f"{label} must be lowercase letters, digits and hyphens, starting with a letter."
            )
        elif kind == "number":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                found.append(f"{label} must be a number.")
            elif ("min" in field and value < field["min"]) or (
                "max" in field and value > field["max"]
            ):
                found.append(f"{label} is out of range.")
        elif kind == "boolean" and not isinstance(value, bool):
            found.append(f"{label} must be true or false.")
        elif kind == "choice" and value not in field.get("options", []):
            found.append(f"{label} must be one of {', '.join(field['options'])}.")
        elif kind == "multichoice" and not (
            isinstance(value, list) and set(value) <= set(field.get("options", []))
        ):
            found.append(f"{label} must be some of {', '.join(field['options'])}.")
        elif kind == "feature_groups" and not (
            isinstance(value, list)
            and all(isinstance(v, dict) and v.get("name") for v in value)
        ):
            found.append(f"{label} must be a list of feature groups.")
    return found


def slug_of(doc: dict, answers: dict) -> str | None:
    """The system's slug: the answer to the form's slug field."""
    for field in fields(doc):
        if field.get("type") == "slug":
            return answers.get(field["id"])
    return None


# endregion

# region What a factory writes


def record_factory(meta: dict | None, target: Path) -> None:
    """Record the factory that built the system in its system.yaml, when a project factory delegated to a built-in.

    `meta` is what `hops factory <name> create` put in the click context: the factory's
    name and version, its instructions, and the answers to its own fields, which go to
    `requirements.extra`.
    """
    if not meta:
        return
    spec = target / "system.yaml"
    doc = yaml.safe_load(spec.read_text(encoding="utf-8")) or {}
    record = {"name": meta["name"], "version": meta["version"]}
    if meta.get("instructions"):
        record["instructions"] = meta["instructions"]
    doc["factory"] = record
    if meta.get("extra"):
        doc.setdefault("requirements", {})["extra"] = meta["extra"]
    spec.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")


def factory_name(ctx: Any, default: str) -> str:
    """The factory a built-in's create registers with: the project factory delegating to it, else the built-in."""
    meta = getattr(ctx, "meta", {}).get(META) if ctx is not None else None
    return meta["name"] if meta else default


META = "hops.factory"

# endregion
