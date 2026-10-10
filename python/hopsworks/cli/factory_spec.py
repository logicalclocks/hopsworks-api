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
# The built-in factories; a project factory cannot take their names.
BUILTINS = (
    "ml-batch",
    "ml-realtime",
    "ml-agent",
    "analytics-bronze",
    "analytics-silver",
    "analytics-gold",
    "analytics-pipeline",
    "analytics-ingestion",
)
# The builds a factory can hand its answers to instead of writing its own instructions.
BUILTIN_BUILDS = ("mlsystem", "analytics-bronze", "analytics-silver", "analytics-gold")
FIELD_TYPES = (
    "slug",
    "text",
    "textarea",
    "number",
    "boolean",
    "choice",
    "multichoice",
    "feature_group",
    "feature_groups",
    "list",
    "account_env",
    "secrets",
    "entry",
    "repository",
)
NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
ID = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
SLUG = re.compile(r"^[a-z][a-z0-9-]*$")
PATH = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)*$")
ENV = re.compile(r"^[A-Z][A-Z0-9_]{0,62}$")
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
    """The definition as a dict; raises ValueError when it is empty, too long, not YAML, or not a mapping.

    Args:
        text: The definition's YAML.

    Returns:
        The definition.
    """
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
    """What is wrong with a parsed definition, one line each; empty when it is valid.

    Args:
        doc: The parsed definition.

    Returns:
        The problems.
    """
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
    ids = _check_form(doc.get("form"), "form", False, found)
    _check_phases(doc.get("phases"), found)
    _check_build(doc.get("build"), found)
    _check_list(doc.get("list"), found)
    _check_presets(doc.get("presets"), ids, found)
    _check_changes(doc.get("changes"), found)
    return found


def text_problems(text: str) -> list[str]:
    """The problems of YAML text, including a parse failure.

    Args:
        text: The definition's YAML.

    Returns:
        The problems.
    """
    try:
        return problems(parse(text))
    except ValueError as exc:
        return [str(exc)]


def fields(doc: dict) -> list[dict]:
    """Every top-level field of the create form, in order.

    Args:
        doc: The parsed definition.

    Returns:
        The fields.
    """
    return form_fields(doc.get("form"))


def form_fields(form: Any) -> list[dict]:
    """Every top-level field of a form, the create form or a change's, in order.

    Args:
        form: The form.

    Returns:
        The fields.
    """
    sections = (form if isinstance(form, dict) else {}).get("sections") or []
    return [
        f
        for s in sections
        if isinstance(s, dict)
        for f in (s.get("fields") or [])
        if isinstance(f, dict)
    ]


def _check_form(form: Any, path: str, change: bool, found: list[str]) -> set[str]:
    """Check a form: the create form, which names the system with a slug field, or a change's, which may pick entries of system.yaml."""
    sections = form.get("sections") if isinstance(form, dict) else None
    ids: set[str] = set()
    if not isinstance(sections, list) or not sections:
        found.append(f"{path}.sections must be a non-empty list.")
        return ids
    section_ids: set[str] = set()
    keys: set[str] = set()
    slug = False
    for s, section in enumerate(sections):
        where = f"{path}.sections[{s}]"
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
        slug |= _check_fields(
            section.get("fields"), f"{where}.fields", ids, keys, True, change, found
        )
    if not slug and not change:
        found.append(
            "The form needs a field of type slug, which names the system's directory."
        )
    return ids


def _check_fields(
    items: Any,
    where: str,
    earlier: set[str],
    keys: set[str],
    top: bool,
    change: bool,
    found: list[str],
) -> bool:
    if not isinstance(items, list) or not items:
        found.append(f"{where} must be a non-empty list.")
        return False
    slug = False
    for f, field in enumerate(items):
        slug |= _check_field(field, f"{where}[{f}]", earlier, keys, top, change, found)
    return slug


def _check_field(
    field: Any,
    where: str,
    earlier: set[str],
    keys: set[str],
    top: bool,
    change: bool,
    found: list[str],
) -> bool:
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
    key = field.get("key")
    if key is not None:
        if not isinstance(key, str) or not PATH.match(key):
            found.append(f"{where}.key must be a dotted path of lowercase identifiers.")
        elif key in keys:
            found.append(f"{where}.key {key} is used by another field.")
        else:
            keys.add(key)
    elif isinstance(fid, str):
        if fid in keys:
            found.append(f"{where}.id {fid} is used as another field's key.")
        keys.add(fid)
    if kind in ("choice", "multichoice"):
        options = field.get("options")
        if (
            not isinstance(options, list)
            or not options
            or not all(_option_value(o) for o in options)
        ):
            found.append(
                f"{where}.options must be a non-empty list of text or of {{value, label}}."
            )
    for bound in ("min", "max", "min_items", "max_items"):
        if bound in field and (
            isinstance(field[bound], bool) or not isinstance(field[bound], (int, float))
        ):
            found.append(f"{where}.{bound} must be a number.")
    if kind == "account_env":
        if not isinstance(field.get("env"), str) or not ENV.match(field["env"]):
            found.append(f"{where}.env must name an environment variable in capitals.")
        if not top:
            found.append(f"{where} cannot be an account_env inside a list.")
        if "secret" in field and not isinstance(field["secret"], bool):
            found.append(f"{where}.secret must be true or false.")
    if kind == "secrets" and not top:
        found.append(f"{where} cannot be a secrets inside a list.")
    if kind == "entry":
        if not change or not top:
            found.append(
                f"{where} cannot be an entry outside the top level of a change's form."
            )
        if not isinstance(field.get("from"), str) or not PATH.match(field["from"]):
            found.append(f"{where}.from must be a dotted path into system.yaml.")
        for name in ("value", "show"):
            if name in field and (
                not isinstance(field[name], str) or not ID.match(field[name])
            ):
                found.append(f"{where}.{name} must be a key of the entries.")
        if "fill" in field and not isinstance(field["fill"], bool):
            found.append(f"{where}.fill must be true or false.")
    if "when" in field:
        found.append(
            f"{where}.when is not supported: a factory's form has no conditions; put optional questions in a collapsed section."
        )
    if kind == "list":
        _check_fields(
            field.get("fields"), f"{where}.fields", set(), set(), False, change, found
        )
    if isinstance(fid, str):
        earlier.add(fid)
    return top and kind == "slug"


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
    if builtin is not None and builtin not in BUILTIN_BUILDS:
        found.append(
            f"build.builtin must be one of {', '.join(sorted(BUILTIN_BUILDS))}."
        )
    if builtin is None and not _text(build.get("instructions")):
        found.append("build.instructions is required when build.builtin is not set.")
    if "instructions" in build and not isinstance(build["instructions"], str):
        found.append("build.instructions must be text.")
    if "answers" in build and not isinstance(build["answers"], dict):
        found.append(
            "build.answers must be a mapping of the answers every system gets."
        )
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


def _check_presets(presets: Any, ids: set[str], found: list[str]) -> None:
    if presets is None:
        return
    if not isinstance(presets, list):
        found.append("presets must be a list.")
        return
    seen: set[str] = set()
    for p, preset in enumerate(presets):
        where = f"presets[{p}]"
        preset = preset if isinstance(preset, dict) else {}
        pid = preset.get("id")
        if not isinstance(pid, str) or not NAME.match(pid) or pid in seen:
            found.append(f"{where}.id must be a unique lowercase name.")
        else:
            seen.add(pid)
        if not _text(preset.get("label")):
            found.append(f"{where}.label is required.")
        answers = preset.get("answers")
        if not isinstance(answers, dict):
            found.append(f"{where}.answers must be a mapping of field ids to answers.")
            continue
        found.extend(
            f"{where}.answers names {fid}, which is not a field of the form."
            for fid in answers
            if fid not in ids
        )


def _check_changes(changes: Any, found: list[str]) -> None:
    if changes is None:
        return
    if not isinstance(changes, list):
        found.append("changes must be a list.")
        return
    seen: set[str] = set()
    for c, change in enumerate(changes):
        where = f"changes[{c}]"
        if not isinstance(change, dict):
            found.append(f"{where} must be a mapping.")
            continue
        cid = change.get("id")
        if not isinstance(cid, str) or not NAME.match(cid) or cid in seen:
            found.append(f"{where}.id must be a unique lowercase name.")
        else:
            seen.add(cid)
        if not _text(change.get("label")):
            found.append(f"{where}.label is required.")
        if "description" in change and not isinstance(change["description"], str):
            found.append(f"{where}.description must be text.")
        if not _text(change.get("instructions")):
            found.append(
                f"{where}.instructions are required: what the build does with it."
            )
        _check_form(change.get("form"), f"{where}.form", True, found)


def _option_value(option: Any) -> str | None:
    # An unquoted comma in a flow mapping's label makes extra keys and cuts the label.
    if isinstance(option, dict) and not set(option) <= {"value", "label"}:
        return None
    value = option.get("value") if isinstance(option, dict) else option
    return value if _text(value) else None


def _text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


# region Answers


def value_at(answers: dict, path: str) -> Any:
    """The answer at a dotted path of the answers a form sent.

    Args:
        answers: The answers.
        path: The dotted path.

    Returns:
        The answer, None when there is none.
    """
    node: Any = answers
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node


# A git repository a system's code goes to: https://host/owner/name(.git), ssh://..., or git@host:owner/name.
REPO_URL = re.compile(r"^(https://[^\s/]+/\S+|ssh://\S+|git@[^\s:]+:\S+)$")
REPO_PROVIDERS = ("github", "gitlab", "bitbucket")


def repo_provider(url: str) -> str:
    """The git host a repository URL is on: github, gitlab, bitbucket, or git for any other.

    Args:
        url: The repository URL.

    Returns:
        The provider.
    """
    host = (
        re.sub(r"^(https://|ssh://)?([^@/]*@)?", "", url)
        .split("/")[0]
        .split(":")[0]
        .lower()
    )
    return next((name for name in REPO_PROVIDERS if name in host), "git")


def repository_problems(value: Any, label: str) -> list[str]:
    """What is wrong with a repository answer, {create: true} or {create: false, url}.

    Args:
        value: The answer.
        label: The question, for the messages.

    Returns:
        The problems.
    """
    if not isinstance(value, dict) or not isinstance(value.get("create", True), bool):
        return [f"{label} must say whether to create a new GitHub repository."]
    if value.get("create", True):
        return []
    url = str(value.get("url") or "").strip()
    if not url:
        return [
            f"{label} is unchecked: give the URL of the repository to store the code in."
        ]
    if re.match(r"^https://[^/@\s]*:[^/@\s]*@", url):
        return [
            f"{label}: the URL must not carry a password or token; the build pushes with the terminal's git login."
        ]
    if not REPO_URL.match(url):
        return [
            f"{url} is not a git repository URL (https://host/owner/name or git@host:owner/name)."
        ]
    return []


def repo_record(value: Any) -> dict:
    """What system.yaml records for a repository answer: url new for one the build creates, else url and provider.

    Args:
        value: The answer.

    Returns:
        The record.
    """
    if isinstance(value, str):
        return {"url": value}
    if not isinstance(value, dict) or value.get("create", True):
        return {"url": "new", "provider": "github"}
    url = str(value["url"]).strip()
    return {"url": url, "provider": repo_provider(url)}


def secrets_problems(value: Any, label: str) -> list[str]:
    """What is wrong with a secrets answer, the names of the account variables holding the secrets.

    The values are saved in the account before the answers are written, so an
    answer carrying anything but names is refused: it would put a secret in system.yaml.

    Args:
        value: The answer.
        label: The question, for the messages.

    Returns:
        The problems.
    """
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        return [
            f"{label} must be a list of account variable names; the values are saved in your account, never in the answers."
        ]
    return [
        f"{label}: {name!r} is not an environment variable name (capitals, digits and underscores, starting with a letter)."
        for name in value
        if not ENV.match(name)
    ]


def _empty(value: Any) -> bool:
    return value is None or value == "" or value == []


def _field_problems(field: dict, value: Any, label: str) -> list[str]:
    kind = field["type"]
    if _empty(value):
        return (
            [f"{label} is required."] if field.get("required") or kind == "slug" else []
        )
    options = [_option_value(o) for o in field.get("options") or []]
    if kind == "slug" and not (isinstance(value, str) and SLUG.match(value)):
        return [
            f"{label} must be lowercase letters, digits and hyphens, starting with a letter."
        ]
    if kind == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return [f"{label} must be a number."]
        if ("min" in field and value < field["min"]) or (
            "max" in field and value > field["max"]
        ):
            return [f"{label} is out of range."]
    if kind == "boolean" and not isinstance(value, bool):
        return [f"{label} must be true or false."]
    if kind == "choice" and value not in options:
        return [f"{label} must be one of {', '.join(options)}."]
    if kind == "multichoice" and not (
        isinstance(value, list) and set(value) <= set(options)
    ):
        return [f"{label} must be some of {', '.join(options)}."]
    if kind == "feature_group" and not (isinstance(value, dict) and value.get("name")):
        return [f"{label} must be a feature group."]
    if kind == "feature_groups" and not (
        isinstance(value, list)
        and all(isinstance(v, dict) and v.get("name") for v in value)
    ):
        return [f"{label} must be a list of feature groups."]
    if kind == "repository":
        return repository_problems(value, label)
    if kind == "secrets":
        return secrets_problems(value, label)
    if kind == "list":
        if not isinstance(value, list):
            return [f"{label} must be a list."]
        found = []
        if "min_items" in field and len(value) < field["min_items"]:
            found.append(f"{label} needs at least {field['min_items']}.")
        for i, item in enumerate(value):
            item = item if isinstance(item, dict) else {}
            for sub in field.get("fields") or []:
                found += _field_problems(
                    sub,
                    item.get(sub.get("key") or sub["id"]),
                    f"{label} {i + 1}: {sub['label']}",
                )
        return found
    return []


def answer_problems(
    doc: dict, answers: dict, form: Any = None, system: dict | None = None
) -> list[str]:
    """What keeps the answers from creating a system, or from requesting a change, one line each, as the form checks them.

    `form` is a change's form, the create form when None; `system` is the system.yaml a
    change's entry fields pick from, which are not checked without it.
    The answers are nested by each field's key (its id when it has none); an account_env field's
    value never reaches the answers, since the UI saves it as an account variable, and a
    secrets field's answer is only the names of the account variables the UI saved.

    Args:
        doc: The parsed definition.
        answers: The answers.
        form: The form answered; default: the create form.
        system: The system.yaml a change is made to.

    Returns:
        The problems.
    """
    found = []
    for field in form_fields(doc.get("form") if form is None else form):
        if field["type"] == "account_env":
            continue
        value = value_at(answers, field.get("key") or field["id"])
        found += _field_problems(field, value, field["label"])
        if (
            field["type"] == "entry"
            and system is not None
            and not _empty(value)
            and value not in [e["value"] for e in entries(system, field)]
        ):
            found.append(f"{field['label']}: {value} is not in system.yaml.")
    return found


def items_at(doc: Any, path: str) -> list:
    """The items of the list at a dotted path of system.yaml; a list met on the way is walked through, so `marts.jobs` is every mart's jobs.

    Args:
        doc: The system.yaml.
        path: The dotted path.

    Returns:
        The items.
    """
    nodes = [doc]
    for part in path.split("."):
        found = []
        for node in nodes:
            for item in node if isinstance(node, list) else [node]:
                if isinstance(item, dict) and part in item:
                    found.append(item[part])
        nodes = found
    return [i for node in nodes for i in (node if isinstance(node, list) else [node])]


def entries(system: dict, field: dict) -> list[dict]:
    """What an entry field offers from system.yaml: `{value, label, entry}` for each item of its `from` list.

    Args:
        system: The system.yaml.
        field: The entry field.

    Returns:
        The entries.
    """
    value, show = field.get("value") or "slug", field.get("show") or "name"
    return [
        {
            "value": item.get(value),
            "label": str(item.get(show) or item.get(value)),
            "entry": item,
        }
        for item in items_at(system, field["from"])
        if isinstance(item, dict) and item.get(value) is not None
    ]


def change_of(doc: dict, change_id: str) -> dict:
    """The change of the definition with this id; raises KeyError when it has none.

    Args:
        doc: The parsed definition.
        change_id: The change's id.

    Returns:
        The change.
    """
    for change in doc.get("changes") or []:
        if change.get("id") == change_id:
            return change
    raise KeyError(change_id)


def slug_of(doc: dict, answers: dict) -> str | None:
    """The system's slug: the answer to the form's slug field.

    Args:
        doc: The parsed definition.
        answers: The answers.

    Returns:
        The slug, None when the form has no slug field.
    """
    for field in fields(doc):
        if field.get("type") == "slug":
            return value_at(answers, field.get("key") or field["id"])
    return None


# endregion

# region What a factory writes


def record_factory(meta: dict | None, target: Path) -> None:
    """Record the factory that built the system in its system.yaml, when a project factory delegated to a built-in.

    `meta` is what `hops factory run <name>` put in the click context: the factory's
    name and version, its instructions, and the answers to its own fields, which go to
    `requirements.extra`.

    Args:
        meta: What `hops factory run` put in the click context.
        target: The system's directory.
    """
    if not meta:
        return
    from hopsworks.cli import system_doc

    record = {"name": meta["name"], "version": meta["version"]}
    if meta.get("digest"):
        record["digest"] = meta["digest"]
    if meta.get("instructions"):
        record["instructions"] = meta["instructions"]

    def change(doc: dict) -> None:
        doc["factory"] = record
        if meta.get("extra"):
            doc.setdefault("requirements", {})["extra"] = meta["extra"]

    system_doc.update(target, change)


def factory_name(ctx: Any, default: str) -> str:
    """The factory a built-in's create registers with: the project factory delegating to it, else the built-in.

    Args:
        ctx: Click context.
        default: The built-in's own name.

    Returns:
        The factory's name.
    """
    meta = getattr(ctx, "meta", {}).get(META) if ctx is not None else None
    return meta["name"] if meta else default


META = "hops.factory"

# endregion
