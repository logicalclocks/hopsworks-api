"""``hops factory`` — the software factories of this project, and the systems they build.

Six are built in, as YAML definitions the cluster ships: ``ml-batch``,
``ml-realtime`` and ``ml-agent`` build ML systems, ``analytics-bronze``,
``analytics-silver`` and ``analytics-gold`` build analytics layers. A
project's own factories are YAML definitions (apiVersion hopsworks.ai/factory/v1) its data owners create,
import, clone and delete. ``hops factory run <name>`` builds a system with a
factory, and ``hops factory system ...`` lists, reports on and deletes the
systems built.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

import click
import yaml
from hopsworks.cli import factory_spec, output, session
from hopsworks.cli.commands import analytics, build, mlsystem


TEMPLATE = Path(__file__).resolve().parent.parent / "templates" / "hops-factory.md"


@click.group("factory")
def factory_group() -> None:
    """The software factories: the built-in ML system and analytics layer factories, and the project's own."""


factory_group.add_command(mlsystem.system_group)


# region Managing factories


@factory_group.command("list")
@click.pass_context
def factory_list(ctx: click.Context) -> None:
    """List the factories: the built-in ones and the project's own, with how many systems each built."""
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    factories = factory_api._list()
    if output.JSON_MODE:
        output.print_json(
            [
                {k: v for k, v in f.items() if k not in ("spec", "definition")}
                for f in factories
            ]
        )
        return
    output.print_table(
        ["NAME", "TITLE", "KIND", "VERSION", "SYSTEMS", "ENABLED"],
        [
            [
                f.get("name"),
                f.get("title"),
                "built-in" if f.get("builtin") else "project",
                f.get("currentVersion"),
                f.get("systems", 0),
                "yes" if f.get("enabled") else "no",
            ]
            for f in factories
        ],
    )


@factory_group.command("get")
@click.argument("name")
@click.option("--version", type=int, help="An earlier version of the definition.")
@click.pass_context
def factory_get(ctx: click.Context, name: str, version: int | None) -> None:
    """Print the YAML definition of factory NAME."""
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    factory = factory_api._get(name, version)
    if output.JSON_MODE:
        output.print_json(factory)
        return
    click.echo(factory.get("definition", ""), nl=False)


@factory_group.command("export")
@click.argument("name")
@click.option("--version", type=int, help="An earlier version of the definition.")
@click.option(
    "-o",
    "--output",
    "out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="The file to write; defaults to <name>.factory.yaml.",
)
@click.pass_context
def factory_export(
    ctx: click.Context, name: str, version: int | None, out: Path | None
) -> None:
    """Write the YAML definition of factory NAME to a file, to import in another project or cluster."""
    from hopsworks_common.core import factory_api

    project = session.get_project(ctx)
    factory = factory_api._get(name, version)
    target = out or Path(f"{name}.factory.yaml")
    header = f"# Exported from project {getattr(project, 'name', '?')}, factory {name} version {factory.get('version')}.\n"
    target.write_text(header + factory.get("definition", ""), encoding="utf-8")
    output.success(f"Wrote {target}")


@factory_group.command("validate")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
def factory_validate(file: Path) -> None:
    """Check the factory definition in FILE, without a cluster."""
    found = factory_spec.text_problems(file.read_text(encoding="utf-8"))
    if found:
        raise click.ClickException("\n  ".join(["invalid definition:", *found]))
    output.success(f"{file} is a valid factory definition")


@factory_group.command("import")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--name", help="Import it under this name instead of the one it carries.")
@click.option(
    "--update",
    is_flag=True,
    help="Save it as a new version of the factory of its name.",
)
@click.option("--yes", is_flag=True, help="Skip the review and the confirmation.")
@click.pass_context
def factory_import(
    ctx: click.Context, file: Path, name: str | None, update: bool, yes: bool
) -> None:
    """Import the factory definition in FILE, written here or exported from another project.

    The review prints its questions, phases, skills and its build instructions in full:
    Claude Code follows those instructions in your Terminal, with your credentials.
    """
    from hopsworks_common.core import factory_api

    text = file.read_text(encoding="utf-8")
    found = factory_spec.text_problems(text)
    if found:
        raise click.ClickException("\n  ".join(["invalid definition:", *found]))
    doc = factory_spec.parse(text)
    if not yes:
        _review(doc)
        click.confirm("Import this factory?", abort=True)
    session.get_project(ctx)
    if update:
        factory = factory_api._update(name or doc["name"], text)
    else:
        factory = factory_api._create(text, name)
    output.success(
        f"Factory {factory.get('name')} version {factory.get('currentVersion')} saved"
    )


@factory_group.command("clone")
@click.argument("source")
@click.argument("name")
@click.option("--title", help="Its title; defaults to the source's title and (copy).")
@click.pass_context
def factory_clone(
    ctx: click.Context, source: str, name: str, title: str | None
) -> None:
    """Clone factory SOURCE, built-in or the project's own, into a new project factory NAME."""
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    original = factory_api._get(source)
    text = original.get("definition", "")
    new_title = title or f"{original.get('title', source)} (copy)"
    text = _replace_top(text, "title", json.dumps(new_title))
    factory = factory_api._create(text, name)
    output.success(f"Cloned {source} into {factory.get('name')}")


@factory_group.command("enable")
@click.argument("name")
@click.pass_context
def factory_enable(ctx: click.Context, name: str) -> None:
    """Show the project factory NAME in the Factory again."""
    _set_enabled(ctx, name, True)


@factory_group.command("disable")
@click.argument("name")
@click.pass_context
def factory_disable(ctx: click.Context, name: str) -> None:
    """Hide the project factory NAME from the Factory; its systems are kept."""
    _set_enabled(ctx, name, False)


@factory_group.command("delete")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def factory_delete(ctx: click.Context, name: str, yes: bool) -> None:
    """Delete the project factory NAME; refused while systems it built still exist."""
    from hopsworks_common.core import factory_api

    if not yes:
        click.confirm(f"Delete factory {name}?", abort=True)
    session.get_project(ctx)
    factory_api._delete(name)
    output.success(f"Deleted factory {name}")


def _set_enabled(ctx: click.Context, name: str, enabled: bool) -> None:
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    factory_api._set_enabled(name, enabled)
    output.success(f"Factory {name} {'enabled' if enabled else 'disabled'}")


def _replace_top(text: str, key: str, value: str) -> str:
    """The YAML with its top-level `key` set to `value`, keeping everything else as written."""
    line = re.compile(rf"(?m)^{key}:[^\n]*$")
    if line.search(text):
        return line.sub(lambda _: f"{key}: {value}", text, count=1)
    return f"{key}: {value}\n{text}"


def _review(doc: dict) -> None:
    click.echo(f"\n{doc.get('title')} ({doc.get('name')})")
    if doc.get("description"):
        click.echo(f"  {doc['description']}")
    click.echo("\nQuestions:")
    for field in factory_spec.fields(doc):
        flag = " (required)" if field.get("required") else ""
        click.echo(f"  - {field.get('label')} [{field.get('type')}]{flag}")
    click.echo(
        "\nPhases: " + ", ".join(p.get("label", "?") for p in doc.get("phases", []))
    )
    build_spec = doc.get("build") or {}
    if build_spec.get("builtin"):
        click.echo(f"Builds with the built-in {build_spec['builtin']} factory.")
    if build_spec.get("skills"):
        click.echo("Skills: " + ", ".join(build_spec["skills"]))
    if build_spec.get("instructions"):
        click.echo("\nClaude Code will follow these instructions in your Terminal:\n")
        click.echo(build_spec["instructions"])


# endregion

# region Running a factory


@factory_group.command("run")
@click.argument("name")
@click.argument("slug", required=False)
@click.option(
    "--answers",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="A JSON file of the answers to the factory's questions, as the Hopsworks UI writes it; without it they are asked here.",
)
@click.option(
    "--preset",
    help="Start from one of the factory's examples (`hops factory get NAME` lists them under presets).",
)
@click.option(
    "--change",
    "change_id",
    help="Request this change of a recorded system (`hops factory get NAME` lists the factory's changes); its answers come from --answers or are asked here.",
)
@click.option(
    "--no-launch",
    is_flag=True,
    help="Record the system or the change but do not start Claude Code.",
)
@click.pass_context
def factory_run(
    ctx: click.Context,
    name: str,
    slug: str | None,
    answers: Path | None,
    preset: str | None,
    change_id: str | None,
    no_launch: bool,
) -> None:
    """Build a system with the factory NAME, resume the system SLUG, or change it.

    A new system's answers come from --answers, or each of the factory's
    questions is asked in turn. The system is recorded in ./<slug>/system.yaml,
    registered so the Hopsworks UI lists it, and built by Claude Code. A system
    already recorded (SLUG under the current directory, or the current
    directory itself) is resumed from its system.yaml, which holds the
    factory, its version and every answer, so it needs none. With --change,
    the change's answers are recorded in system.yaml as a pending request,
    which the build carries out when it resumes.
    """
    from hopsworks_common.core import factory_api

    session.get_project(ctx)
    definition = factory_api._get(name)
    existing = _recorded(slug)
    if change_id and existing is None:
        raise click.ClickException(
            "a change is made to a recorded system: name its SLUG, or run this in its directory"
        )
    if existing is not None:
        if preset:
            raise click.UsageError(
                f"{existing} is already recorded; its answers are in its system.yaml"
            )
        if change_id:
            _request_change(ctx, definition, existing, change_id, answers)
        elif answers:
            raise click.UsageError(
                f"{existing} is already recorded; pass --change with --answers to change it"
            )
        _resume(ctx, definition, existing, not no_launch)
        return
    if slug:
        raise click.ClickException(
            f"no system {slug!r} under {Path.cwd()}; leave SLUG out to build a new one"
        )
    if not definition.get("enabled", True):
        raise click.ClickException(
            f"the factory {name} is disabled; `hops factory enable {name}` turns it on"
        )
    spec = definition["spec"]
    chosen = _preset(spec, preset)
    data = (
        _read_answers(answers)
        if answers
        else _ask(spec["form"], (chosen or {}).get("answers") or {})
    )
    if chosen:
        data = {**data, **(chosen.get("fixed") or {})}
    create_system(ctx, definition, data, not no_launch)


def _recorded(slug: str | None) -> Path | None:
    """The directory of a system already recorded: SLUG under here (a layer may be in its analytics repository), or here."""
    cwd = Path.cwd()
    if not slug:
        return cwd if (cwd / "system.yaml").is_file() else None
    if (cwd / slug / "system.yaml").is_file():
        return cwd / slug
    return next((d for d in analytics._layer_dirs(cwd) if d.name == slug), None)


def _preset(spec: dict, preset: str | None) -> dict | None:
    if preset is None:
        return None
    presets = spec.get("presets") or []
    for entry in presets:
        if entry.get("id") == preset:
            return entry
    known = ", ".join(p.get("id", "?") for p in presets) or "none"
    raise click.BadParameter(
        f"no preset {preset!r}; this factory has {known}", param_hint="--preset"
    )


def _read_answers(path: Path) -> dict:
    try:
        answers = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise click.BadParameter(str(exc), param_hint="--answers") from exc
    if not isinstance(answers, dict):
        raise click.BadParameter(
            "the answers must be a JSON object", param_hint="--answers"
        )
    return answers


def _ask(form: dict, start: dict, system: dict | None = None) -> dict:
    """The answers to a form's questions, asked section by section; `start` holds a preset's answers by field id.

    The answers are nested at each field's key, as the Hopsworks UI sends them.
    An entry field offers the entries of `system`, the system.yaml a change is made to;
    with `fill`, the chosen entry's values are the defaults of the questions after it.
    """
    answers: dict = {}
    filled: dict = {}
    for section in form["sections"]:
        click.echo()
        click.echo(click.style(section["title"], bold=True))
        for field in section.get("fields") or []:
            key = field.get("key") or field["id"]
            if field["type"] == "account_env":
                _account_env(field)
                continue
            if field["type"] == "entry":
                value, entry = _ask_entry(field, system or {}, start.get(field["id"]))
                if field.get("fill"):
                    filled = entry
            else:
                default = factory_spec.value_at(filled, key) if filled else None
                if default is None:
                    default = start.get(field["id"], field.get("default"))
                value = _ask_field(field, default)
            if value is not None:
                _put(answers, key, value)
    return answers


def _ask_entry(field: dict, system: dict, default: Any) -> tuple[Any, dict]:
    offered = factory_spec.entries(system, field)
    if not offered:
        raise click.ClickException(
            f"{field['label']}: system.yaml has nothing at {field['from']} to choose from"
        )
    for i, entry in enumerate(offered, 1):
        click.echo(f"  {i}. {entry['label']} ({entry['value']})")
    values = [str(e["value"]) for e in offered]
    picked = click.prompt(
        field["label"],
        type=click.Choice(values),
        default=str(default) if str(default) in values else values[0],
    )
    entry = offered[values.index(picked)]
    return entry["value"], entry["entry"]


def _put(answers: dict, path: str, value: Any) -> None:
    *parents, last = path.split(".")
    node = answers
    for part in parents:
        node = node.setdefault(part, {})
    node[last] = value


def _ask_field(field: dict, default: Any, label: str | None = None) -> Any:
    """One answer, asked until it is valid; None for an optional question left empty."""
    label = label or field["label"]
    if field.get("help"):
        click.echo(click.style(f"  {field['help']}", dim=True))
    while True:
        value = _prompt(field, default, label)
        if value == "" or value == []:
            value = None
        found = factory_spec._field_problems(field, value, label)
        if not found:
            return value
        output.warn(found[0])


def _prompt(field: dict, default: Any, label: str) -> Any:
    kind = field["type"]
    options = [factory_spec._option_value(o) for o in field.get("options") or []]
    if kind == "boolean":
        return click.confirm(label, default=bool(default))
    if kind == "choice":
        return click.prompt(
            label,
            type=click.Choice(options),
            default=default if default in options else options[0],
        )
    if kind == "list":
        return _ask_list(field, default if isinstance(default, list) else [], label)
    shown = _shown(kind, default)
    hint = {
        "multichoice": f" ({', '.join(options)}; comma-separated)",
        "feature_group": " (name or name:version)",
        "feature_groups": " (name or name:version, comma-separated)",
    }.get(kind, "")
    raw = click.prompt(
        label + hint, default=shown, show_default=bool(shown), type=str
    ).strip()
    if kind == "number":
        try:
            number = float(raw)
        except ValueError:
            return raw or None
        return int(number) if number.is_integer() else number
    if kind == "multichoice":
        return [v.strip() for v in raw.split(",") if v.strip()]
    if kind == "feature_group":
        return _feature_group(raw) if raw else None
    if kind == "feature_groups":
        return [_feature_group(v.strip()) for v in raw.split(",") if v.strip()]
    return raw


def _shown(kind: str, default: Any) -> str:
    """A default as the prompt shows it and takes it back."""
    if default is None:
        return ""
    if kind == "multichoice" and isinstance(default, list):
        return ",".join(map(str, default))
    if kind in ("feature_group", "feature_groups"):
        groups = default if isinstance(default, list) else [default]
        return ",".join(
            f"{g.get('name')}:{g.get('version', 1)}"
            for g in groups
            if isinstance(g, dict)
        )
    return str(default)


def _feature_group(text: str) -> dict:
    name, _, version = text.partition(":")
    return {"name": name, "version": int(version) if version.isdigit() else 1}


def _ask_list(field: dict, start: list, label: str) -> list:
    items = []
    item_label = field.get("item_label") or "entry"
    while True:
        wanted = len(items) < len(start) or len(items) < field.get("min_items", 0)
        if not click.confirm(f"{label}: add a {item_label}?", default=wanted):
            return items
        preset = start[len(items)] if len(items) < len(start) else {}
        item: dict = {}
        for sub in field.get("fields") or []:
            value = _ask_field(
                sub,
                preset.get(sub["id"], sub.get("default")),
                f"  {item_label} {len(items) + 1}: {sub['label']}",
            )
            if value is not None:
                _put(item, sub.get("key") or sub["id"], value)
        items.append(item)


def _account_env(field: dict) -> None:
    """Ask for an account variable the build reads, unless the account has it, and save it there.

    Hopsworks sets account variables in every job, app and deployment the user
    starts, so the value never enters the answers, system.yaml or a Claude session.
    """
    from hopsworks_common.core import env_var_api

    env = field["env"]
    try:
        api = env_var_api.EnvVarsApi()
        present = {v.name for v in api.get_env_vars(include_value=False)}
    except Exception as exc:  # noqa: BLE001 - the build reads it later, or reports it missing
        output.warn(
            f"Could not read your account environment variables ({exc}); set {env} in "
            "Account settings, Environment variables, before the build needs it."
        )
        return
    if env in present:
        click.echo(
            click.style(
                f"  {field['label']}: {env} is saved in your account.", dim=True
            )
        )
        return
    secret = bool(field.get("secret"))
    while True:
        value = click.prompt(
            field["label"] + (" (not shown)" if secret else ""),
            default="" if secret else str(field.get("default") or ""),
            hide_input=secret,
            show_default=not secret,
        ).strip()
        if value or not field.get("required"):
            break
        output.warn(f"{field['label']} is required.")
    if value:
        api.set_env_var(env, value, visibility="PRIVATE")
        output.success(f"Saved {env} in your account settings.")


def _built_with(definition: dict, target: Path) -> tuple[dict, dict]:
    """The system.yaml in `target`, and the definition of the factory version it was built with."""
    from hopsworks_common.core import factory_api

    doc = yaml.safe_load((target / "system.yaml").read_text(encoding="utf-8")) or {}
    name = definition["name"]
    recorded = doc.get("factory") or {}
    if recorded.get("name") not in (None, name):
        raise click.ClickException(
            f"{target.name} was built by the factory {recorded['name']}; "
            f"run `hops factory run {recorded['name']} {target.name}` instead"
        )
    if recorded.get("version") not in (None, definition.get("version")):
        definition = factory_api._get(name, recorded["version"])
    return doc, definition


def _request_change(
    ctx: click.Context,
    definition: dict,
    target: Path,
    change_id: str,
    answers_path: Path | None,
) -> None:
    """Record a pending change request in the system's system.yaml, from its answers; the build carries it out."""
    from datetime import datetime, timezone

    # A system keeps the factory version it was built with, but its changes are the current
    # version's: each request records the instructions it was made with.
    doc, _ = _built_with(definition, target)
    spec = definition["spec"]
    try:
        change = factory_spec.change_of(spec, change_id)
    except KeyError:
        known = ", ".join(c["id"] for c in spec.get("changes") or []) or "none"
        raise click.BadParameter(
            f"no change {change_id!r}; this factory has {known}", param_hint="--change"
        ) from None
    answers = (
        _read_answers(answers_path) if answers_path else _ask(change["form"], {}, doc)
    )
    found = factory_spec.answer_problems(spec, answers, change["form"], doc)
    if found:
        raise click.ClickException("\n  ".join(["invalid answers:", *found]))
    doc.setdefault("changes", []).append(
        {
            "id": change_id,
            "label": change["label"],
            "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
            "answers": answers,
            "instructions": change["instructions"],
            "status": "pending",
        }
    )
    system_file = target / "system.yaml"
    system_file.write_text(
        yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )
    if _in_git(target):
        subprocess.run(
            [
                "git",
                "-C",
                str(target),
                "commit",
                "-q",
                "-m",
                f"[{target.name}] request: {change['label']}",
                "--",
                "system.yaml",
            ],
            check=False,
            capture_output=True,
        )
    output.success(f"{change['label']} requested in {system_file}")


def _in_git(target: Path) -> bool:
    done = subprocess.run(
        ["git", "-C", str(target), "rev-parse", "--is-inside-work-tree"],
        check=False,
        capture_output=True,
        text=True,
    )
    return done.returncode == 0


def _resume(ctx: click.Context, definition: dict, target: Path, launch: bool) -> None:
    """Start or resume the build of the system recorded in `target`, with the factory version it was built with."""
    _, definition = _built_with(definition, target)
    name = definition["name"]
    output.info(f"Resuming {target.name} from {target / 'system.yaml'}.")
    builtin = (definition["spec"].get("build") or {}).get("builtin")
    if builtin == "mlsystem":
        build.resume(ctx, target, launch)
    elif builtin in ("analytics-bronze", "analytics-silver", "analytics-gold"):
        layer = builtin.removeprefix("analytics-")
        analytics._launch(target, launch, f"/hops-{layer} {target.name}")
    else:
        _write_build_files(target, definition, definition["spec"], target.name)
        analytics._launch(target, launch, f"/hops-factory-{name} {target.name}")


# endregion


def create_system(
    ctx: click.Context, definition: dict, answers: dict, launch: bool
) -> Path:
    """Create a system with a factory from its answers; returns its directory.

    A factory whose build is a built-in's (mlsystem, analytics-bronze|silver|gold, and any
    clone of them) hands the answers that built-in knows, with the factory's constant answers,
    to that built-in's create; the rest are recorded as `requirements.extra`, with the factory's
    instructions, in system.yaml. Any other factory writes system.yaml itself and starts Claude
    Code on the command generated from its instructions.
    """
    spec = definition["spec"]
    found = factory_spec.answer_problems(spec, answers)
    if found:
        raise click.ClickException("\n  ".join(["invalid answers:", *found]))
    build_spec = spec.get("build") or {}
    builtin = build_spec.get("builtin")
    if not builtin:
        return _create_own(ctx, definition, spec, answers, launch)
    known = BUILTIN_KEYS[builtin]
    merged = {**answers, **(build_spec.get("answers") or {})}
    ctx.meta[factory_spec.META] = {
        "name": definition["name"],
        "version": definition["version"],
        "instructions": build_spec.get("instructions"),
        "extra": {k: v for k, v in merged.items() if k not in known},
    }
    data = {k: v for k, v in merged.items() if k in known}
    create = {
        "mlsystem": build.create,
        "analytics-bronze": analytics.create_bronze,
        "analytics-silver": analytics.create_silver,
        "analytics-gold": analytics.create_gold,
    }[builtin]
    return create(ctx, data, launch)


# The answers each built-in build reads; a factory's other answers are its own.
BUILTIN_KEYS = {
    "mlsystem": build.ANSWER_KEYS,
    "analytics-bronze": analytics.BRONZE_KEYS,
    "analytics-silver": analytics.ANSWER_KEYS,
    "analytics-gold": analytics.GOLD_KEYS,
}


def _create_own(
    ctx: click.Context,
    definition: dict,
    spec: dict,
    answers: dict,
    launch: bool,
) -> Path:
    name, version = definition["name"], definition["version"]
    slug = factory_spec.slug_of(spec, answers)
    target = Path.cwd() / slug
    system_file = target / "system.yaml"
    if system_file.exists():
        output.info(f"{slug} already exists here; resuming it.")
    else:
        target.mkdir(parents=True, exist_ok=True)
        phases = [
            {k: p[k] for k in ("key", "label", "minutes") if k in p}
            for p in spec["phases"]
        ]
        doc: dict = {
            "schema_version": 1,
            "factory": {"name": name, "version": version, "phases": phases},
            "system": {
                "name": slug.replace("-", " ").capitalize(),
                "slug": slug,
                "version": "0.1.0",
                "status": "draft",
            },
            "requirements": {
                **answers,
                **((spec.get("build") or {}).get("answers") or {}),
            },
        }
        for phase in phases:
            doc.setdefault(phase["key"], {})["status"] = "pending"
        system_file.write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    _write_build_files(target, definition, spec, slug)
    output.success(f"Recorded in {system_file}")
    try:
        mlsystem.register(ctx, target, slug, name)
    except Exception as exc:  # noqa: BLE001 - the system is recorded either way
        output.warn(
            f"Not registered in the project's Factory ({exc}); run `hops factory system register {target} --factory {name}`."
        )
    analytics._launch(target, launch, f"/hops-factory-{name} {slug}")
    return target


def _write_build_files(target: Path, definition: dict, spec: dict, slug: str) -> None:
    """The system's AGENTS.md and the slash command Claude Code builds it with."""
    name = definition["name"]
    build_spec = spec.get("build") or {}
    skills = build_spec.get("skills") or []
    values = {
        "{name}": name,
        "{title}": spec.get("title", name),
        "{version}": str(definition["version"]),
        "{slug}": slug,
        "{phases}": ", ".join(f"`{p['key']}` ({p['label']})" for p in spec["phases"]),
        "{skills}": (
            "- Load these skills before you start: "
            + ", ".join(f"**{s}**" for s in skills)
            + ".\n"
            if skills
            else ""
        ),
    }
    text = TEMPLATE.read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace(key, value)
    # Last, so nothing in the user's instructions is read as a placeholder.
    text = text.replace("{instructions}", build_spec.get("instructions", "").strip())
    commands = target / ".claude" / "commands"
    commands.mkdir(parents=True, exist_ok=True)
    (commands / f"hops-factory-{name}.md").write_text(text, encoding="utf-8")
    agents = target / "AGENTS.md"
    if not agents.exists():
        agents.write_text(
            f"# {slug}\n\n"
            f"Built by the {spec.get('title', name)} factory (`{name}`) from `system.yaml`.\n"
            f'Build or resume it with `claude "/hops-factory-{name} {slug}"` in this directory.\n',
            encoding="utf-8",
        )


# endregion
